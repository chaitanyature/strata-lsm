// Package strata implements a single-writer embedded log-structured key/value store.
package strata

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"unicode/utf8"
)

type Op struct {
	Key    string
	Value  string
	Delete bool
}
type batch struct {
	Seq uint64
	Ops []Op
}
type manifest struct {
	Seq    uint64
	Tables []string
}
type table []Op
type DB struct {
	mu        sync.RWMutex
	dir       string
	wal, lock *os.File
	mem       map[string]Op
	tables    []table // oldest first
	current   manifest
	seq       uint64
	failed    error
}

func syncDir(path string) error {
	d, e := os.Open(path)
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}

func Open(dir string) (*DB, error) {
	if e := os.MkdirAll(dir, 0755); e != nil {
		return nil, e
	}
	l, e := os.OpenFile(filepath.Join(dir, "LOCK"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(l.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		l.Close()
		return nil, fmt.Errorf("acquire writer lock: %w", e)
	}
	d := &DB{dir: dir, lock: l, mem: make(map[string]Op)}
	if e = d.load(); e != nil {
		d.Close()
		return nil, e
	}
	return d, nil
}

func (d *DB) load() error {
	raw, e := os.ReadFile(filepath.Join(d.dir, "MANIFEST"))
	if e == nil {
		if e = json.Unmarshal(raw, &d.current); e != nil {
			return fmt.Errorf("manifest: %w", e)
		}
	} else if !os.IsNotExist(e) {
		return e
	}
	live := map[string]bool{}
	for _, name := range d.current.Tables {
		if filepath.Base(name) != name || filepath.Ext(name) != ".sst" || live[name] {
			return errors.New("invalid manifest table name")
		}
		t, e := loadTable(filepath.Join(d.dir, name))
		if e != nil {
			return e
		}
		d.tables = append(d.tables, t)
		live[name] = true
	}
	d.seq = d.current.Seq
	w, e := os.OpenFile(filepath.Join(d.dir, "WAL"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	d.wal = w
	if e = syncDir(d.dir); e != nil {
		return e
	}
	r := bufio.NewReader(w)
	var good int64
	var previous uint64
	for {
		p, e := readFrame(r)
		if e == io.EOF {
			break
		}
		if e == io.ErrUnexpectedEOF {
			if e = w.Truncate(good); e != nil {
				return e
			}
			if e = w.Sync(); e != nil {
				return e
			}
			break
		}
		if e != nil {
			return fmt.Errorf("WAL offset %d: %w", good, e)
		}
		var b batch
		if e = json.Unmarshal(p, &b); e != nil {
			return e
		}
		if b.Seq == 0 || (previous != 0 && b.Seq != previous+1) || len(b.Ops) == 0 {
			return errors.New("invalid WAL sequence or batch")
		}
		previous = b.Seq
		if b.Seq > d.seq {
			if b.Seq != d.seq+1 {
				return errors.New("gap after checkpoint")
			}
			for _, op := range b.Ops {
				d.mem[op.Key] = op
			}
			d.seq = b.Seq
		}
		good += int64(16 + len(p))
	}
	// All surviving frames may already be covered by the durable checkpoint.
	// Discard that prefix before appending a new sequence after recovery.
	if good > 0 && d.seq == d.current.Seq {
		if e = w.Truncate(0); e != nil {
			return e
		}
		if e = w.Sync(); e != nil {
			return e
		}
	}
	if _, e = w.Seek(0, io.SeekEnd); e != nil {
		return e
	}
	// A crash before or after manifest publication can leave unreferenced files.
	entries, e := os.ReadDir(d.dir)
	if e != nil {
		return e
	}
	for _, entry := range entries {
		n := entry.Name()
		if (len(n) > 6 && n[:6] == "table-" && filepath.Ext(n) == ".sst" && !live[n]) || n == "MANIFEST.tmp" {
			if e = os.Remove(filepath.Join(d.dir, n)); e != nil {
				return e
			}
		}
	}
	return nil
}

func loadTable(path string) (table, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var out table
	// The terminal frame detects truncation exactly on a record boundary.
	for {
		p, e := readFrame(r)
		if e != nil {
			return nil, fmt.Errorf("table %s: %w", filepath.Base(path), e)
		}
		if string(p) == "null" {
			if _, e = r.ReadByte(); e != io.EOF {
				return nil, errors.New("bytes after table footer")
			}
			return out, nil
		}
		var op Op
		if e = json.Unmarshal(p, &op); e != nil {
			return nil, e
		}
		if len(out) > 0 && out[len(out)-1].Key >= op.Key {
			return nil, errors.New("table keys out of order")
		}
		out = append(out, op)
	}
}

func (d *DB) writable() error {
	if d.lock == nil {
		return errors.New("database closed")
	}
	return d.failed
}
func (d *DB) poison(e error) error {
	if e != nil {
		d.failed = fmt.Errorf("reopen database after I/O error: %w", e)
	}
	return d.failed
}

// Write publishes the entire batch only after its WAL frame has been synced.
func (d *DB) Write(ops ...Op) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.writable(); e != nil {
		return e
	}
	if len(ops) == 0 {
		return nil
	}
	for _, o := range ops {
		if !utf8.ValidString(o.Key) || !utf8.ValidString(o.Value) {
			return errors.New("keys and values must be UTF-8")
		}
	}
	b := batch{d.seq + 1, ops}
	p, e := json.Marshal(b)
	if e != nil {
		return e
	}
	record, e := frame(p)
	if e != nil {
		return e
	}
	if _, e = d.wal.Write(record); e != nil {
		return d.poison(e)
	}
	if e = d.wal.Sync(); e != nil {
		return d.poison(e)
	}
	for _, o := range ops {
		d.mem[o.Key] = o
	}
	d.seq++
	return nil
}

func (d *DB) Get(key string) (string, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if o, ok := d.mem[key]; ok {
		return o.Value, !o.Delete
	}
	return lookup(d.tables, key)
}
func (d *DB) Snapshot() Snapshot {
	d.mu.RLock()
	defer d.mu.RUnlock()
	ts := append([]table(nil), d.tables...)
	ts = append(ts, sorted(d.mem))
	return Snapshot{ts, d.seq}
}

func (d *DB) Flush() error   { return d.checkpoint(false) }
func (d *DB) Compact() error { return d.checkpoint(true) }

func (d *DB) checkpoint(compact bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.writable(); e != nil {
		return e
	}
	if len(d.mem) == 0 && (!compact || len(d.tables) < 2) {
		return nil
	}
	t := sorted(d.mem)
	tables := append([]table(nil), d.tables...)
	names := append([]string(nil), d.current.Tables...)
	if compact {
		var merged table
		Snapshot{append(tables, t), d.seq}.Scan("", "", func(k, v string) bool { merged = append(merged, Op{Key: k, Value: v}); return true })
		t = merged
		tables = nil
		names = nil
	}
	f, e := os.CreateTemp(d.dir, "table-*.sst")
	if e != nil {
		return d.poison(e)
	}
	name := filepath.Base(f.Name())
	bw := bufio.NewWriter(f)
	emit := func(payload []byte) error {
		b, e := frame(payload)
		if e == nil {
			_, e = bw.Write(b)
		}
		return e
	}
	for _, o := range t {
		p, err := json.Marshal(o)
		if err == nil {
			err = emit(p)
		}
		if err != nil {
			f.Close()
			return d.poison(err)
		}
	}
	e = emit([]byte("null"))
	if e == nil {
		e = bw.Flush()
	}
	if e == nil {
		e = f.Sync()
	}
	closeErr := f.Close()
	if e == nil {
		e = closeErr
	}
	if e != nil {
		return d.poison(e)
	}
	// Ensure the immutable table's directory entry is durable before referencing it.
	if e = syncDir(d.dir); e != nil {
		return d.poison(e)
	}
	next := manifest{d.seq, append(names, name)}
	p, e := json.Marshal(next)
	if e != nil {
		return e
	}
	mf, e := os.OpenFile(filepath.Join(d.dir, "MANIFEST.tmp"), os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0600)
	if e != nil {
		return d.poison(e)
	}
	_, e = mf.Write(p)
	if e == nil {
		e = mf.Sync()
	}
	closeErr = mf.Close()
	if e == nil {
		e = closeErr
	}
	if e == nil {
		e = os.Rename(mf.Name(), filepath.Join(d.dir, "MANIFEST"))
	}
	if e == nil {
		e = syncDir(d.dir)
	}
	if e != nil {
		return d.poison(e)
	}
	old := d.current
	d.current = next
	d.tables = append(tables, t)
	d.mem = make(map[string]Op)
	if e = d.wal.Truncate(0); e == nil {
		_, e = d.wal.Seek(0, io.SeekStart)
	}
	if e == nil {
		e = d.wal.Sync()
	}
	if e != nil {
		return d.poison(e)
	}
	if compact {
		for _, n := range old.Tables {
			if e = os.Remove(filepath.Join(d.dir, n)); e != nil {
				return d.poison(e)
			}
		}
	}
	return nil
}

func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.lock == nil {
		return nil
	}
	var e error
	if d.wal != nil {
		e = d.wal.Close()
	}
	other := d.lock.Close()
	d.lock = nil
	if e != nil {
		return e
	}
	return other
}
