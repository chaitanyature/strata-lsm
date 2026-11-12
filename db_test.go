package strata

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func open(t testing.TB, dir string) *DB {
	t.Helper()
	d, e := Open(dir)
	must(t, e)
	t.Cleanup(func() { d.Close() })
	return d
}
func all(s Snapshot) map[string]string {
	m := map[string]string{}
	s.Scan("", "", func(k, v string) bool { m[k] = v; return true })
	return m
}

func TestCheckpointAndOldSnapshot(t *testing.T) {
	dir := t.TempDir()
	d := open(t, dir)
	must(t, d.Write(Op{Key: "a", Value: "old"}, Op{Key: "b", Value: "tail"}, Op{Key: "", Value: "zero"}))
	must(t, d.Flush())
	snap := d.Snapshot()
	must(t, d.Write(Op{Key: "a", Delete: true}, Op{Key: "b", Value: "new"}, Op{Key: "c", Value: "third"}))
	must(t, d.Flush())
	must(t, d.Compact())
	must(t, d.Close())
	d = open(t, dir)
	if got := all(snap); !reflect.DeepEqual(got, map[string]string{"a": "old", "b": "tail", "": "zero"}) {
		t.Fatal(got)
	}
	if got := all(d.Snapshot()); !reflect.DeepEqual(got, map[string]string{"b": "new", "c": "third", "": "zero"}) {
		t.Fatal(got)
	}
	visited := 0
	d.Snapshot().Scan("b", "d", func(k, v string) bool {
		visited++
		if k != "b" {
			t.Fatal(k)
		}
		return false
	})
	if visited != 1 {
		t.Fatal(visited)
	}
}

func TestEveryTornBatchPrefix(t *testing.T) {
	dir := t.TempDir()
	d := open(t, dir)
	must(t, d.Write(Op{Key: "base", Value: "ack"}))
	must(t, d.Flush())
	must(t, d.Close())
	p, _ := json.Marshal(batch{2, []Op{{Key: "x", Value: "one"}, {Key: "y", Value: "two"}}})
	record, e := frame(p)
	must(t, e)
	for n := 0; n <= len(record); n++ {
		must(t, os.WriteFile(filepath.Join(dir, "WAL"), record[:n], 0600))
		d = open(t, dir)
		for _, k := range []string{"x", "y"} {
			_, ok := d.Get(k)
			if ok != (n == len(record)) {
				t.Fatalf("cut %d: %s visible=%v", n, k, ok)
			}
		}
		if v, _ := d.Get("base"); v != "ack" {
			t.Fatal("lost checkpoint")
		}
		must(t, d.Write(Op{Key: "after", Value: "repair"}))
		must(t, d.Close())
	}
}

func TestCorruptCompleteFrameFailsClosed(t *testing.T) {
	dir := t.TempDir()
	d := open(t, dir)
	must(t, d.Write(Op{Key: "x", Value: "one"}))
	must(t, d.Close())
	path := filepath.Join(dir, "WAL")
	raw, e := os.ReadFile(path)
	must(t, e)
	for _, i := range []int{1, 5, 8, 13, len(raw) - 1} {
		damaged := bytes.Clone(raw)
		damaged[i] ^= 1
		must(t, os.WriteFile(path, damaged, 0600))
		if bad, e := Open(dir); e == nil {
			bad.Close()
			t.Fatalf("accepted corruption at byte %d", i)
		}
		unchanged, e := os.ReadFile(path)
		must(t, e)
		if !bytes.Equal(damaged, unchanged) {
			t.Fatal("corruption was silently repaired")
		}
	}
}

func TestManifestWinsOverStaleWALAndOrphans(t *testing.T) {
	dir := t.TempDir()
	d := open(t, dir)
	must(t, d.Write(Op{Key: "k", Value: "old"}))
	old, e := os.ReadFile(filepath.Join(dir, "WAL"))
	must(t, e)
	must(t, d.Flush())
	must(t, d.Write(Op{Key: "k", Delete: true}))
	must(t, d.Compact())
	must(t, d.Close())
	must(t, os.WriteFile(filepath.Join(dir, "WAL"), old, 0600))
	orphan := filepath.Join(dir, "table-abandoned.sst")
	must(t, os.WriteFile(orphan, []byte("partial"), 0600))
	d = open(t, dir)
	if _, ok := d.Get("k"); ok {
		t.Fatal("stale WAL resurrected a tombstone")
	}
	if _, e := os.Stat(orphan); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("orphan still present", e)
	}
	must(t, d.Write(Op{Key: "next", Value: "ok"}))
	must(t, d.Close())
	d = open(t, dir)
	if v, _ := d.Get("next"); v != "ok" {
		t.Fatal(v)
	}
}

func TestTruncatedTableAndWriterLock(t *testing.T) {
	dir := t.TempDir()
	d := open(t, dir)
	if other, e := Open(dir); e == nil {
		other.Close()
		t.Fatal("second writer acquired lock")
	}
	must(t, d.Write(Op{Key: "x", Value: "one"}))
	must(t, d.Flush())
	fname := filepath.Join(dir, d.current.Tables[0])
	must(t, d.Close())
	info, e := os.Stat(fname)
	must(t, e)
	must(t, os.Truncate(fname, info.Size()-20))
	if bad, e := Open(dir); e == nil {
		bad.Close()
		t.Fatal("missing table footer accepted")
	}
}

func TestRandomHistoryAgainstMap(t *testing.T) {
	rng := rand.New(rand.NewSource(793))
	dir := t.TempDir()
	d := open(t, dir)
	model := map[string]string{}
	for step := 0; step < 500; step++ {
		switch rng.Intn(10) {
		case 0:
			must(t, d.Flush())
		case 1:
			must(t, d.Compact())
		case 2:
			must(t, d.Close())
			d = open(t, dir)
		default:
			var ops []Op
			for n := rng.Intn(4) + 1; n > 0; n-- {
				k := strconv.Itoa(rng.Intn(35))
				v := strconv.Itoa(step)
				del := rng.Intn(4) == 0
				ops = append(ops, Op{k, v, del})
				if del {
					delete(model, k)
				} else {
					model[k] = v
				}
			}
			must(t, d.Write(ops...))
		}
		if got := all(d.Snapshot()); !reflect.DeepEqual(got, model) {
			t.Fatalf("step %d: got %v want %v", step, got, model)
		}
	}
}

func TestAtomicSnapshotUnderConcurrentWrites(t *testing.T) {
	d := open(t, t.TempDir())
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 150; n++ {
				s := d.Snapshot()
				a, x := s.Get("x")
				b, y := s.Get("y")
				if a != b || x != y {
					t.Error("torn read")
					return
				}
			}
		}()
	}
	for n := 0; n < 100; n++ {
		v := strconv.Itoa(n)
		must(t, d.Write(Op{Key: "x", Value: v}, Op{Key: "y", Value: v}))
		if n%13 == 0 {
			must(t, d.Flush())
		}
	}
	wg.Wait()
}

func TestAcknowledgedWriteSurvivesKilledWriter(t *testing.T) {
	if dir := os.Getenv("STRATA_CRASH_TEST_DIR"); dir != "" {
		d, e := Open(dir)
		must(t, e)
		must(t, d.Write(Op{Key: "confirmed", Value: "yes"}))
		fmt.Println("ACK")
		for {
			time.Sleep(time.Second)
		}
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestAcknowledgedWriteSurvivesKilledWriter$")
	cmd.Env = append(os.Environ(), "STRATA_CRASH_TEST_DIR="+dir)
	stdout, e := cmd.StdoutPipe()
	must(t, e)
	must(t, cmd.Start())
	defer cmd.Process.Kill()
	ack, e := bufio.NewReader(stdout).ReadString('\n')
	must(t, e)
	if strings.TrimSpace(ack) != "ACK" {
		t.Fatal(ack)
	}
	must(t, cmd.Process.Kill())
	if cmd.Wait() == nil {
		t.Fatal("expected killed child")
	}
	if v, ok := open(t, dir).Get("confirmed"); !ok || v != "yes" {
		t.Fatal("lost acknowledged write")
	}
}

func BenchmarkSnapshotPointRead(b *testing.B) {
	d := open(b, b.TempDir())
	var batch []Op
	for i := 0; i < 10000; i++ {
		batch = append(batch, Op{Key: fmt.Sprintf("k%05d", i), Value: "data"})
	}
	must(b, d.Write(batch...))
	must(b, d.Flush())
	s := d.Snapshot()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s.Get("k03123")
	}
}
