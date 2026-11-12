package strata

import (
	"container/heap"
	"sort"
)

type Snapshot struct {
	tables   []table
	Sequence uint64
}

func sorted(m map[string]Op) table {
	t := make(table, 0, len(m))
	for _, o := range m {
		t = append(t, o)
	}
	sort.Slice(t, func(i, j int) bool { return t[i].Key < t[j].Key })
	return t
}
func (t table) seek(k string) int {
	return sort.Search(len(t), func(i int) bool { return t[i].Key >= k })
}
func lookup(ts []table, k string) (string, bool) {
	for i := len(ts) - 1; i >= 0; i-- {
		j := ts[i].seek(k)
		if j < len(ts[i]) && ts[i][j].Key == k {
			o := ts[i][j]
			return o.Value, !o.Delete
		}
	}
	return "", false
}
func (s Snapshot) Get(k string) (string, bool) { return lookup(s.tables, k) }

type cursor struct {
	rows     table
	pos, gen int
}
type cursors []cursor

func (q cursors) Len() int { return len(q) }
func (q cursors) Less(i, j int) bool {
	a, b := q[i], q[j]
	x, y := a.rows[a.pos].Key, b.rows[b.pos].Key
	if x == y {
		return a.gen > b.gen
	}
	return x < y
}
func (q cursors) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *cursors) Push(x any)   { *q = append(*q, x.(cursor)) }
func (q *cursors) Pop() any {
	last := len(*q) - 1
	entry := (*q)[last]
	*q = (*q)[:last]
	return entry
}

// Scan visits visible keys in [start,end). An empty end means unbounded.
// A false callback return stops iteration, so a small limit does not scan the tail.
func (s Snapshot) Scan(start, end string, visit func(string, string) bool) {
	q := cursors{}
	for gen, t := range s.tables {
		if pos := t.seek(start); pos < len(t) {
			q = append(q, cursor{t, pos, gen})
		}
	}
	heap.Init(&q)
	last := ""
	seen := false
	for len(q) > 0 {
		c := heap.Pop(&q).(cursor)
		o := c.rows[c.pos]
		if end != "" && o.Key >= end {
			return
		}
		if !seen || last != o.Key {
			last = o.Key
			seen = true
			if !o.Delete && !visit(o.Key, o.Value) {
				return
			}
		}
		c.pos++
		if c.pos < len(c.rows) {
			heap.Push(&q, c)
		}
	}
}
