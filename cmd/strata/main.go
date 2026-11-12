package main

import (
	"encoding/json"
	"errors"
	"fmt"
	store "github.com/chaitanyature/strata-lsm"
	"os"
)

func run(args []string) error {
	usage := errors.New("usage: strata DIR put KEY VALUE | get KEY | del KEY | batch JSON | scan [START [END]] | flush | compact")
	if len(args) < 2 {
		return usage
	}
	d, e := store.Open(args[0])
	if e != nil {
		return e
	}
	defer d.Close()
	rest := args[2:]
	switch args[1] {
	case "put":
		if len(rest) == 2 {
			return d.Write(store.Op{Key: rest[0], Value: rest[1]})
		}
	case "get":
		if len(rest) == 1 {
			if v, ok := d.Get(rest[0]); ok {
				fmt.Println(v)
				return nil
			}
			return errors.New("key absent")
		}
	case "del":
		if len(rest) == 1 {
			return d.Write(store.Op{Key: rest[0], Delete: true})
		}
	case "batch":
		if len(rest) == 1 {
			var ops []store.Op
			if e = json.Unmarshal([]byte(rest[0]), &ops); e != nil {
				return e
			}
			return d.Write(ops...)
		}
	case "scan":
		if len(rest) <= 2 {
			bounds := [2]string{}
			copy(bounds[:], rest)
			out := json.NewEncoder(os.Stdout)
			var err error
			d.Snapshot().Scan(bounds[0], bounds[1], func(k, v string) bool { err = out.Encode([2]string{k, v}); return err == nil })
			return err
		}
	case "flush":
		if len(rest) == 0 {
			return d.Flush()
		}
	case "compact":
		if len(rest) == 0 {
			return d.Compact()
		}
	}
	return usage
}
func main() {
	if e := run(os.Args[1:]); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
