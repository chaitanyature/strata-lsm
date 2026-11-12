# Strata LSM

A crash-recoverable embedded key/value engine for Go. One writer appends an entire batch to a checksummed write-ahead log, while readers can hold immutable snapshots across flushes, compactions and database close. The implementation uses the Go standard library and Unix file locking.

## Run it

Requires Go 1.22+ on Linux or macOS. From the repository root:

```sh
go build ./cmd/strata
./strata demo-db batch '[{"Key":"order/101","Value":"queued"},{"Key":"order/102","Value":"ready"}]'
./strata demo-db flush
./strata demo-db del order/101
./strata demo-db compact
./strata demo-db scan order/ order0
# ["order/102","ready"]
go test -race ./...
```

## Read and write paths

| Operation | Behavior |
| --- | --- |
| Write | Serialize one UTF-8 batch, append its frame, sync the WAL, then apply it under a writer lock. Readers see the entire batch. |
| Get | Probe the mutable map, then binary-search immutable sorted tables newest first. A tombstone ends the search. |
| Snapshot | Copy and sort the current mutable map and retain references to immutable tables. A snapshot carries its sequence number. |
| Scan | Seek each table to the lower bound. A heap merges keys in order, preferring the newest version and suppressing tombstones. Returning false from the callback stops early. |
| Flush | Sync a sorted table, sync its directory entry, publish a synced manifest by rename, sync the directory, then recycle the WAL. |
| Compact | Merge every table and the mutable map. Drop shadowed values and tombstones, publish the replacement, then unlink old tables. Active snapshots retain their in-memory versions. |

## Recovery contract

A process kill after a successful write leaves a complete WAL frame or a checkpoint containing the batch. On open, Strata replays contiguous batches after the manifest checkpoint. Old WAL prefixes that survived checkpoint publication are harmless. A partial final frame is truncated to the last complete boundary. A complete frame with a bad checksum stops recovery and preserves the bytes for inspection. A table footer detects truncation exactly between records.

An I/O error during mutation disables further writes on that handle. Close and reopen to determine the durable state. A failing caller may encounter its batch on recovery when the underlying write succeeded but acknowledgment failed. The durability model assumes a local filesystem honoring file and directory sync and atomic same-directory rename. The process-kill test exercises process failure; power-cut behavior depends on the storage stack.

## Cost and limits

For T immutable tables, point reads do at most T binary searches. Scanning K candidate records uses O(K log(T + 1)) merge work after seeking each table. Duplicate and deleted records count toward K. Full compaction runs synchronously under the writer lock. Tables are decoded into memory on open, so size the store for RAM. Applications choose when to flush and compact. A serialized batch is limited to 32 MiB. Keys and values are UTF-8 strings and a scan uses bytewise Go string order.

## Reproduce the failure checks

```sh
go test -run 'TornBatch|CorruptComplete|ManifestWins|KilledWriter' -v
go test -run RandomHistoryAgainstMap -count=1
go test -bench SnapshotPointRead -benchmem -run '^$'
```

The tests cut a batch at every byte boundary, corrupt headers and payloads, reopen after stale-log publication, kill an acknowledged writer, and compare a seeded history of writes, deletes, flushes and reopenings with a map.
