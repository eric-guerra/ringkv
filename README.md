# RingKV

A **Dynamo-style distributed key-value store** in Java and Netty: consistent hashing with virtual nodes, N/W/R quorum replication, hybrid logical clocks, read repair, sloppy quorums around failed nodes, and a write-ahead log for durability.

> Portfolio reference implementation of a 2011 distributed-systems project, built with modern Java (records, sealed interfaces, pattern-matching `switch`).

## Architecture

```
client ──TCP line protocol──▶ any node (coordinator)
                                 │  ring.preferenceList(key, N), skipping suspect nodes
                ┌────────────────┼────────────────┐
                ▼                ▼                ▼
            replica A        replica B        replica C      RPUT/RGET over pooled Netty connections
         Memory + WAL      Memory + WAL      Memory + WAL
```

| Concern | Implementation |
|---|---|
| Partitioning | `ConsistentHashRing`: 64-bit FNV-1a with a murmur finalizer, 128 vnodes per node; adding a node moves about 1/N of the keys, all to the new node |
| Replication | Coordinator fans out to N replicas, succeeds after W acks or R reads; `R + W > N` gives read-your-writes |
| Versioning | `HybridClock`: 48-bit millis plus a 16-bit counter. Monotonic, advances past remote timestamps, tolerates clock steps backwards |
| Conflicts | Last-writer-wins on (timestamp, writer id). Deletes are **tombstones**, so delayed older writes cannot resurrect keys |
| Failure detection | Heartbeat pings; peers silent beyond `suspectAfterMs` are skipped and the next ring node takes the write (sloppy quorum) |
| Anti-entropy | **Read repair**: after all replicas answer, stale or missing replicas receive the winning version |
| Durability | `WriteAheadLog` with a CRC32 per record and optional fsync. Replay stops cleanly at torn or corrupt tails; atomic-rename compaction runs when the log exceeds 3× the live keys |
| Networking | Netty server with in-order pipelined replies; client transport with per-peer pooled connections, FIFO response matching and timeouts |

## Wire protocol

```
GET key            → VAL <b64> <ts> <writer> | NIL | ERR CODE msg
PUT key <b64>      → OK | ERR UNAVAILABLE|QUORUM_FAILED msg
DEL key            → OK
RGET / RPUT / PING   (node-to-node)
```

## Run a 4-node cluster

```bash
docker compose up --build
mvn -q exec:java -Dexec.mainClass=com.ericguerra.ringkv.cli.KvCli -Dexec.args="localhost 7001"
ringkv> put user:42 {"name":"Ada"}
OK
ringkv> get user:42
{"name":"Ada"}   (ts=..., writer=n1)
docker compose stop node2     # writes and reads keep working
```

## Test

```bash
mvn test
```

Tests cover ring balance, key movement on membership change and remove/add symmetry; codec round-trips and malformed input; last-writer-wins and tombstone ordering; WAL replay, torn-write and corrupt-checksum handling, and compaction; HLC monotonicity; and a 5-node in-process cluster (one replica down, quorum failure, sloppy quorum after failure detection, read repair of a stale replica, unavailability).
