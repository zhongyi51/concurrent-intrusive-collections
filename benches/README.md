# Queue benchmark

> Historical measurements below use the 0.1.0 API names. In 0.2.0 the public
> queue is `ConcurrentShardedQueue`; the FIFO shard is private. Current benchmarks
> compare the public queue at 1, 8, and 32 shards, so new results do not include
> the old direct `ms` baseline.

The later [allocation/CPU profiling report](PROFILING.md) records the compact
shard layout and empty-scan optimization, with a fresh before/after comparison.
The results below are the original sharding-selection baseline, before that change.

## Decision

Use the sharded implementation for `ConcurrentQueue`, as requested for workloads
that accept per-shard FIFO. Keep the original Michael–Scott implementation as
`ConcurrentFifoQueue` for global FIFO, atomic close, and cheap empty polling.
Sharding does not preserve the old ordering contract and is not faster everywhere.

The default shard count follows the stack: available logical parallelism rounded
up to a power of two (32 on the measured host). Do not hard-code eight from this
machine's results: eight wins several producer/consumer transfer cases, while
32 wins 16-worker local round trips. Use `with_concurrency(n)` to tune a workload.
The single-shard variant measures the routing overhead without removing contention.

## Reproduce

No additional benchmark dependencies are required. Run the benchmark alone,
without tests, Miri, or another benchmark competing for the CPU.

```powershell
New-Item -ItemType Directory -Path .tmp -Force | Out-Null
cargo bench --locked --offline --bench queue -- --items 524288 --samples 9 > .tmp/queue-benchmark.csv
./benches/summarize.ps1 -InputCsv .tmp/queue-benchmark.csv -OutputCsv .tmp/queue-summary.csv
```

`--items` sets the total node count, rounded down to a common multiple of producer
and consumer counts. Samples should be odd for the supplied median reporter.
Each mode/workload gets one discarded warmup per implementation, then nine
measured samples. Implementation order rotates and reverses between rounds.

- `roundtrip-N`: N workers each push a new node and then pop one. This measures
  local affinity and contention; a pop may consume another worker's node.
- `transfer-PpCc`: separate producer and consumer threads. Producers send fixed
  equal batches; consumers drain fixed equal quotas. This forces remote probing
  and covers asymmetric 1:7 and 7:1 workloads. There is no shared progress counter
  or `close()` in the measured interval.
- `empty-N`: N workers repeatedly attempt empty pops. Throughput counts attempts.

For nonempty workloads, **one push plus one successful pop counts as two
operations**. Failed pops are included in elapsed time but excluded from useful
operation throughput; their counts are stored in the raw CSV. A failed pop yields
before retrying. Time runs from the main thread releasing the start barrier until
all workers reach the completion barrier. Thread creation/join, TLS and collector
registration, queue construction/destruction, and final GC draining are excluded.
Worker IDs are initialized serially in role order before the timed start, so the
test has reproducible contiguous role placement rather than a lucky spawn race.
Threads are not pinned to physical CPUs.

Two modes separate algorithm cost from allocation/reclamation interactions:

- `preallocated-pinned`: nodes are allocated before timing, and an external epoch
  guard delays destruction until after timing. Queue methods still pin/unpin and
  schedule reclamation normally. This is an artificial allocation-free workload,
  not a claim about application end-to-end throughput.
- `allocated-ebr`: nodes are allocated inside the timed loop; EBR runs normally.
  Reclamation performed by operations is included. The final deferred backlog and
  retained dummies are destroyed after timing, so this does not measure the full
  allocation-to-destruction lifetime either.

Nodes contain a link, integer ID, and producer index. Destruction increments a
cache-padded per-producer counter, without a shared global counter or per-node Arc.
After each sample, all nodes must be destroyed before the next sample begins.
The harness checks successful-pop counts, ID sums, and final emptiness; the test
suite separately checks exact unique-ID sets and per-producer FIFO. Sum checks
alone would not establish absence of duplicates.

## Recorded results: 2026-10-09

Windows x86_64 MSVC, Intel Core i7-12700F (12 physical cores / 20 logical CPUs),
Rust 1.93.0, Crossbeam epoch 0.9.21, release defaults. CPU affinity, clock frequency,
OS scheduling, and background activity were not controlled. Results apply to this
host and these synthetic workloads, not an architecture-independent ranking.

The table reports median **million operations per second** for `allocated-ebr`.
Ratios compare median rates, not confidence intervals. P25/P75 sample spreads and
all preallocated results are in the summary CSV.

| Workload | Original MS | 8 shards | 32 shards (default here) | Default / MS |
| --- | ---: | ---: | ---: | ---: |
| 1-worker round trip | 29.46 | 28.26 | 28.03 | 0.95x |
| 4-worker round trip | 13.68 | 60.06 | 63.06 | 4.61x |
| 8-worker round trip | 10.94 | 105.78 | 110.68 | 10.11x |
| 16-worker round trip | 8.55 | 91.20 | 150.40 | 17.59x |
| 1 producer / 1 consumer | 18.86 | 20.33 | 18.01 | 0.96x |
| 4 producers / 4 consumers | 9.52 | 33.25 | 24.14 | 2.53x |
| 8 producers / 8 consumers | 7.41 | 72.01 | 47.26 | 6.38x |
| 1 producer / 7 consumers | 10.27 | 11.49 | 12.87 | 1.25x |
| 7 producers / 1 consumer | 10.96 | 34.12 | 24.50 | 2.24x |
| 1-worker empty polling | 78.92 | 34.48 | 11.41 | 0.14x |
| 16-worker empty polling | 339.26 | 205.12 | 88.57 | 0.26x |

The improvement is consistent across both modes for multi-producer contention,
but much of the round-trip benefit comes from local shard affinity. Empty queues
expose the O(shards) scan cost directly. Fewer active producers do not exploit all
shards, and consumers can still contend on the same populated shard. These are
reasons to keep the strict queue and configurable shard counts available.

- [All 720 measured samples](results/2026-10-09-windows-raw.csv)
- [Medians, P25/P75, and speedups](results/2026-10-09-windows-summary.csv)
