# Allocation and CPU profiling: 2026-10-09

> Historical measurements below use the 0.1.0 API names. In 0.2.0 the public
> queue is `ConcurrentShardedQueue`; the FIFO shard is private. Current benchmarks
> compare the public queue at 1, 8, and 32 shards, so new results do not include
> the old direct `ms` baseline.

The library changes are limited to `src/fifo_queue.rs`: co-locate the initial
dummy link with head, hint inlining of the shard pop routine, and skip loading
tail when a validated head has no successor. There is no node pool, allocation
cache, extra shared state, API change, or weakening of FIFO/reclamation contracts.

## Allocation findings

`profile.rs` wraps `System` with an allocation-free counting allocator. Each phase
enables counters only around the operation under inspection; printing, node
preallocation, and collector draining are outside the measured phase. Counts
include Crossbeam work triggered by that operation. This instrumentation is for
allocation attribution, **not** for throughput timing.

On this Windows x86_64 host:

| Phase | Calls measured | Heap allocations |
| --- | ---: | ---: |
| First `epoch::pin()` | 1 | 3, requesting 5,016 bytes total |
| Construct strict FIFO | 1 | 0 |
| Construct 32-shard queue | 1 | 1: **12,288 -> 8,192 bytes** |
| Construct 32-shard stack | 1 | 1: 4,096 bytes |
| Warm empty pop (each collection) | 8,192 | 0 |
| Queue state queries | 16,384 | 0 |
| Queue push with preallocated nodes | 8,192 | 0 |
| Stack push with preallocated nodes | 8,192 | 0 |
| Queue pop | 8,192 | 129 allocations of 2,072 bytes |
| Stack pop | 8,192 | 130 allocations of 2,072 bytes |
| Direct EBR retirement, without a collection | 8,192 | 128 allocations of 2,072 bytes |
| Callback capturing 512 bytes | 1 | 0 |
| Close 32-shard queue | 1 | 0 |

There is no extra node-wrapper allocation or heap-boxed callback in these paths.
The pop allocations are EBR batch storage, consistent with the direct-retirement
control. Exact batch counts depend on collector state; they are not per-node
allocations or a promise of fixed counts. Crossbeam 0.9.21 stores small deferred
closures inline (`deferred.rs`); a `defer_destroy` closure fits that inline space.
Caller-created `Owned::new` node allocations remain necessary with this ownership
API and are deliberately outside the preallocated-push phase.

The old strict queue occupied 384 bytes: two 128-byte `CachePadded` fields plus
an eight-byte `initial_next` and alignment padding. `Head { pointer, initial_next }`
now shares one padded block, reducing each shard to 256 bytes. Head and tail
remain isolated. The 24-byte sharded-queue handle is unchanged. These exact sizes
are platform-dependent; Crossbeam's padded block is not necessarily one physical
cache line.

[Before allocation counts](results/2026-10-09-profile-before-alloc.csv) and
[after counts](results/2026-10-09-profile-after-alloc.csv) are recorded. Stack
push/pop attribution was added to the final profiling harness; stack code did
not change.

## CPU findings

Samply/Windows ETW sampled only the profiling program's main thread at 1 kHz for
five-second workloads. The release profile included debug information. Local
`llvm-symbolizer` resolved Rust PDB addresses where samply's sidecar contained
only PE function ranges. Reported leaf percentages group by physical function,
including its inlined accessors; inclusive percentages must not be added together.

The original empty-32-shard workload produced 5,018 sampled stacks. **58.09%** of
leaf samples fell inside the separate `ConcurrentFifoQueue::pop_with_guard`
function and **29.53%** in the sharded pop loop. Disassembly showed a six-register
function prologue and a tail load on every empty shard probe. The method was not
inlined into the scan despite being generic.

An ordinary `#[inline]` hint eliminates that repeated call in the measured build.
Checking the validated head's null next link before loading tail also avoids a
second padded block on empty probes. Head validation and all Acquire/Release
orderings remain; nonempty paths still help a lagging tail before retiring a
dummy. This is an empty fast path, not removal of the tail-helping protocol.

The after profile contains 5,011 stacks: the separate pop-helper frame disappears,
and **77.07%** of leaf samples fall in the combined sharded pop routine. This
demonstrates the call-path change; throughput is assessed separately below.

The original allocated round-trip profile contained 5,010 stacks. It showed
normal node allocation, epoch pin/unpin, and deferred freeing as substantial work
(for example, `RtlAllocateHeap` had 20.44% inclusive samples). Removing pinning,
pooling nodes, or bypassing EBR would require a different lifetime protocol, so
these were left intact. The callback's `Option` also remained: the measured
allocation count is zero, and eliminating it would need a broader refactor.

Saved summaries:

- [Before: empty scan](results/2026-10-09-profile-before-empty.txt)
- [Before: allocated round trip](results/2026-10-09-profile-before-roundtrip.txt)
- [After: empty scan](results/2026-10-09-profile-after-empty.txt)

The compressed raw profiles and matching original EXE/PDB pairs are in ignored
`.tmp/profile-before*` and `.tmp/profile-after*` files locally, not committed data.

## Throughput check

Same i7-12700F / 20 logical CPUs / Rust 1.93.0 host and workload accounting as
[the queue benchmark](README.md). Fixed before/after release binaries ran alone,
alternating which version ran first for each workload. Every configuration used
524,288 requested nodes, one discarded warmup, and nine measured samples. Both
allocation modes and all 1/8/32-shard plus strict-FIFO configurations were retained:
720 samples per version. Profilers and tests were not running during these trials.

For **32 shards**, allocated-EBR median Mops/s:

| Workload | Before | After | Ratio |
| --- | ---: | ---: | ---: |
| 1-worker empty polling | 11.20 | 21.26 | **1.90x** |
| 16-worker empty polling | 86.80 | 143.64 | **1.65x** |
| 1-worker round trip | 27.98 | 28.72 | 1.03x |
| 4-worker round trip | 63.30 | 63.73 | 1.01x |
| 8-worker round trip | 106.72 | 105.25 | 0.99x |
| 16-worker round trip | 146.23 | 149.52 | 1.02x |
| 1 producer / 1 consumer | 19.32 | 17.81 | 0.92x |
| 4 producers / 4 consumers | 30.17 | 35.07 | 1.16x |
| 8 producers / 8 consumers | 42.92 | 47.23 | 1.10x |
| 1 producer / 7 consumers | 12.21 | 13.46 | 1.10x |
| 7 producers / 1 consumer | 17.64 | 25.52 | 1.45x |

The 1p1c dip prompted a reversed-order, 31-sample recheck. With 32 shards it
changed from 18.36 to 20.15 Mops/s, so the initial default-configuration regression
was not reproduced. However, **8-shard allocated 1p1c remained slower**, changing
from 18.55 to 16.42 Mops/s in that recheck (about -11%). Preallocated 8-shard 1p1c
changed from 31.82 to 33.23 Mops/s. Other non-default configurations also have
small decreases in the full results. We keep the changes for the clear memory
reduction and default empty-scan improvement; this is **not an across-the-board
speedup**. CPU placement, allocator behavior, and scheduling were uncontrolled,
and sample quartiles are spreads, not statistical confidence intervals.

- Full comparison: [before raw](results/2026-10-09-profile-before-raw.csv),
  [after raw](results/2026-10-09-profile-after-raw.csv),
  [before summary](results/2026-10-09-profile-before-summary.csv),
  [after summary](results/2026-10-09-profile-after-summary.csv).
- Recheck: [before raw](results/2026-10-09-profile-recheck-before-1p1c.csv),
  [after raw](results/2026-10-09-profile-recheck-after-1p1c.csv),
  [before summary](results/2026-10-09-profile-recheck-before-1p1c-summary.csv),
  [after summary](results/2026-10-09-profile-recheck-after-1p1c-summary.csv).

## Reproduce profiling

Allocation counting needs only Cargo; native CPU sampling here used the already
installed samply, xperf, LLVM symbolizer, and Python 3.10+. No library dependency
was added.

```powershell
$env:CARGO_PROFILE_BENCH_DEBUG = '1'
cargo bench --locked --offline --bench profile
$exe = (Get-ChildItem target/release/deps/profile-*.exe | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName
samply record --save-only --no-open --main-thread-only --unstable-presymbolicate --output .tmp/empty.json.gz -- $exe --cpu empty
python benches/profile_summary.py .tmp/empty.json.gz
# Replace --cpu empty with --cpu roundtrip for allocation/GC CPU work.
```

Before rebuilding, preserve the matched EXE/PDB pair when symbolizing an older
profile; `profile_summary.py` accepts the saved EXE as its optional second argument.
For throughput comparisons, use ordinary release builds without the profiling
allocator or debug override, save each benchmark EXE, then:

```powershell
./benches/compare.ps1 -Before ./before.exe -After ./after.exe -Prefix .tmp/comparison
./benches/summarize.ps1 -InputCsv .tmp/comparison-before.csv -OutputCsv .tmp/before-summary.csv
./benches/summarize.ps1 -InputCsv .tmp/comparison-after.csv -OutputCsv .tmp/after-summary.csv
```

Release and Rust 1.85 tests pass (47 runtime + six doctests), as does strict Clippy.
All 47 runtime tests also pass Miri with the documented Crossbeam configuration
(`-Zmiri-tree-borrows -Zmiri-ignore-leaks`); the existing provenance and collector
limitations still apply.

## Box API validation (0.3.0)

Re-ran `cargo bench --locked --bench profile` after replacing public Owned inputs
with Box. See [raw counts](results/2026-10-09-box-api-alloc.csv). Preallocated push
performed **zero extra allocations for 8,192 nodes**, for both the sharded queue
and stack. Warm empty operations, state queries, and a large callback capture
also allocated zero times. Pop still incurs batched epoch metadata allocations,
as in the standalone EBR baseline. This checks allocation behavior, not a fresh
throughput comparison. Conversion preserves the complete node address, also
covered by ownership/rejection tests.
