# Contributing

Use Rust 1.85 or newer. Keep link traits minimal and keep unsafe invariants close
to the code. See [the safety audit](docs/SAFETY.md) before changing memory ordering,
publication, ownership, tags, or retirement.

Run before submitting:

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features
cargo test --locked --release --all-features
cargo test --locked --no-default-features
cargo run --locked --example work_queue
cargo package --locked
```

Use `cargo check --all-targets` to compile benchmarks. Avoid `cargo test
--all-targets`: the custom benchmark binaries intentionally run sustained workloads.
To run Miri, use the command and limitations documented in the README. CI pins
nightly-2026-01-30 for reproducibility; update that pin deliberately with evidence.

For concurrent changes, test exact unique IDs and destructor counts, not only
aggregate counts. Keep callback timing separate from linearization order. Avoid
unbounded waits in tests. Changes to strict FIFO behavior should exercise the
bounded-history checker as well as targeted race tests.

Performance changes should include before/after measurements using the same
workload and operation accounting. Run benchmarks without competing profilers or
tests. Keep regressions and workload limitations in the report. See
[benchmark instructions](benches/README.md) and [profiling instructions](benches/PROFILING.md).

Pull requests should explain the behavior changed, the relevant invariant, and
the checks run. Do not commit `.tmp`, `target`, credentials, local profiler
binaries, or IDE configuration. Release steps are in [RELEASING.md](RELEASING.md).
