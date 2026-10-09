# Reporting a vulnerability

Report suspected memory-safety defects privately through
[GitHub private vulnerability reporting](https://github.com/zhongyi51/concurrent-intrusive-collections/security/advisories/new).
Include a minimal reproducer, crate/toolchain versions, platform, and any Miri or
sanitizer output. Do not include credentials or unrelated application data.

The latest released version is the maintained version; older versions do not
have a separate backport guarantee. This is a pre-1.0 unsafe/concurrent library.
The documented trait contracts must hold for safety; please distinguish an
invalid `unsafe impl` from a defect reproducible with a conforming implementation.

See [SAFETY.md](docs/SAFETY.md) for ownership invariants and validation limits.
