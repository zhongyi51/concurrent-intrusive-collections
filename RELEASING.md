# Releasing

The crate name is `concurrent-intrusive-collections`. Releases use matching
`Cargo.toml` versions, changelog entries, and annotated Git tags (`v0.1.0`, etc.).
Publication is permanent: a version cannot be overwritten.

## Checks

1. Update the manifest version and changelog. Update the lockfile when needed.
2. Run the checks in CONTRIBUTING.md and inspect `cargo package --list`.
3. Commit and push the intended files. Wait for CI on that exact commit.
4. `cargo publish --dry-run --locked` must succeed from the clean checkout.

The Cargo include list packages source, tests, executable examples, benchmark
Rust code, license, README, changelog, and the safety document. CI configurations,
raw profiling data, local executables/PDBs, `.tmp`, and IDE files stay outside the
crate archive. Check the extracted package before publishing a new release.

## First publication

crates.io Trusted Publishing requires an already-existing crate. Publish the
initial version using the maintainer's local Cargo credential:

```sh
cargo publish --locked
```

Then configure Trusted Publishing in the crate's settings:

- Repository owner: `zhongyi51`
- Repository: `concurrent-intrusive-collections`
- Workflow: `publish.yml`
- Environment: `crates-io`

The GitHub environment should permit release tags. No long-lived Cargo token
needs to be copied into GitHub secrets. The official setup is documented at
[crates.io](https://crates.io/docs/trusted-publishing).

After verifying the published version and configuring the publisher, push the tag:

```sh
git tag -a v0.1.0 -m "Release 0.1.0"
git push origin v0.1.0
```

The Publish workflow reruns CI. If the crate version already exists, it downloads
the official archive and checks its clean Git commit against the tag before
skipping upload. This supports the initial bootstrap and safe retries; it does
not silently accept an unrelated publication. It then creates a GitHub release
using the matching changelog entry.

## Later versions

After the version/changelog change is pushed and CI passes, push its matching tag.
The Publish workflow reruns CI, obtains a short-lived crates.io OIDC credential,
publishes the crate, and creates the GitHub release. All third-party actions are
pinned to full commit IDs. Release uploads are serialized per tag.

To retry a failed workflow, dispatch Publish on that same tag, or rerun the
failed workflow. Never move a released tag or change source beneath a published
version. If authentication fails, verify the exact repository/workflow/environment
mapping; do not bypass checks or print tokens in logs.

Verify the crates.io version, archived `.cargo_vcs_info.json`, and docs.rs build
after publication. Documentation generation can finish after the crate is indexed.
