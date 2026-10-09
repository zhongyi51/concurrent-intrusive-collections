"""Validate a release tag; make bootstrap/retries safe for existing crate versions."""
import io
import json
import os
import pathlib
import subprocess
import sys
import tarfile
import urllib.error
import urllib.request

metadata = json.loads(subprocess.check_output(
    ["cargo", "metadata", "--format-version", "1", "--no-deps"], text=True
))
package = next(p for p in metadata["packages"] if p["name"] == "concurrent-intrusive-collections")
name, version = package["name"], package["version"]
tag = os.environ.get("GITHUB_REF_NAME", "")
if os.environ.get("GITHUB_REF_TYPE") != "tag" or tag != "v" + version:
    sys.exit(f"Release must run on tag v{version}, matching Cargo.toml")

if sys.argv[1] == "notes":
    changelog = pathlib.Path("CHANGELOG.md").read_text(encoding="utf-8")
    heading = f"## [{version}]"
    if heading not in changelog:
        sys.exit("Missing changelog entry")
    notes = changelog[changelog.index(heading):].split("\n## ", 1)[0]
    print(notes.strip())
elif sys.argv[1] == "registry":
    def fetch(url):
        request = urllib.request.Request(url, headers={"User-Agent": f"{name}-release-check"})
        return urllib.request.urlopen(request, timeout=60).read()

    try:
        fetch(f"https://crates.io/api/v1/crates/{name}/{version}")
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise
        published = False
    else:
        archive = fetch(f"https://static.crates.io/crates/{name}/{name}-{version}.crate")
        # Read one member, never extract an external tarball into the workspace.
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as crate:
            info = json.load(crate.extractfile(f"{name}-{version}/.cargo_vcs_info.json"))
        if info["git"]["sha1"] != os.environ["GITHUB_SHA"] or info["git"].get("dirty", False):
            sys.exit("Existing crate version was not published from this clean commit")
        published = True
    print(f"Registry version already published from this commit: {published}")
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        output.write(f"published={str(published).lower()}\n")
else:
    sys.exit("Expected registry or notes")
