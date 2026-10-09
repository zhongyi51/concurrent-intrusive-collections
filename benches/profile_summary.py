"""Summarize local samply samples using its --unstable-presymbolicate sidecar."""
import collections
import gzip
import json
import pathlib
import subprocess
import sys

path = pathlib.Path(sys.argv[1])
with gzip.open(path, "rt", encoding="utf-8") as source:
    profile = json.load(source)
with path.with_suffix(".syms.json").open(encoding="utf-8") as source:
    symbols = json.load(source)
tables = {table["debug_name"].lower(): table for table in symbols["data"]}
for table in tables.values():
    table["addresses"] = dict(table["known_addresses"])

# On Windows samply may fall back to PE function ranges despite a local Rust
# PDB. LLVM can resolve these addresses directly. Keep a matching EXE/PDB pair
# before rebuilding; optionally pass that saved EXE as the second argument.
resolved = {}
for lib in profile["libs"]:
    if not lib["name"].startswith("profile-"):
        continue
    table = tables.get(lib["debugName"].lower())
    if not table:
        continue
    binary = sys.argv[2] if len(sys.argv) > 2 else lib["path"]
    addresses = [str(address) for address in table["addresses"]]
    output = subprocess.check_output([
        "llvm-symbolizer", "--obj=" + binary, "--relative-address",
        "--output-style=JSON", *addresses,
    ], text=True)
    for result in json.loads(output):
        # Last entry is the enclosing physical function, rather than an inlined
        # atomic/Cell accessor. This matches a machine-code hotspot breakdown.
        symbol = result["Symbol"][-1]
        if symbol["FunctionName"]:
            resolved[(lib["debugName"].lower(), int(result["Address"], 16))] = symbol["FunctionName"]

for thread in profile["threads"]:
    frames = thread["frameTable"]
    funcs = thread["funcTable"]
    stacks = thread["stackTable"]
    strings = thread["stringArray"]

    def name(frame):
        func = frames["func"][frame]
        resource = funcs["resource"][func]
        if resource < 0:
            return strings[funcs["name"][func]]
        lib = profile["libs"][thread["resourceTable"]["lib"][resource]]
        table = tables.get(lib["debugName"].lower())
        address = frames["address"][frame]
        local_name = resolved.get((lib["debugName"].lower(), address))
        if local_name:
            return local_name
        index = table["addresses"].get(address) if table else None
        if index is not None:
            symbol = table["symbol_table"][index]
            return symbols["string_table"][symbol["symbol"]]
        return lib["name"] + ":" + strings[funcs["name"][func]]

    leaf = collections.Counter()
    inclusive = collections.Counter()
    addresses = collections.Counter()
    for stack in thread["samples"]["stack"]:
        if stack is None:
            continue
        frame = stacks["frame"][stack]
        leaf[name(frame)] += 1
        addresses[(name(frame), frames["address"][frame])] += 1
        seen = set()
        while stack is not None:
            seen.add(name(stacks["frame"][stack]))
            stack = stacks["prefix"][stack]
        inclusive.update(seen)
    total = leaf.total()
    print(f"{thread['name']}: {total} non-null sampled stacks")
    for title, counter in [("Leaf", leaf), ("Inclusive", inclusive)]:
        print(title)
        for label, count in counter.most_common(15):
            print(f"  {count:5d} {100 * count / total:6.2f}% {label}")
    print("Top leaf instruction addresses (module-relative)")
    for (label, address), count in addresses.most_common(10):
        print(f"  {count:5d} 0x{address:x} {label}")
