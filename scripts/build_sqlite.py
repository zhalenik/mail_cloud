"""Build the pinned SQLite shared library used by the container's stdlib sqlite3.

Upstream publishes SHA3-256 checksums for both the archive and sqlite3.c:
https://www.sqlite.org/download.html
https://www.sqlite.org/releaselog/3_54_1.html
The amalgamation is compiled directly; no downloaded build scripts are executed.
"""
from __future__ import annotations

import argparse
import hashlib
import io
from pathlib import Path
import subprocess
import tempfile
from urllib.request import urlopen
import zipfile


URL = "https://www.sqlite.org/2026/sqlite-amalgamation-3540100.zip"
ARCHIVE_SHA3 = "92281c86e2957f9b9305890f9ac04204caa7b677cc868dc326d942689df28e30"
SOURCE_SHA3 = "e2b869d3003b33c942b7dd7063604fbc53553e1b66609570df39b9b7b558ee81"


def build(output: Path, archive: Path | None = None) -> None:
    if archive is None:
        with urlopen(URL, timeout=60) as response:
            data = response.read(8 * 1024 * 1024)
    else:
        data = archive.read_bytes()
    if hashlib.sha3_256(data).hexdigest() != ARCHIVE_SHA3:
        raise RuntimeError("SQLite archive does not match the pinned upstream checksum")
    with zipfile.ZipFile(io.BytesIO(data)) as bundle:
        source = bundle.read("sqlite-amalgamation-3540100/sqlite3.c")
    if hashlib.sha3_256(source).hexdigest() != SOURCE_SHA3:
        raise RuntimeError("SQLite source does not match the upstream release checksum")
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="sqlite-build-") as directory:
        source_path = Path(directory) / "sqlite3.c"
        source_path.write_bytes(source)
        subprocess.run([
            "cc", "-O2", "-fPIC", "-shared", "-fstack-protector-strong",
            "-D_FORTIFY_SOURCE=2", "-DSQLITE_THREADSAFE=1",
            "-DSQLITE_ENABLE_COLUMN_METADATA", "-DSQLITE_ENABLE_FTS5",
            "-DSQLITE_ENABLE_RTREE", "-DSQLITE_ENABLE_DBSTAT_VTAB",
            "-DSQLITE_ENABLE_MATH_FUNCTIONS", "-Wl,-z,relro,-z,now",
            "-Wl,-soname,libsqlite3.so.0", str(source_path),
            "-o", str(output), "-lpthread", "-ldl", "-lm",
        ], check=True)
    print("Built SQLite 3.54.1 from the verified upstream amalgamation")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--archive", type=Path, help="Use an already downloaded, checksum-verified archive")
    args = parser.parse_args()
    build(args.output, args.archive)
