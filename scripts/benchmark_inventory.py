#!/usr/bin/env python3
"""Reproducible bounded-page cloud-inventory benchmark, without a live account.

CloudInventory runs unchanged against a real FULL/WAL SQLite database. The
remote own-tree endpoint is generated lazily, at most 500 entries per call.
Cloud hashes and sizes in that tree are synthetic metadata, not real media.
The Linux hash-cache exercise additionally reads real, small temporary files.
This measures this machine's local indexing, not Mail networking or NAS speed.
"""
from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
from pathlib import Path
import platform
import sqlite3
import sys
import tempfile
import threading
import time
from unittest import mock


_PEAK_OBSERVED_RSS_MB = 0.0


def rss_mb():
    global _PEAK_OBSERVED_RSS_MB
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                value = int(line.split()[1]) / 1024
                _PEAK_OBSERVED_RSS_MB = max(_PEAK_OBSERVED_RSS_MB, value)
                return value
    except OSError:
        pass
    return None


def peak_mb():
    try:
        import resource
        value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return max(_PEAK_OBSERVED_RSS_MB, value / (1024 ** 2 if sys.platform == "darwin" else 1024))
    except ImportError:
        return None


def disk_bytes(database):
    return {name: path.stat().st_size if path.exists() else 0
            for name, path in (("db", database), ("wal", Path(str(database) + "-wal")),
                               ("shm", Path(str(database) + "-shm")))}


class VirtualCloud:
    """A formula-based own tree; it never retains the whole file listing."""

    chunk_size = 10000
    original = "/Архив"
    moved_root = "/Перенесено"

    def __init__(self, count):
        self.count = count
        self.moved = 0
        self.dropped = 0
        self.revision = 1
        self.calls = 0
        self.max_page_items = 0
        self.max_requested_limit = 0

    @staticmethod
    def digest(number):
        return hashlib.sha1(f"offline inventory fixture {number}".encode()).hexdigest().upper()

    @staticmethod
    def size(number):
        return 1024 ** 2 + (number % 4096) * 1024

    def path_for(self, number, *, moved=None):
        threshold = self.moved if moved is None else moved
        base = self.moved_root if number < threshold else self.original
        return f"{base}/chunk_{number // self.chunk_size:04d}/media_{number:06d}.jpg"

    def item(self, number):
        result = {"kind": "file", "path": self.path_for(number), "hash": self.digest(number),
                  "size": self.size(number), "nodeid": str(number + 1)}
        if number % 29:
            result["is_unlim_upload"] = number % 10 != 0
        return result

    def _bounds(self, root):
        return (0, self.moved) if root == self.moved_root else (self.moved, self.count - self.dropped)

    def list_folder(self, path, *, offset=0, limit=500):
        assert type(offset) is int and offset >= 0
        assert type(limit) is int and 1 <= limit <= 500
        self.calls += 1
        self.max_requested_limit = max(self.max_requested_limit, limit)
        excluded = 0
        if path == "/":
            raw = [{"kind": "folder", "path": self.original},
                   {"kind": "folder", "path": self.moved_root},
                   {"kind": "excluded", "path": "/ForeignMount"},
                   {"kind": "excluded", "path": "/TrashCollection"}]
            total = len(raw)
            raw = raw[offset:offset + limit]
            items = [item for item in raw if item["kind"] != "excluded"]
            listed_paths = [item["path"] for item in raw]
            excluded = len(raw) - len(items)
        elif path in (self.original, self.moved_root):
            start, end = self._bounds(path)
            first = start // self.chunk_size
            stop = (end + self.chunk_size - 1) // self.chunk_size if start < end else first
            total = stop - first
            items = [{"kind": "folder", "path": f"{path}/chunk_{number:04d}"}
                     for number in range(first + offset, min(stop, first + offset + limit))]
            listed_paths = [item["path"] for item in items]
        else:
            root, suffix = path.rsplit("/", 1)
            assert root in (self.original, self.moved_root) and suffix.startswith("chunk_")
            chunk = int(suffix.removeprefix("chunk_"))
            first, last = self._bounds(root)
            start, end = max(first, chunk * self.chunk_size), min(last, (chunk + 1) * self.chunk_size)
            total = max(0, end - start)
            items = [self.item(number) for number in range(start + offset, min(end, start + offset + limit))]
            listed_paths = [item["path"] for item in items]
        listed = len(listed_paths)
        assert offset + listed <= total
        self.max_page_items = max(self.max_page_items, listed)
        following = offset + listed
        return {"path": path, "items": items, "total": total, "listed": listed, "excluded": excluded,
                "listed_paths": listed_paths, "next_offset": following if following < total else None,
                "revision": {"tree": "offline-own-tree", "rev": self.revision}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--count", type=int, default=300000)
    parser.add_argument("--hash-files", type=int, default=512)
    parser.add_argument("--workdir", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.count < 1000:
        parser.error("--count must be at least 1000")
    if args.hash_files < 10:
        parser.error("--hash-files must be at least 10")
    sys.path.insert(0, str(args.repo.resolve()))
    from mailru_photos import inventory as module
    from mailru_photos.api import ProtocolError, cloud_hash
    from mailru_photos.inventory import CloudInventory
    from mailru_photos.transport import Cancelled

    temporary = tempfile.TemporaryDirectory(prefix="mail-cloud-inventory-") if args.workdir is None else None
    folder = Path(temporary.name) if temporary else args.workdir.resolve()
    if not temporary:
        folder.mkdir(parents=True, exist_ok=True)
        if any(folder.iterdir()):
            parser.error("--workdir must be new or empty; no existing data is overwritten")
    database = folder / "inventory.sqlite3"
    world = VirtualCloud(args.count)
    stop = threading.Event()
    started = time.perf_counter()
    report = {
        "benchmark": "cloud-inventory-v0.8.0", "count": args.count, "page_size": 500,
        "environment": {"python": platform.python_version(), "sqlite": sqlite3.sqlite_version,
                        "platform": platform.platform(), "cpu_count": os.cpu_count(),
                        "inventory_source_sha256": hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest(),
                        "benchmark_source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()},
        "scope": {"production_inventory_unmodified": True, "real_sqlite": True, "journal_mode": "WAL",
                  "synchronous": "FULL", "virtual_remote_listing": True, "synthetic_remote_hashes": True,
                  "remote_pages_generated_lazily": True, "network_requests": False, "account_access": False,
                  "media_uploads_or_deletions": False, "real_local_hash_files": args.hash_files,
                  "real_local_hash_file_bytes": 4096,
                  "limitations": ["Local indexing timings describe this benchmark machine only.",
                                  "No Mail API latency, rate limiting, live response shape or service consistency is measured.",
                                  "No 1 TB archive is read; only small local files exercise the ctime cache.",
                                  "This is not a claim of throughput or total application memory on a TrueNAS server."]},
        "baseline_rss_mb": rss_mb(), "measurements": {}, "assertions": [], "plans": {},
    }
    peak_disk = {"db": 0, "wal": 0, "shm": 0}
    phase = [""]
    progress_at = [0.0]

    def emit(value):
        print(json.dumps(value, ensure_ascii=False), flush=True)

    def observe_disk():
        current = disk_bytes(database)
        for key, value in current.items():
            peak_disk[key] = max(peak_disk[key], value)
        return current

    def progress(value):
        rss_mb()
        observe_disk()
        now = time.perf_counter()
        if now - progress_at[0] >= 5:
            progress_at[0] = now
            emit({"progress": phase[0], **value, "rss_mb": rss_mb()})

    def measured(name, action):
        phase[0] = name
        before = time.perf_counter()
        result = action()
        record = {"seconds": time.perf_counter() - before, "result": result, "rss_mb": rss_mb(),
                  "peak_rss_mb": peak_mb(), "disk_bytes": observe_disk()}
        report["measurements"][name] = record
        emit({"completed": name, **record})
        return result

    def incomplete(index):
        assert not index.complete
        for callback in (lambda: index.has_size(world.size(0)),
                         lambda: index.find(world.digest(0), world.size(0), world.path_for(0))):
            try:
                callback()
            except ProtocolError:
                pass
            else:
                raise AssertionError("An old or incomplete index was incorrectly accepted as proof")

    def verify_matches(index):
        matched = relocated = 0
        modes = {"unlimited": 0, "regular": 0, "unknown": 0}
        for number in range(world.count - world.dropped):
            target = world.path_for(number, moved=0)
            found = index.find(world.digest(number), world.size(number), target)
            assert found is not None and found["path"] == world.path_for(number), number
            matched += 1
            relocated += found["path"] != target
            expected = "unknown" if number % 29 == 0 else "regular" if number % 10 == 0 else "unlimited"
            assert found["storage_mode"] == expected, (number, found["storage_mode"])
            modes[expected] += 1
        for number in range(world.count - world.dropped, world.count):
            assert index.find(world.digest(number), world.size(number), world.path_for(number)) is None, number
        assert matched == world.count - world.dropped and relocated == world.moved
        return {"matched": matched, "found_at_other_path": relocated, "deleted_absent": world.dropped,
                "actual_storage_modes": modes}

    index = CloudInventory(database)
    try:
        assert index.db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert index.db.execute("PRAGMA synchronous").fetchone()[0] == 2
        incomplete(index)

        def refresh():
            before_calls = world.calls
            result = index.refresh(world, stop=stop, progress=progress)
            assert index.complete and result["cloud_files"] == world.count - world.dropped
            assert index.db.execute("SELECT COUNT(*) FROM files").fetchone()[0] == world.count - world.dropped
            assert result["cloud_pages"] == world.calls - before_calls
            assert result["excluded"] == 2
            return {**result, "endpoint_calls": world.calls - before_calls,
                    "moved_files": world.moved, "deleted_files": world.dropped}

        measured("initial_refresh", refresh)
        measured("initial_lookup_every_file", lambda: verify_matches(index))
        world.moved, world.dropped, world.revision = args.count // 30, args.count // 60, 2
        measured("first_changed_refresh", refresh)
        measured("first_changed_lookup_every_file", lambda: verify_matches(index))
        world.moved, world.dropped, world.revision = args.count // 15, args.count // 30, 3
        measured("second_changed_refresh", refresh)
        measured("second_changed_lookup_every_file", lambda: verify_matches(index))
        report["assertions"].extend([
            "Initial listing uses real SQLite and matches every virtual remote file by full hash plus size.",
            "Two complete subsequent refreshes remove deleted paths instead of trusting the previous index.",
            "Moved files are found by content at their current different cloud path.",
            "Missing storage flag remains unknown; exact false is regular and exact true is unlimited.",
            "Every refresh rereads all cloud pages; local history never substitutes for a cloud listing.",
            "Foreign mounts and virtual trash entries are counted for pagination but never traversed.",
        ])

        def cancel_refresh():
            threshold = min(50000, (world.count - world.dropped) // 2)
            before_calls = world.calls
            last = {}
            def interrupted(value):
                nonlocal last
                last = value
                progress(value)
                if value["cloud_files"] >= threshold:
                    stop.set()
            try:
                index.refresh(world, stop=stop, progress=interrupted)
            except Cancelled:
                pass
            else:
                raise AssertionError("Mid-inventory cancellation was ignored")
            incomplete(index)
            stop.clear()
            return {"last_progress": last, "endpoint_calls": world.calls - before_calls,
                    "partial_index_rejected": True}

        measured("cancelled_refresh", cancel_refresh)
        measured("complete_refresh_after_cancellation", refresh)
        measured("recovered_lookup_every_file", lambda: verify_matches(index))
        report["assertions"].append("Cancellation invalidates partial and old matches; a following full refresh recovers.")
        assert world.max_page_items <= 500 and world.max_requested_limit == 500
        report["max_generated_page_items"] = world.max_page_items
        report["max_requested_page_limit"] = world.max_requested_limit
        report["total_virtual_endpoint_calls"] = world.calls
        lookup_plan = [tuple(row) for row in index.db.execute(
            "EXPLAIN QUERY PLAN SELECT * FROM files WHERE size=? AND hash=?",
            (world.size(0), world.digest(0)))]
        assert any("files_content" in row[3] and "SEARCH" in row[3] for row in lookup_plan)
        report["plans"]["content_lookup"] = lookup_plan
        report["assertions"].append("Content lookup uses the size/hash SQLite index; no whole-cloud Python collection is built.")

        def failed_page_cases():
            results = {}
            class BrokenCloud(VirtualCloud):
                def __init__(self, failure):
                    super().__init__(1500)
                    self.failure = failure

                def list_folder(self, path, *, offset=0, limit=500):
                    result = super().list_folder(path, offset=offset, limit=limit)
                    if path.startswith(self.original + "/chunk_") and offset == 500:
                        if self.failure == "incomplete":
                            result["items"] = []
                            result["listed_paths"] = []
                            result["listed"] = 0
                            result["next_offset"] = None
                        elif self.failure == "repeated":
                            old = super().list_folder(path, offset=0, limit=limit)
                            result["items"] = old["items"]
                            result["listed_paths"] = old["listed_paths"]
                        elif self.failure == "revision_changed":
                            result["revision"]["rev"] += 1
                    return result
            with CloudInventory(folder / "faults.sqlite3") as faulty:
                for failure in ("incomplete", "repeated", "revision_changed"):
                    cloud = BrokenCloud(failure)
                    try:
                        faulty.refresh(cloud, stop=stop)
                    except ProtocolError:
                        incomplete(faulty)
                        results[failure] = {"rejected": True, "endpoint_calls": cloud.calls}
                    else:
                        raise AssertionError(f"Invalid listing accepted: {failure}")
                recovered = faulty.refresh(VirtualCloud(1500), stop=stop)
                assert recovered["cloud_files"] == 1500
                results["subsequent_refresh"] = {"complete": True, "cloud_files": 1500}
            return results

        measured("invalid_pages", failed_page_cases)
        report["assertions"].append("Truncated pages, repeated-page paths and revision changes fail closed; next refresh succeeds.")

        local = folder / "tiny_local_media"
        local.mkdir()
        for number in range(args.hash_files):
            (local / f"media_{number:04d}.jpg").write_bytes(number.to_bytes(8, "big") * 512)

        def candidate(path):
            details = path.stat(follow_symlinks=False)
            return {"local_path": str(path), "size": details.st_size, "mtime_ns": details.st_mtime_ns,
                    "device": details.st_dev, "inode": str(details.st_ino)}

        def hash_all():
            files = 0
            for path in local.iterdir():
                index.local_hash(candidate(path), stop=stop)
                files += 1
            return {"files": files}

        with mock.patch.object(module, "cloud_hash", wraps=cloud_hash) as hasher:
            measured("local_hash_first_read", hash_all)
            assert hasher.call_count == args.hash_files
            before = hasher.call_count
            measured("local_hash_unchanged_second_read", hash_all)
            expected_new = 0 if sys.platform.startswith("linux") else args.hash_files
            assert hasher.call_count - before == expected_new
            report["measurements"]["local_hash_unchanged_second_read"]["actual_hash_calculations"] = hasher.call_count - before
            changed = max(1, args.hash_files // 10)
            for number in range(changed):
                path = local / f"media_{number:04d}.jpg"
                path.rename(local / f"renamed_{number:04d}.jpg")
            for number in range(changed, 2 * changed):
                path = local / f"media_{number:04d}.jpg"
                details = path.stat()
                old_digest, _ = index.local_hash(candidate(path), stop=stop)
                path.write_bytes(b"X" * details.st_size)
                os.utime(path, ns=(details.st_atime_ns, details.st_mtime_ns))
                assert cloud_hash(path)[0] != old_digest
            before = hasher.call_count
            measured("local_hash_renamed_and_rewritten", hash_all)
            expected_new = 2 * changed if sys.platform.startswith("linux") else args.hash_files
            assert hasher.call_count - before == expected_new
            report["measurements"]["local_hash_renamed_and_rewritten"].update(
                {"renamed": changed, "rewritten_same_size_and_mtime": changed,
                 "actual_hash_calculations": hasher.call_count - before})
            before = hasher.call_count
            measured("close_checkpoint", lambda: index.close())
            assert disk_bytes(database)["wal"] == 0
            index = CloudInventory(database)
            incomplete(index)
            measured("local_hash_after_restart", hash_all)
            expected_new = 0 if sys.platform.startswith("linux") else args.hash_files
            assert hasher.call_count - before == expected_new
            report["measurements"]["local_hash_after_restart"]["actual_hash_calculations"] = hasher.call_count - before
        report["assertions"].extend([
            "Real tiny-file hashing is cached across repeated reads and restart on Linux with unchanged identity and ctime.",
            "Renamed files are checked at the new path; same-size rewrites with restored mtime invalidate the Linux ctime cache.",
            "Closing checkpoints and truncates the inventory WAL; reopening requires a fresh remote listing before matching.",
        ])
        assert index.db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        index.close()
    gc.collect()
    report["peak_observed_disk_bytes"] = peak_disk
    report["final_disk_bytes"] = disk_bytes(database)
    report["final_rss_mb"] = rss_mb()
    report["peak_rss_mb"] = peak_mb()
    report["observed_peak_rss_mb"] = _PEAK_OBSERVED_RSS_MB
    report["total_seconds"] = time.perf_counter() - started
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    emit({"finished": True, "output": str(args.output), "seconds": report["total_seconds"],
          "peak_rss_mb": report["peak_rss_mb"], "final_disk_bytes": report["final_disk_bytes"]})
    if temporary:
        temporary.cleanup()


if __name__ == "__main__":
    main()
