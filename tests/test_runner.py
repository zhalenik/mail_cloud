"""Offline end-to-end queue/runner tests; every server operation is simulated.

FakeCloud uses the actual CloudAPI.upload_photo state machine, replacing its
HTTP, account, quota, byte transfer and metadata boundary.  No credentials,
sockets or external service are used.
"""
from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path
import posixpath
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from mailru_photos.api import (
    AccountInfo, CloudAPI, FileIdentity, PROTOCOL_VERSION, ProtocolError,
    RemoteError, UnsupportedPhoto, cloud_hash,
)
from mailru_photos.queue import Queue
from mailru_photos.runner import ProbeRequired, QuotaGuard, probe, run
from mailru_photos.transport import Cancelled, Response, TransportError


EMAIL = "owner@example.test"


class InstantEvent(threading.Event):
    """Avoid real probe settling waits while retaining cancellation semantics."""
    def __init__(self, cancel_on_wait: int | None = None):
        super().__init__()
        self.waits = []
        self.cancel_on_wait = cancel_on_wait

    def wait(self, timeout=None):
        self.waits.append(timeout)
        if len(self.waits) == self.cancel_on_wait:
            self.set()
        return self.is_set()


class FakeCloud(CloudAPI):
    def __init__(self, *, stop=None, quota_values=None, barrier=None):
        # Deliberately do not initialize any token manager or HTTP factory.
        self.stop = stop if stop is not None else threading.Event()
        self._fake_lock = threading.RLock()
        self.remote = {}
        self.commits = []
        self.upload_calls = []
        self.failures = {}
        self.after_commit_failures = {}
        self.after_confirmation_stat_failures = {}
        self.quota_values = list(quota_values or [100])
        self.quota_rule = None
        self.quota_calls = 0
        self.account_calls = 0
        self.closed_threads = 0
        self.barrier = barrier
        self._active_uploads = 0
        self.max_active_uploads = 0

    def _check_stopped(self):
        if self.stop.is_set():
            raise Cancelled("Offline test cancellation")

    def require_account(self, expected_email=None):
        self._check_stopped()
        with self._fake_lock:
            self.account_calls += 1
        if expected_email and expected_email != EMAIL:
            raise ProtocolError("Unexpected account in offline fixture")
        return AccountInfo(EMAIL, True, 2_000_000_000)

    def quota(self):
        self._check_stopped()
        with self._fake_lock:
            self.quota_calls += 1
            if self.quota_rule is not None:
                value = self.quota_rule(self)
            else:
                value = self.quota_values[0]
                if len(self.quota_values) > 1:
                    self.quota_values.pop(0)
        if isinstance(value, BaseException):
            if isinstance(value, Cancelled):
                self.stop.set()
            raise value
        return {"used_bytes": value, "total_bytes": 100_000_000,
                "mail_bytes": 0, "combined_used_bytes": value}

    def ensure_parent(self, path):
        self._check_stopped()

    def stat(self, path):
        self._check_stopped()
        with self._fake_lock:
            value = self.remote.get(path)
            if value is not None:
                failure = self.after_confirmation_stat_failures.pop(path, None)
                if failure is not None:
                    raise failure
            return dict(value) if value else None

    def _upload_blob(self, path, digest, identity, *, progress=None, is_retried=False):
        self._check_stopped()
        with self._fake_lock:
            self.upload_calls.append(str(path))
            failures = self.failures.get(Path(path).name, [])
            failure = failures.pop(0) if failures else None
            self._active_uploads += 1
            self.max_active_uploads = max(self.max_active_uploads, self._active_uploads)
        try:
            if self.barrier is not None:
                self.barrier.wait(timeout=5)
            if failure is not None:
                if isinstance(failure, Cancelled):
                    self.stop.set()
                raise failure
        finally:
            with self._fake_lock:
                self._active_uploads -= 1

    def _request(self, method, url, *, body=b"", **kwargs):
        self._check_stopped()
        if method != "POST" or not url.endswith("/api/v1/private/add"):
            raise AssertionError(f"Unexpected simulated request: {method} {url}")
        payload = json.loads(body)
        if payload.get("unlimited_upload") is not True or payload.get("overwrite") is not False:
            raise AssertionError("The runner must keep unlimited and no-overwrite commit options")
        with self._fake_lock:
            self.remote[payload["path"]] = {"hash": payload["hash"], "size": payload["size"]}
            self.commits.append(payload)
            failure = self.after_commit_failures.pop(payload["path"], None)
        if failure is not None:
            raise failure
        response = {"path": payload["path"], "name": posixpath.basename(payload["path"]),
                    "nodeid": "offline-node-" + payload["hash"][:8], "is_unlim_upload": True}
        return Response(200, {}, json.dumps(response).encode())

    def close_thread(self):
        with self._fake_lock:
            self.closed_threads += 1


class EagerGuard(QuotaGuard):
    def __init__(self, api, account, baseline):
        super().__init__(api, account, baseline, seconds=0, every_files=1)


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.photos = self.root / "photos"
        self.photos.mkdir()
        self.queue = Queue(self.root / "queue.db", destination="/Archive", account_id=EMAIL)
        self.queue.add_root(self.photos, "camera")

    def tearDown(self):
        self.queue.close()
        self.tmp.cleanup()

    def make_photos(self, count):
        for index in range(count):
            (self.photos / f"{index:03}.jpg").write_bytes(b"offline-photo-fixture:" + str(index).encode() + b"x" * 128)
        scanned = self.queue.scan()
        self.queue.set_metadata("last_scan", asdict(scanned))
        self.assertTrue(scanned.complete)

    def seed_pass(self):
        evidence = {"result": "provisional_pass", "protocol": PROTOCOL_VERSION,
                    "account": EMAIL, "destination": self.queue.destination,
                    "before": {"used_bytes": 100}, "finished_at": time.time() - 1}
        self.queue.set_metadata("probe", evidence)
        return evidence

    def test_scan_and_probe_are_required_before_run(self):
        api = FakeCloud()
        with self.assertRaises(ProbeRequired):
            run(self.queue, api, workers=1)
        self.make_photos(1)
        with self.assertRaises(ProbeRequired):
            run(self.queue, api, workers=1)
        self.assertEqual(api.commits, [])

    def test_explicitly_skipped_probe_uploads_without_fabricating_probe_evidence(self):
        self.make_photos(3)
        api = FakeCloud(stop=InstantEvent())
        result = run(self.queue, api, workers=2, require_probe=False)
        self.assertEqual(result["outcome"], "complete")
        self.assertEqual(result["stats"]["uploaded"], 3)
        self.assertIsNone(self.queue.get_metadata("probe"))
        self.assertFalse(self.queue.get_metadata("last_run")["preflight_check"])
        self.assertEqual(api.stop.waits, [])
        for job in self.queue.list_jobs():
            proof = self.queue.upload_history(job["id"])[-1]["verified_metadata"]
            self.assertIs(proof["is_unlim_upload"], True)
            self.assertIs(proof["stat_matched"], True)
        with self.assertRaises(ProbeRequired):
            run(self.queue, FakeCloud(), workers=1)

    def test_skipping_probe_does_not_accept_missing_unlimited_confirmation(self):
        self.make_photos(3)
        api = FakeCloud()
        request = api._request
        def unconfirmed(*args, **kwargs):
            response = request(*args, **kwargs)
            payload = json.loads(response.body)
            payload["is_unlim_upload"] = False
            return Response(200, {}, json.dumps(payload).encode())
        api._request = unconfirmed
        result = run(self.queue, api, workers=1, require_probe=False)
        self.assertTrue(result["stopped"])
        self.assertEqual(result["stats"]["uploaded"], 0)
        self.assertEqual(result["stats"]["failed"], 1)
        self.assertEqual(result["stats"]["pending"], 2)
        self.assertEqual(len(api.commits), 1)

    def test_skipping_probe_keeps_quota_monitoring(self):
        self.make_photos(5)
        api = FakeCloud()
        api.quota_rule = lambda client: 200 if len(client.commits) >= 2 else 100
        with patch("mailru_photos.runner.QuotaGuard", EagerGuard):
            result = run(self.queue, api, workers=1, require_probe=False)
        self.assertTrue(result["stopped"])
        self.assertEqual(result["stats"]["uploaded"], 2)
        self.assertEqual(result["stats"]["pending"], 3)

    def test_skipping_probe_still_requires_scan_and_explicit_boolean(self):
        api = FakeCloud()
        with self.assertRaises(ProbeRequired):
            run(self.queue, api, require_probe=False)
        for value in (None, 0, 1, "false"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                run(self.queue, api, require_probe=value)
        self.assertEqual(api.commits, [])

    def test_full_commit_hash_size_and_two_quota_samples_qualify(self):
        self.make_photos(5)
        api = FakeCloud(stop=InstantEvent())
        evidence = probe(self.queue, api, count=3, settle_seconds=30)
        self.assertEqual(evidence["result"], "provisional_pass")
        self.assertEqual(evidence["per_file_unlimited_flag"], "is_unlim_upload_in_add_response")
        self.assertEqual(len(evidence["samples"]), 3)
        self.assertEqual(len(evidence["measurements"]), 2)
        self.assertEqual(api.stop.waits, [15, 15])
        for sample in evidence["samples"]:
            self.assertTrue(sample["commit_accepted"])
            self.assertTrue(sample["stat_matched"])
            self.assertFalse(sample["recovered_commit"])
            self.assertIs(sample["is_unlim_upload"], True)
            self.assertEqual(sample["unlimited_evidence"], "commit_response")
            self.assertRegex(sample["hash"], r"^[A-F0-9]{40}$")
        result = run(self.queue, api, workers=2)
        self.assertEqual(result["outcome"], "complete")
        self.assertEqual(result["stats"]["uploaded"], 5)
        self.assertEqual(len(api.commits), 5)

    def test_changed_quota_during_probe_blocks_bulk_run(self):
        self.make_photos(3)
        api = FakeCloud(stop=InstantEvent(), quota_values=[100, 120, 120])
        evidence = probe(self.queue, api, count=1, settle_seconds=30)
        self.assertEqual(evidence["result"], "quota_changed")
        self.assertEqual(self.queue.stats()["uploaded"], 1)
        with self.assertRaises(ProbeRequired):
            run(self.queue, api, workers=1)
        self.assertEqual(len(api.commits), 1)

    def test_cancelled_probe_preserves_successful_samples_and_releases_current_claim(self):
        self.make_photos(3)
        second = self.queue.list_jobs()[1]
        api = FakeCloud(stop=InstantEvent())
        api.failures[Path(second["local_path"]).name] = [Cancelled("test pause")]
        with self.assertRaises(Cancelled):
            probe(self.queue, api, count=3, settle_seconds=30)
        evidence = self.queue.get_metadata("probe")
        self.assertEqual(evidence["result"], "interrupted")
        self.assertEqual(len(evidence["samples"]), 1)
        self.assertEqual(self.queue.stats()["uploaded"], 1)
        self.assertEqual(self.queue.stats()["pending"], 2)
        self.assertEqual(self.queue.stats()["running"], 0)
        with self.assertRaises(ProbeRequired):
            run(self.queue, FakeCloud(), workers=1)

    def test_cancelled_probe_settling_keeps_already_confirmed_samples(self):
        self.make_photos(2)
        api = FakeCloud(stop=InstantEvent(cancel_on_wait=1))
        with self.assertRaises(Cancelled):
            probe(self.queue, api, count=1, settle_seconds=30)
        report = self.queue.get_metadata("probe")
        self.assertEqual(report["result"], "interrupted")
        self.assertEqual(len(report["samples"]), 1)
        self.assertEqual(report["measurements"], [])
        self.assertEqual(self.queue.stats()["uploaded"], 1)

    def test_corrupt_or_stale_probe_evidence_never_opens_gate(self):
        self.make_photos(1)
        changes = [
            {"result": "needs_recheck"}, {"account": "someone-else@example.test"},
            {"destination": "/Elsewhere"}, {"protocol": "another-protocol"},
            {"finished_at": time.time() - 86401}, {"finished_at": "bad-timestamp"},
            {"finished_at": float("inf")}, {"before": {}},
        ]
        for change in changes:
            with self.subTest(change=change):
                evidence = self.seed_pass()
                evidence.update(change)
                # Infinity is deliberately not representable by safe queue
                # metadata; exercise the gate using get_metadata's boundary.
                if change.get("finished_at") == float("inf"):
                    original = self.queue.get_metadata
                    with patch.object(self.queue, "get_metadata", side_effect=lambda key, default=None: evidence if key == "probe" else original(key, default)):
                        with self.assertRaises(ProbeRequired):
                            run(self.queue, FakeCloud(), workers=1)
                else:
                    self.queue.set_metadata("probe", evidence)
                    with self.assertRaises(ProbeRequired):
                        run(self.queue, FakeCloud(), workers=1)
        self.assertEqual(self.queue.stats()["uploaded"], 0)

    def test_multithreaded_run_claims_every_photo_once(self):
        self.make_photos(8)
        self.seed_pass()
        api = FakeCloud(barrier=threading.Barrier(4))
        result = run(self.queue, api, workers=4)
        self.assertFalse(result["stopped"])
        self.assertEqual(result["outcome"], "complete")
        self.assertEqual(result["stats"]["uploaded"], 8)
        self.assertEqual(len({entry["path"] for entry in api.commits}), 8)
        self.assertEqual(len(api.commits), 8)
        self.assertEqual(api.max_active_uploads, 4)
        self.assertEqual(api.closed_threads, 4)

    def test_lost_add_response_cannot_confirm_unlimited_or_complete_upload(self):
        self.make_photos(1)
        self.seed_pass()
        api = FakeCloud()
        remote_path = self.queue.list_jobs()[0]["remote_path"]
        api.after_commit_failures[remote_path] = TransportError("Response lost after remote commit")
        started = time.monotonic()
        with patch("mailru_photos.runner.random.random", return_value=0):
            result = run(self.queue, api, workers=1, max_attempts=2)
        self.assertLess(time.monotonic() - started, 8)
        self.assertEqual(result["outcome"], "stopped")
        self.assertEqual(result["stats"]["uploaded"], 0)
        self.assertEqual(result["stats"]["failed"], 1)
        self.assertEqual(len(api.commits), 1)
        row = self.queue.list_jobs()[0]
        self.assertEqual(row["attempts"], 2)
        self.assertEqual(self.queue.upload_history(row["id"]), [])
        intent = json.loads(self.queue._db.execute("SELECT intent_json FROM commit_intents WHERE job_id=?", (row["id"],)).fetchone()[0])
        self.assertIsNot(intent.get("unlimited_confirmed"), True)
        self.assertEqual(self.queue.get_metadata("probe")["result"], "needs_recheck")

    def test_confirmed_unlimited_commit_survives_interrupted_stat_and_recovers(self):
        self.make_photos(1)
        self.seed_pass()
        api = FakeCloud()
        remote_path = self.queue.list_jobs()[0]["remote_path"]
        api.after_confirmation_stat_failures[remote_path] = Cancelled("Interrupted after unlimited confirmation")
        first = run(self.queue, api, workers=1)
        self.assertEqual(first["outcome"], "stopped")
        self.assertEqual(first["stats"]["uploaded"], 0)
        self.assertEqual(first["stats"]["pending"], 1)
        saved = json.loads(self.queue._db.execute("SELECT intent_json FROM commit_intents").fetchone()[0])
        self.assertIs(saved["unlimited_confirmed"], True)
        self.assertGreater(saved["unlimited_confirmed_at"], 0)
        api.stop.clear()
        resumed = run(self.queue, api, workers=1)
        self.assertEqual(resumed["outcome"], "complete")
        self.assertEqual(resumed["stats"]["uploaded"], 1)
        self.assertEqual(len(api.commits), 1)
        row = self.queue.list_jobs()[0]
        evidence = self.queue.upload_history(row["id"])[0]["verified_metadata"]
        self.assertIs(evidence["is_unlim_upload"], True)
        self.assertTrue(evidence["recovered_commit"])
        self.assertFalse(evidence["commit_accepted"])
        self.assertEqual(evidence["unlimited_evidence"], "saved_commit_response")
        self.assertEqual(self.queue._db.execute("SELECT COUNT(*) FROM commit_intents").fetchone()[0], 0)

    def test_no_intent_means_existing_matching_file_is_a_conflict(self):
        self.make_photos(1)
        self.seed_pass()
        api = FakeCloud()
        row = self.queue.list_jobs()[0]
        digest, identity = cloud_hash(row["local_path"])
        api.remote[row["remote_path"]] = {"hash": digest, "size": identity.size}
        result = run(self.queue, api, workers=1)
        self.assertEqual(result["outcome"], "finished_with_errors")
        self.assertEqual(result["stats"]["uploaded"], 0)
        self.assertEqual(result["stats"]["failed"], 1)
        self.assertEqual(api.commits, [])
        self.assertEqual(api.upload_calls, [])

    def test_retry_limit_is_terminal_and_is_not_reported_as_complete(self):
        self.make_photos(1)
        self.seed_pass()
        api = FakeCloud()
        name = Path(self.queue.list_jobs()[0]["local_path"]).name
        api.failures[name] = [RemoteError(503)]
        result = run(self.queue, api, workers=1, max_attempts=1)
        self.assertEqual(result["outcome"], "finished_with_errors")
        self.assertEqual(result["stats"]["permanent_failures"], 1)
        self.assertEqual(len(api.upload_calls), 1)
        self.assertEqual(self.queue.get_metadata("probe")["result"], "provisional_pass")

    def test_skipped_photos_are_explicit_in_the_final_outcome(self):
        self.make_photos(2)
        self.seed_pass()
        api = FakeCloud()
        name = Path(self.queue.list_jobs()[0]["local_path"]).name
        api.failures[name] = [UnsupportedPhoto("unsupported offline fixture")]
        result = run(self.queue, api, workers=1)
        self.assertEqual(result["outcome"], "finished_with_skips")
        self.assertEqual(result["stats"]["skipped"], 1)
        self.assertEqual(result["stats"]["uploaded"], 1)

    def test_quota_rise_mid_run_stops_remaining_work_and_invalidates_probe(self):
        self.make_photos(90)
        self.seed_pass()
        api = FakeCloud()
        api.quota_rule = lambda client: 200 if len(client.commits) >= 50 else 100
        result = run(self.queue, api, workers=3)
        self.assertTrue(result["stopped"])
        self.assertEqual(result["outcome"], "stopped")
        self.assertGreaterEqual(result["stats"]["uploaded"], 50)
        self.assertLess(result["stats"]["uploaded"], 90)
        self.assertGreater(result["stats"]["pending"], 0)
        self.assertEqual(result["stats"]["running"], 0)
        self.assertEqual(self.queue.get_metadata("probe")["result"], "needs_recheck")

    def test_cancellation_inside_worker_quota_guard_is_saved_without_revoking_probe(self):
        self.make_photos(2)
        self.seed_pass()
        api = FakeCloud(quota_values=[100, Cancelled("cancel during quota")])
        with patch("mailru_photos.runner.QuotaGuard", EagerGuard):
            result = run(self.queue, api, workers=1)
        self.assertTrue(result["stopped"])
        self.assertEqual(self.queue.get_metadata("last_run")["outcome"], "stopped")
        self.assertEqual(self.queue.get_metadata("probe")["result"], "provisional_pass")
        self.assertEqual(self.queue.stats()["running"], 0)

    def test_cancellation_inside_final_quota_guard_is_also_saved(self):
        self.make_photos(0)
        self.seed_pass()
        api = FakeCloud(quota_values=[100, Cancelled("cancel final quota")])
        result = run(self.queue, api, workers=1)
        self.assertTrue(result["stopped"])
        self.assertEqual(self.queue.get_metadata("last_run")["outcome"], "stopped")
        self.assertEqual(self.queue.get_metadata("probe")["result"], "provisional_pass")

    def test_network_outage_in_guard_preserves_prior_probe(self):
        self.make_photos(2)
        self.seed_pass()
        api = FakeCloud(quota_values=[100, TransportError("temporary offline network")])
        with patch("mailru_photos.runner.QuotaGuard", EagerGuard):
            result = run(self.queue, api, workers=1)
        self.assertTrue(result["stopped"])
        self.assertTrue(result["errors"])
        self.assertEqual(self.queue.get_metadata("probe")["result"], "provisional_pass")

    def test_interrupt_on_last_attempt_can_resume_without_a_new_probe(self):
        self.make_photos(1)
        self.seed_pass()
        api = FakeCloud()
        name = Path(self.queue.list_jobs()[0]["local_path"]).name
        api.failures[name] = [KeyboardInterrupt()]
        interrupted = run(self.queue, api, workers=1, max_attempts=1)
        self.assertTrue(interrupted["stopped"])
        self.assertEqual(self.queue.get_metadata("probe")["result"], "provisional_pass")
        resumed = run(self.queue, FakeCloud(), workers=1, max_attempts=1)
        self.assertEqual(resumed["outcome"], "complete")
        self.assertEqual(resumed["stats"]["uploaded"], 1)
        self.assertEqual(self.queue.list_jobs()[0]["attempts"], 1)

    def test_cancelled_last_attempt_is_released_without_spending_retry(self):
        self.make_photos(1)
        self.seed_pass()
        api = FakeCloud()
        name = Path(self.queue.list_jobs()[0]["local_path"]).name
        api.failures[name] = [Cancelled("pause upload")]
        first = run(self.queue, api, workers=1, max_attempts=1)
        self.assertTrue(first["stopped"])
        row = self.queue.list_jobs()[0]
        self.assertEqual((row["status"], row["attempts"]), ("pending", 0))
        second = run(self.queue, FakeCloud(), workers=1, max_attempts=1)
        self.assertEqual(second["outcome"], "complete")

    def test_incomplete_upload_evidence_cannot_qualify_probe_or_mark_uploaded(self):
        self.make_photos(1)
        api = FakeCloud(stop=InstantEvent())

        def incomplete_upload(path, remote_path, *, expected, **kwargs):
            return {"hash": "A" * 40, "size": expected.size, "path": remote_path,
                    "commit_accepted": True, "recovered_commit": False, "stat_matched": False}

        api.upload_photo = incomplete_upload
        with self.assertRaises(ProtocolError):
            probe(self.queue, api, count=1, settle_seconds=30)
        self.assertEqual(self.queue.stats()["uploaded"], 0)
        self.assertEqual(self.queue.get_metadata("probe")["result"], "failed")

    def test_non_true_or_missing_unlimited_flag_cannot_qualify_even_with_saved_confirmation(self):
        cases = ({}, {"is_unlim_upload": False}, {"is_unlim_upload": 1},
                 {"is_unlim_upload": "true"}, {"is_unlim_upload": None})
        self.make_photos(len(cases))
        for flag in cases:
            with self.subTest(flag=flag):
                api = FakeCloud(stop=InstantEvent())

                def incomplete_upload(path, remote_path, *, expected, save_intent, save_confirmation, **kwargs):
                    digest = "A" * 40
                    save_intent(digest)
                    save_confirmation(digest)
                    return {"hash": digest, "size": expected.size, "path": remote_path,
                            "commit_accepted": True, "recovered_commit": False, "stat_matched": True,
                            "unlimited_evidence": "commit_response", **flag}

                api.upload_photo = incomplete_upload
                with self.assertRaises(ProtocolError):
                    probe(self.queue, api, count=1, settle_seconds=30)
                self.assertEqual(self.queue.stats()["uploaded"], 0)
                self.assertEqual(self.queue.get_metadata("probe")["result"], "failed")

    def test_unlimited_result_without_durable_confirmation_cannot_mark_uploaded(self):
        self.make_photos(1)
        api = FakeCloud(stop=InstantEvent())

        def unpersisted_upload(path, remote_path, *, expected, save_intent, **kwargs):
            digest = "A" * 40
            save_intent(digest)
            return {"hash": digest, "size": expected.size, "path": remote_path,
                    "commit_accepted": True, "recovered_commit": False, "stat_matched": True,
                    "is_unlim_upload": True, "unlimited_evidence": "commit_response"}

        api.upload_photo = unpersisted_upload
        with self.assertRaises(ProtocolError):
            probe(self.queue, api, count=1, settle_seconds=30)
        self.assertEqual(self.queue.stats()["uploaded"], 0)
        self.assertEqual(self.queue.get_metadata("probe")["result"], "failed")


class ProbeTimingValidationTests(unittest.TestCase):
    def test_nonfinite_or_unbounded_probe_wait_is_rejected_before_network(self):
        for value in (float("nan"), float("inf"), 86401):
            with self.subTest(value=value), self.assertRaises(ValueError):
                probe(None, None, settle_seconds=value)


if __name__ == "__main__":
    unittest.main()
