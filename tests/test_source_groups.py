"""Logical sources, migration, bounded selections and party creation proof."""
from __future__ import annotations

import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest

from mailru_photos.queue import Queue, QueueConfigurationError, SCHEMA_VERSION, ScanCollisionError


class SourceGroupTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.db = self.base / "queue.sqlite3"
        self.queue = Queue(self.db, destination="/Archive", account_id="test-account")

    def tearDown(self):
        self.queue.close()
        self.tmp.cleanup()

    def directory(self, name):
        path = self.base / name
        path.mkdir(parents=True, exist_ok=True)
        return path

    def photo(self, directory, name="photo.jpg", contents=b"photo"):
        path = directory / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(contents)
        return path

    def group(self, name, *, owner=None):
        directory = self.directory(name)
        self.photo(directory)
        return self.queue.add_source_group([{"path": str(directory)}], name, owner_key=owner)

    def batch(self, batch_id="party-1"):
        group = self.group("owned", owner="plan-batch:" + batch_id)
        self.queue.set_active_batch(batch_id, group["id"])
        self.queue.scan(root_ids=self.queue.group_root_ids([group["id"]]))
        return group, self.queue.claim()

    def accepted(self, job, *, mode="unlimited", confirmed=True, finished=False):
        digest = "A" * 40
        self.queue.save_commit_intent(job, digest)
        if confirmed:
            self.queue.confirm_unlimited_commit(job, digest)
        receipt = {"path": job.remote_path, "size": job.size, "commit_accepted": True,
                   "is_unlim_upload": mode == "unlimited", "storage_mode": mode, "verified": finished}
        self.queue.record_cloud_observation(job, mode=mode, remote_hash=digest, remote_id="node-1", metadata=receipt)
        if finished:
            self.queue.mark_uploaded(job, remote_hash=digest, remote_id="node-1", verified_metadata=receipt)
        else:
            self.queue.mark_failed(job, "verification intentionally incomplete", retry_delay=None)
        return digest

    def test_group_scan_preserves_structure_excludes_descendants_and_root_direct_files(self):
        root = self.directory("Фотоархив")
        first = self.directory("Фотоархив/2020")
        second = self.directory("Фотоархив/2021")
        self.photo(root, "direct.jpg", b"root")
        self.photo(first, "trip/a.jpg", b"a")
        self.photo(first, "private/no.jpg", b"excluded")
        self.photo(second, "b.jpg", b"bb")
        group = self.queue.add_source_group([
            {"path": str(root), "recursive": False},
            {"path": str(first), "remote_subdir": "2020", "excluded_paths": ["private"]},
            {"path": str(second), "remote_subdir": "2021"},
        ], "Архив", remote_prefix="/Фото")
        self.assertEqual(group["member_count"], 3)
        ids = self.queue.group_root_ids([group["id"]])
        result = self.queue.scan(root_ids=ids)
        self.assertTrue(result.complete)
        self.assertEqual(result.added, 3)
        self.assertEqual({row["remote_path"] for row in self.queue.list_jobs(root_ids=ids)},
                         {"/Фото/direct.jpg", "/Фото/2020/trip/a.jpg", "/Фото/2021/b.jpg"})
        self.assertEqual({row["source_group_id"] for row in self.queue.list_jobs()}, {group["id"]})
        self.assertEqual(self.queue.source_group_stats()[0]["total_bytes"], 7)
        self.assertEqual(self.queue.source_group_members(group["id"], limit=2)["total"], 3)

    def test_overlap_check_handles_interposed_sibling_and_excluded_child(self):
        parent = self.directory("a")
        sibling = self.directory("a-b")
        child = self.directory("a/child")
        with self.assertRaises(QueueConfigurationError):
            self.queue.add_source_group([{"path": str(parent)}, {"path": str(sibling)}, {"path": str(child)}], "bad")
        self.assertEqual(self.queue.source_groups(), [])
        first = self.queue.add_source_group([{"path": str(parent), "excluded_paths": ["child"]}], "parent")
        second = self.queue.add_source_group([{"path": str(child)}], "child")
        self.assertNotEqual(first["id"], second["id"])
        with self.assertRaises(QueueConfigurationError):
            self.queue.update_source_group(first["id"], "/Archive/parent", members=[{"path": str(parent)}])
        self.assertEqual(self.queue.source_group_members(first["id"])["members"][0]["excluded_preview"], ["child"])

    def test_group_activation_is_idempotent_only_for_exact_normalized_definition(self):
        a, b = self.directory("a"), self.directory("b")
        members = [{"path": str(a), "excluded_paths": ["x/y", "x", "x-other"]}, {"path": str(b), "remote_subdir": "b"}]
        first = self.queue.add_source_group(members, "party", owner_key="plan-batch:42")
        again = self.queue.add_source_group(list(reversed(members)), "party", owner_key="plan-batch:42")
        self.assertEqual(first["id"], again["id"])
        self.assertEqual(len(self.queue.roots()), 2)
        with self.assertRaises(QueueConfigurationError):
            self.queue.add_source_group(members, "renamed", owner_key="plan-batch:42")
        self.assertEqual(self.queue.source_group_members(first["id"])["members"][0]["excluded_preview"], ["x", "x-other"])

    def test_group_edit_is_atomic_preserves_receipts_and_does_not_delete_physical_files(self):
        a, b = self.directory("a"), self.directory("b")
        pa, pb = self.photo(a), self.photo(b)
        group = self.queue.add_source_group([{"path": str(a), "remote_subdir": "a"}, {"path": str(b), "remote_subdir": "b"}], "both")
        self.queue.scan()
        job = self.queue.claim()
        self.queue.mark_uploaded(job, remote_hash="A" * 40, verified_metadata={"is_unlim_upload": True})
        uploaded_path = job.remote_path
        self.queue.update_source_group(group["id"], "/New")
        rows = self.queue.list_jobs()
        self.assertEqual(next(row["remote_path"] for row in rows if row["status"] == "uploaded"), uploaded_path)
        self.assertEqual(next(row["remote_path"] for row in rows if row["status"] == "pending"), "/New/b/photo.jpg")
        self.queue.update_source_group(group["id"], "/New", members=[{"path": str(b), "remote_subdir": "b"}])
        self.assertEqual(self.queue.jobs_summary()["total"], 1)
        self.assertEqual(self.queue.upload_history(job.id)[0]["origin_job_id"], job.id)
        self.assertIsNone(self.queue.upload_history(job.id)[0]["job_id"])
        self.assertTrue(pa.exists() and pb.exists())
        highwater = max(root.id for root in self.queue.roots())
        self.assertEqual(self.queue.remove_source_group(group["id"]), 1)
        new_group = self.group("later")
        self.assertGreater(new_group["id"], highwater)
        self.assertTrue(pa.exists() and pb.exists())

    def test_group_destination_collision_rolls_back_every_member(self):
        group = self.group("first")
        other = self.group("second")
        self.queue.scan()
        before = self.queue.list_jobs()
        with self.assertRaises(ScanCollisionError):
            self.queue.update_source_group(group["id"], other["remote_prefix"])
        self.assertEqual(self.queue.list_jobs(), before)
        self.assertEqual(self.queue.source_group(group["id"])["remote_prefix"], group["remote_prefix"])

    def test_group_can_atomically_swap_two_member_destinations(self):
        a, b = self.directory("a"), self.directory("b")
        self.photo(a)
        self.photo(b)
        group = self.queue.add_source_group([{"path": str(a), "remote_subdir": "a"},
                                             {"path": str(b), "remote_subdir": "b"}], "both")
        self.queue.scan()
        self.queue.update_source_group(group["id"], group["remote_prefix"], members=[
            {"path": str(a), "remote_subdir": "b"}, {"path": str(b), "remote_subdir": "a"},
        ])
        rows = {row["local_path"]: row for row in self.queue.list_jobs()}
        self.assertEqual(rows[str(a / "photo.jpg")]["remote_path"], "/Archive/both/b/photo.jpg")
        self.assertEqual(rows[str(b / "photo.jpg")]["remote_path"], "/Archive/both/a/photo.jpg")
        self.assertFalse(self.queue._db.execute("SELECT 1 FROM jobs WHERE remote_key LIKE 'remap:%'").fetchone())

    def test_scoped_list_export_retry_and_clear_never_touch_another_group(self):
        first, second = self.group("first"), self.group("second")
        self.queue.scan()
        roots = self.queue.group_root_ids([second["id"]])
        job = self.queue.claim(root_ids=roots)
        self.queue.mark_failed(job, "retry me", retry_delay=None)
        self.assertEqual(self.queue.jobs_summary(root_ids=roots)["total"], 1)
        self.assertEqual([row["id"] for row in self.queue.iter_export(root_ids=roots)], [job.id])
        self.assertEqual([row["id"] for row in self.queue.iter_tasks(root_ids=roots)], [job.id])
        self.assertEqual(self.queue.retry_failed(include_permanent=True, root_ids=self.queue.group_root_ids([first["id"]])), 0)
        self.assertEqual(self.queue.retry_failed(include_permanent=True, root_ids=roots), 1)
        self.assertEqual(self.queue.clear_tasks(root_ids=roots), 1)
        self.assertEqual(self.queue.list_jobs()[0]["source_group_id"], first["id"])

    def test_group_scope_is_invalidated_across_connections_after_members_change(self):
        first, second = self.directory("first"), self.directory("second")
        group = self.queue.add_source_group([{"path": str(first)}], "group")
        old_scope = self.queue.group_root_ids([group["id"]])
        with Queue(self.db, destination="/Archive") as other:
            other.update_source_group(group["id"], "/Changed", members=[{"path": str(second)}])
        with self.assertRaises(QueueConfigurationError):
            self.queue.stats(root_ids=old_scope)
        new_root = self.queue.roots()[0]
        self.assertNotEqual(new_root.id, group["id"])
        self.assertEqual(self.queue.root_group_id(new_root.id), group["id"])

    def test_scoped_sorted_pages_use_group_indexes_for_every_tab_and_field(self):
        group = self.group("one")
        self.queue.scan()
        roots = self.queue.group_root_ids([group["id"]])
        for status in (None, "pending", "existing", "uploaded_regular", "uploaded_unlimited", "paused"):
            for field in ("name", "progress", "status", "size", "created_at", "completed_at"):
                with self.subTest(status=status, field=field):
                    trace = []
                    self.queue._db.set_trace_callback(trace.append)
                    self.queue.list_jobs(status, root_ids=roots, sort_by=field, limit=1)
                    self.queue._db.set_trace_callback(None)
                    select = next(sql for sql in trace if "SELECT * FROM jobs INDEXED BY jobs_group_" in sql)
                    plan = " ".join(row[3] for row in self.queue._db.execute("EXPLAIN QUERY PLAN " + select))
                    self.assertNotIn("TEMP B-TREE", plan)
                    self.assertIn("source_group_id=?", plan)

    def test_many_members_have_bounded_state_and_one_group_seek_per_claim(self):
        members = []
        for number in range(620):
            directory = self.directory(f"many/d{number:04}")
            self.photo(directory)
            members.append({"path": str(directory), "remote_subdir": f"d{number:04}"})
        group = self.queue.add_source_group(members, "many")
        brief = self.queue.source_groups()[0]
        self.assertEqual(brief["member_count"], 620)
        self.assertEqual(len(brief["members_preview"]), 3)
        self.assertNotIn("members", brief)
        self.assertLess(len(json.dumps(brief)), 2500)
        roots = self.queue.group_root_ids([group["id"]])
        self.assertEqual(self.queue.scan(root_ids=roots).added, 620)
        statements = []
        self.queue._db.set_trace_callback(statements.append)
        job = self.queue.claim(root_ids=roots)
        self.queue._db.set_trace_callback(None)
        self.assertIsNotNone(job)
        self.assertLess(len(statements), 70)
        self.assertTrue(any("INDEXED BY jobs_pending_group" in sql for sql in statements))
        self.assertFalse(any("INDEXED BY jobs_pending_root" in sql for sql in statements))
        self.queue.release(job)
        statements = []
        self.queue._db.set_trace_callback(statements.append)
        candidates = self.queue.reconcile_candidates(root_ids=roots, limit=2)
        self.queue._db.set_trace_callback(None)
        self.assertEqual(len(candidates), 2)
        self.assertEqual(sum("FROM jobs INDEXED BY jobs_reconcile_" in sql for sql in statements), 1)

    def test_member_previews_and_details_do_not_return_an_unbounded_exclusion_list(self):
        root = self.directory("excluded")
        group = self.queue.add_source_group([{"path": str(root), "excluded_paths": [f"ignore-{i:05}" for i in range(6000)]}], "excluded")
        for member in (group["members_preview"][0], self.queue.source_group_members(group["id"])["members"][0],
                       self.queue.source_group(group["id"], include_members=True)["members"][0]):
            self.assertEqual(member["excluded_count"], 6000)
            self.assertEqual(len(member["excluded_preview"]), 3)
            self.assertNotIn("excluded_paths", member)
            self.assertLess(len(json.dumps(member)), 1000)

    def test_source_group_pages_and_scoped_aggregates_exclude_unrelated_parties(self):
        first = self.group("manual")
        second = self.group("party", owner="plan-batch:second")
        third = self.group("manual-later")
        self.queue.scan()
        page = self.queue.source_groups_page(limit=1, offset=1)
        self.assertEqual((page["total"], page["offset"], page["limit"]), (3, 1, 1))
        self.assertEqual([item["id"] for item in page["sources"]], [second["id"]])
        manual = self.queue.source_groups_page(kind="manual")
        self.assertEqual([item["id"] for item in manual["sources"]], [first["id"], third["id"]])
        self.assertEqual(manual["total"], 2)
        self.assertEqual(self.queue.source_group_counts(), {"total": 3, "manual": 2})
        statements = []
        self.queue._db.set_trace_callback(statements.append)
        selected = self.queue.source_group_stats([second["id"]])
        self.queue._db.set_trace_callback(None)
        self.assertEqual([item["id"] for item in selected], [second["id"]])
        self.assertEqual(selected[0]["total_count"], 1)
        select = next(sql for sql in statements if "FROM jobs INDEXED BY jobs_group" in sql)
        plan = " ".join(row[3] for row in self.queue._db.execute("EXPLAIN QUERY PLAN " + select))
        self.assertIn("SEARCH jobs USING INDEX jobs_group (source_group_id=?)", plan)
        self.assertEqual(self.queue.source_group_stats([]), [])
        with self.assertRaises(QueueConfigurationError):
            self.queue.source_group_stats([999999])
        for kwargs in ({"limit": 201}, {"offset": -1}, {"kind": "invalid"}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.queue.source_groups_page(**kwargs)

    def test_source_search_normalizes_aliases_and_treats_paths_and_metacharacters_literally(self):
        path = self.directory("MixedCase/папка_100%")
        group = self.queue.add_source_group([{"path": str(path)}], "СЕМЕЙНЫЙ Архив")
        self.group("unrelated", owner="plan-batch:other")
        self.assertEqual(self.queue.source_groups_page(q="семейный")["sources"][0]["id"], group["id"])
        self.assertEqual(self.queue.source_groups_page(q="папка_100%")["total"], 1)
        self.assertEqual(self.queue.source_groups_page(q="100%")["total"], 1)
        self.assertEqual(self.queue.source_groups_page(q="%_")["total"], 0)
        self.assertEqual(self.queue.source_groups_page(q="' OR 1=1 --")["total"], 0)
        self.assertEqual(self.queue.source_groups_page(kind="manual", q="unrelated")["total"], 0)
        if os.name != 'nt':
            self.assertEqual(self.queue.source_groups_page(q="MixedCase")["total"], 1)
            self.assertEqual(self.queue.source_groups_page(q="mixedcase")["total"], 0)
        with self.assertRaises(ValueError):
            self.queue.source_groups_page(q="x" * 257)

    def test_existing_cloud_objects_never_enter_upload_tabs_or_uploaded_totals(self):
        group = self.group("first")
        self.queue.scan()
        candidate = self.queue.reconcile_candidates()[0]
        self.assertTrue(self.queue.mark_remote_existing(candidate, remote_hash="A" * 40, storage_mode="unlimited"))
        stats = self.queue.stats()
        self.assertEqual(stats["existing"], 1)
        self.assertEqual(stats["uploaded_total_count"], 0)
        self.assertEqual(self.queue.jobs_summary("uploaded_unlimited")["total"], 0)
        self.assertEqual(self.queue.source_group_stats()[0]["cloud_count"], 1)
        self.assertEqual(self.queue.source_group_stats()[0]["existing_count"], 1)

    def test_creation_receipts_survive_fresh_scan_and_source_removal(self):
        group, job = self.batch()
        digest = self.accepted(job, finished=True)
        receipts = self.queue.batch_cloud_objects("party-1")
        self.assertEqual(len(receipts), 1)
        self.assertEqual(receipts[0]["remote_path"], job.remote_path)
        self.assertEqual(self.queue.upload_history(job.id)[0]["batch_id"], "party-1")
        self.queue.reset_for_scan(self.queue.group_root_ids([group["id"]]))
        self.queue.scan()
        candidate = self.queue.reconcile_candidates()[0]
        self.queue.mark_remote_existing(candidate, remote_hash=digest)
        self.assertEqual(len(self.queue.batch_cloud_objects("party-1")), 1)
        self.queue.remove_source_group(group["id"])
        receipt = self.queue.batch_cloud_objects("party-1")[0]
        self.assertIsNone(receipt["job_id"])
        self.assertFalse(self.queue.mark_batch_cloud_deleted("party-1", receipt["id"], expected_hash="wrong", expected_path=job.remote_path))
        self.assertTrue(self.queue.mark_batch_cloud_deleted("party-1", receipt["id"], expected_hash=digest, expected_path=job.remote_path))
        self.assertEqual(self.queue.batch_cloud_objects("party-1"), [])

    def test_false_flag_kept_and_verification_failure_still_have_creation_proof(self):
        group, job = self.batch()
        self.accepted(job, mode="regular", confirmed=False)
        receipt = self.queue.batch_cloud_objects("party-1")[0]
        self.assertEqual(receipt["mode"], "regular")
        self.assertEqual(receipt["verified"], 0)
        self.assertEqual(self.queue.get_job(job.id)["status"], "failed")
        self.assertEqual(self.queue.upload_history(job.id), [])

    def test_existing_discovery_and_unaccepted_attempt_never_acquire_batch_ownership(self):
        group, job = self.batch()
        self.queue.save_commit_intent(job, "A" * 40)
        self.queue.record_cloud_observation(job, mode="unknown", remote_hash="A" * 40,
                                            metadata={"commit_accepted": False, "path": job.remote_path})
        self.assertEqual(self.queue.batch_cloud_objects("party-1"), [])
        self.queue.release(job)
        self.queue.reset_for_scan()
        self.queue.scan()
        # No intent is automatically restored by a mere scanner lookup.
        candidate = self.queue.reconcile_candidates()[0]
        self.queue.mark_remote_existing(candidate, remote_hash="A" * 40)
        self.assertEqual(self.queue.batch_cloud_objects("party-1"), [])

    def test_recovery_retains_original_batch_without_active_runtime_metadata(self):
        group, job = self.batch()
        digest = "A" * 40
        self.queue.save_commit_intent(job, digest)
        self.queue.confirm_unlimited_commit(job, digest)
        self.queue.mark_failed(job, "process interrupted", retry_delay=None)
        self.queue.reset_for_scan()
        self.queue.scan()
        self.queue.set_active_batch(None)
        restored = self.queue.claim()
        self.assertEqual(self.queue.get_commit_intent(restored)["batch_id"], "party-1")
        receipt = {"path": restored.remote_path, "size": restored.size, "commit_accepted": False,
                   "recovered_commit": True, "is_unlim_upload": True, "verified": True}
        self.queue.record_cloud_observation(restored, mode="unlimited", remote_hash=digest, metadata=receipt)
        self.queue.mark_uploaded(restored, remote_hash=digest, verified_metadata=receipt)
        self.assertEqual(len(self.queue.batch_cloud_objects("party-1")), 1)

    def test_wrong_runtime_group_does_not_claim_a_manual_upload(self):
        group, job = self.batch()
        context = self.queue.get_metadata("active_batch")
        self.queue.set_metadata("active_batch", {**context, "id": "another"})
        self.accepted(job, finished=True)
        self.assertEqual(self.queue.batch_cloud_objects("party-1"), [])
        self.assertEqual(self.queue.batch_cloud_objects("another"), [])

    def _assert_stale_runtime_context_does_not_own_new_creations(self, field, value):
        group, job = self.batch()
        old_context = self.queue.get_metadata("active_batch")
        self.queue.set_metadata("active_batch", {**old_context, field: value})
        self.accepted(job, finished=True)
        self.assertEqual(self.queue.batch_cloud_objects("party-1"), [])
        self.assertIsNone(self.queue.upload_history(job.id)[0]["batch_id"])

    def test_stale_process_id_cannot_attribute_cli_uploads_to_a_party(self):
        self._assert_stale_runtime_context_does_not_own_new_creations("pid", os.getpid() + 10000)

    def test_stale_process_nonce_cannot_attribute_new_uploads_after_pid_reuse(self):
        self._assert_stale_runtime_context_does_not_own_new_creations("process_nonce", "previous-process")

    def test_confirmed_intent_keeps_provenance_when_runtime_context_is_stale(self):
        group, job = self.batch()
        digest = "A" * 40
        self.queue.save_commit_intent(job, digest)
        self.queue.confirm_unlimited_commit(job, digest)
        self.queue.mark_failed(job, "interrupted after server acknowledgement", retry_delay=None)
        old_context = self.queue.get_metadata("active_batch")
        self.queue.set_metadata("active_batch", {**old_context, "process_nonce": "previous-process"})
        self.queue.request_control(job.id, "start")
        recovered = self.queue.claim(job_id=job.id)
        receipt = {"commit_accepted": False, "recovered_commit": True, "path": job.remote_path,
                   "size": job.size, "is_unlim_upload": True}
        self.queue.record_cloud_observation(recovered, mode="unlimited", remote_hash=digest, metadata=receipt)
        self.queue.mark_uploaded(recovered, remote_hash=digest, verified_metadata=receipt)
        self.assertEqual(len(self.queue.batch_cloud_objects("party-1")), 1)
        self.assertEqual(self.queue.upload_history(job.id)[0]["batch_id"], "party-1")

    def test_runtime_context_helper_checks_group_ownership_before_mutation(self):
        group = self.group("manual")
        with self.assertRaises(QueueConfigurationError):
            self.queue.set_active_batch("party-1", group["id"])
        self.assertIsNone(self.queue.get_metadata("active_batch"))
        with self.assertRaises(ValueError):
            self.queue.set_active_batch(None, group["id"])

    def test_normal_task_cloud_deletion_clears_party_cleanup_receipts(self):
        group, job = self.batch()
        digest = self.accepted(job)
        self.assertTrue(self.queue.mark_cloud_deleted(job.id, expected_hash=digest, expected_path=job.remote_path))
        self.assertEqual(self.queue.batch_cloud_objects("party-1"), [])

    def test_frozen_batch_receipt_with_no_node_id_is_superseded_by_a_foreign_same_content_commit(self):
        group, job = self.batch()
        self.queue.save_commit_intent(job, "A" * 40)
        self.queue.record_cloud_observation(job, mode="unlimited", remote_hash="A" * 40,
                                            metadata={"commit_accepted": True, "path": job.remote_path, "size": job.size})
        self.queue.mark_failed(job, "incomplete stat", retry_delay=None)
        receipt = self.queue.batch_cloud_objects("party-1")[0]
        self.assertIsNone(receipt["remote_id"])
        self.assertEqual(self.queue.batch_cloud_object_state("party-1", receipt["id"])["state"], "current")
        self.queue.set_active_batch(None)
        self.queue.request_control(job.id, "start")
        replacement = self.queue.claim(job_id=job.id)
        self.queue.save_commit_intent(replacement, "A" * 40)
        self.queue.record_cloud_observation(replacement, mode="unlimited", remote_hash="A" * 40,
            metadata={"commit_accepted": True, "path": job.remote_path, "size": job.size})
        self.queue.mark_failed(replacement, "another creation", retry_delay=None)
        observed = self.queue.batch_cloud_object_state("party-1", receipt["id"])
        self.assertEqual(observed["state"], "superseded")
        self.assertEqual(observed["object"]["id"], receipt["id"])
        self.assertFalse(self.queue.mark_batch_cloud_deleted("party-1", receipt["id"],
                                                           expected_hash="A" * 40, expected_path=job.remote_path))

    def test_batch_receipt_state_requires_its_exact_tombstone_and_survives_detach(self):
        group, job = self.batch()
        digest = self.accepted(job)
        receipt = self.queue.batch_cloud_objects("party-1")[0]
        self.queue.remove_source_group(group["id"])
        self.assertEqual(self.queue.batch_cloud_object_state("party-1", receipt["id"])["state"], "current")
        self.assertTrue(self.queue.mark_batch_cloud_deleted("party-1", receipt["id"], expected_hash=digest, expected_path=job.remote_path))
        self.assertEqual(self.queue.batch_cloud_object_state("party-1", receipt["id"])["state"], "deleted")
        self.assertEqual(self.queue.batch_cloud_object_state("another-batch", receipt["id"]), {"state": "missing", "object": None})
        self.assertEqual(self.queue.batch_cloud_object_state("party-1", receipt["id"] + 1000), {"state": "missing", "object": None})

    def test_later_unattributed_deletion_is_not_an_exact_batch_cleanup_checkpoint(self):
        group, job = self.batch()
        digest = self.accepted(job)
        receipt = self.queue.batch_cloud_objects("party-1")[0]
        self.assertTrue(self.queue.mark_cloud_deleted(job.id, expected_hash=digest, expected_path=job.remote_path))
        self.assertEqual(self.queue.batch_cloud_object_state("party-1", receipt["id"])["state"], "superseded")

    def test_schema_five_upgrade_preserves_ids_history_and_read_only_reopen(self):
        directory = self.directory("legacy")
        self.photo(directory)
        root = self.queue.add_root(directory, "legacy")
        self.queue.scan()
        job = self.queue.claim()
        self.queue.mark_uploaded(job, remote_hash="A" * 40, verified_metadata={"is_unlim_upload": True})
        before = self.queue.upload_history(job.id)
        db = self.queue._db
        db.execute("DROP TRIGGER jobs_source_group_insert")
        db.execute("DROP TRIGGER jobs_source_group_update")
        for row in db.execute("SELECT name,sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL").fetchall():
            if any(column in row["sql"] for column in ("source_group_id", "group_id", "batch_id")):
                db.execute(f"DROP INDEX {row['name']}")
        db.execute("ALTER TABLE jobs DROP COLUMN source_group_id")
        for column in ("group_id", "recursive", "exclusions_json", "remote_subdir"):
            db.execute(f"ALTER TABLE roots DROP COLUMN {column}")
        db.execute("ALTER TABLE upload_results DROP COLUMN batch_id")
        db.execute("ALTER TABLE cloud_observations DROP COLUMN batch_id")
        db.execute("DROP TABLE source_groups")
        db.execute("PRAGMA user_version=5")
        self.queue.close()
        self.queue = Queue(self.db, destination="/Archive")
        self.assertEqual(self.queue._db.execute("PRAGMA user_version").fetchone()[0], SCHEMA_VERSION)
        self.assertEqual(self.queue.source_groups()[0]["id"], root.id)
        self.assertEqual(self.queue.roots()[0], root)
        self.assertEqual(self.queue.get_job(job.id)["source_group_id"], root.id)
        self.assertEqual(self.queue.upload_history(job.id), before)
        backups = list((self.base / "backups").glob("*schema5-before6-*.sqlite3"))
        self.assertEqual(len(backups), 1)
        with sqlite3.connect(backups[0]) as saved:
            self.assertEqual(saved.execute("PRAGMA user_version").fetchone()[0], 5)
        with Queue(self.db, destination="/Archive") as reader:
            self.assertEqual(reader._db.total_changes, 0)
            self.assertEqual(reader.source_group_stats()[0]["uploaded_count"], 1)


if __name__ == "__main__":
    unittest.main()
