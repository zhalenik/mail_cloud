"""Offline CLI workflows and cross-process token-lock boundaries."""

from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from mailru_photos import cli
from mailru_photos.api import ProtocolError
from mailru_photos.auth import AuthError, Tokens, TokenStore
from mailru_photos.queue import MEDIA_EXTENSIONS, PHOTO_EXTENSIONS, Queue, QueueBusyError


class CLITests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.state = self.base / "state"
        self.source = self.base / "source"
        self.source.mkdir()
        self.db = self.state / "queue.sqlite3"

    def tearDown(self):
        self.tmp.cleanup()

    def invoke(self, *args):
        out = io.StringIO()
        err = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli.main(["--state", str(self.state), *map(str, args)])
        return code, out.getvalue(), err.getvalue()

    def success(self, *args):
        code, out, err = self.invoke(*args)
        self.assertEqual(code, 0, err or out)
        self.assertEqual(err, "")
        return out

    def init(self):
        self.success("init", "--destination", "/Archive")

    def photo(self, name="photo.jpg", *, root=None, contents=b"offline-fixture"):
        path = (root or self.source) / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(contents)
        return path

    def test_local_workflow_scan_report_and_recover_without_authentication(self):
        first = self.photo("2020/one.JPG")
        second = self.photo("2021/two.png", contents=b"another-photo")
        self.photo("ignored.txt")
        before = {first: first.read_bytes(), second: second.read_bytes()}
        with mock.patch.object(cli, "TokenManager", side_effect=AssertionError("local command read credentials")), \
             mock.patch.object(cli, "CloudAPI", side_effect=AssertionError("local command used network")):
            self.init()
            self.success("add-folder", self.source, "--alias", "Камера")
            self.success("scan")
            status = json.loads(self.success("status"))
            self.assertTrue(status["last_scan"]["complete"])
            self.assertEqual(status["stats"]["pending"], 2)
            self.assertEqual(status["stats"]["total_bytes"], sum(map(len, before.values())))
            self.assertIsNone(status["account"])
            with Queue(self.db, destination="/Archive") as queue:
                self.assertEqual({row["remote_path"] for row in queue.list_jobs()}, {
                    "/Archive/Камера/2020/one.JPG", "/Archive/Камера/2021/two.png",
                })
                job = queue.claim()
                self.assertIsNotNone(job)
            self.assertEqual(json.loads(self.success("recover")), {"recovered": 1})
            recovered = json.loads(self.success("status"))
            self.assertEqual(recovered["stats"]["pending"], 2)
            self.assertEqual(recovered["stats"]["running"], 0)
            # A malformed credential file must not affect local diagnostics,
            # and neither its bytes nor tokens should enter the report.
            secret = "private-token-sentinel-do-not-export"
            (self.state / "auth.dat").write_text(secret, encoding="utf-8")
            output = self.base / "reports" / "diagnostic.json"
            self.success("report", "--output", output)
            report_text = output.read_text(encoding="utf-8")
            report = json.loads(report_text)
            self.assertEqual(report["destination"], "/Archive")
            self.assertEqual(report["stats"]["total"], 2)
            self.assertTrue(report["last_scan"]["complete"])
            self.assertNotIn(secret, report_text)
            self.assertNotIn("auth.dat", report_text)
        self.assertEqual({path: path.read_bytes() for path in before}, before)

    def test_adding_source_invalidates_scan_until_next_complete_scan(self):
        self.init()
        self.photo()
        self.success("add-folder", self.source, "--alias", "camera")
        self.success("scan")
        extra = self.base / "extra"
        extra.mkdir()
        self.photo("other.heic", root=extra)
        self.success("add-folder", extra, "--alias", "old_phone")
        status = json.loads(self.success("status"))
        self.assertFalse(status["last_scan"]["complete"])
        self.assertEqual(status["last_scan"]["reason"], "source_added")
        self.success("scan")
        status = json.loads(self.success("status"))
        self.assertTrue(status["last_scan"]["complete"])
        self.assertEqual(status["stats"]["total"], 2)

    def test_incomplete_scan_cannot_reuse_previous_success(self):
        self.init()
        self.success("add-folder", self.source, "--alias", "camera")
        self.success("scan")
        self.source.rmdir()
        code, _, _ = self.invoke("scan")
        self.assertEqual(code, 2)
        status = json.loads(self.success("status"))
        self.assertFalse(status["last_scan"]["complete"])
        self.assertGreater(status["last_scan"]["error_count"], 0)

    def test_interrupted_scan_invalidates_previous_success(self):
        self.init()
        self.photo()
        self.success("add-folder", self.source, "--alias", "camera")
        self.success("scan")
        with mock.patch.object(Queue, "scan", side_effect=KeyboardInterrupt):
            code, _, err = self.invoke("scan")
        self.assertEqual(code, 130)
        self.assertIn("recover", err)
        status = json.loads(self.success("status"))
        self.assertFalse(status["last_scan"]["complete"])
        self.assertEqual(status["last_scan"]["reason"], "scan_started")

    def test_custom_extensions_and_rescan_do_not_duplicate_jobs(self):
        self.init()
        self.photo("first.JPG")
        self.photo("second.png")
        self.photo("third.heic")
        self.success("add-folder", self.source, "--alias", "camera")
        self.success("scan", "--extensions", "jpg, .PNG")
        self.success("scan", "--extensions", "jpg, .PNG")
        status = json.loads(self.success("status"))
        self.assertEqual(status["stats"]["total"], 2)
        self.assertEqual(status["last_scan"]["added"], 0)
        self.assertEqual(status["last_scan"]["unchanged"], 2)

    def test_default_scan_includes_video_and_upgrades_legacy_default(self):
        self.init()
        self.photo('still.jpg')
        self.photo('nested/clip.MOV')
        self.photo('other.MP4')
        self.photo('ignored.txt')
        self.success('add-folder', self.source, '--alias', 'camera')
        with Queue(self.db, destination='/Archive') as queue:
            queue.set_metadata('extensions', sorted(PHOTO_EXTENSIONS))
        self.success('scan')
        with Queue(self.db, destination='/Archive') as queue:
            self.assertEqual(queue.stats()['pending'], 3)
            self.assertEqual(set(queue.get_metadata('extensions')), MEDIA_EXTENSIONS)
        self.success('scan')
        self.assertEqual(json.loads(self.success('status'))['last_scan']['unchanged'], 3)

    def test_explicit_photo_only_filter_survives_interrupted_scan_and_restart(self):
        self.init()
        self.photo('still.jpg')
        self.photo('clip.MOV')
        self.success('add-folder', self.source, '--alias', 'camera')
        with mock.patch.object(Queue, 'scan', side_effect=KeyboardInterrupt):
            code, _, _ = self.invoke('scan', '--extensions', ','.join(sorted(PHOTO_EXTENSIONS)))
        self.assertEqual(code, 130)
        with Queue(self.db, destination='/Archive') as queue:
            self.assertIs(queue.get_metadata('extensions_custom'), True)
            self.assertEqual(queue.discovery_extensions(), PHOTO_EXTENSIONS)
            with queue.exclusive_run():
                self.assertFalse(queue.upgrade_media_discovery())
        self.success('scan')
        self.assertEqual(json.loads(self.success('status'))['stats']['total'], 1)

    def test_scan_reuses_saved_extensions_until_explicitly_replaced(self):
        self.init()
        self.photo("one.jpg")
        self.photo("two.png")
        self.photo("three.CR3")
        self.success("add-folder", self.source, "--alias", "camera")
        self.success("scan", "--extensions", "jpg,cr3")
        self.success("scan")
        status = json.loads(self.success("status"))
        self.assertEqual(status["last_scan"]["unchanged"], 2)
        self.assertEqual(status["stats"]["pending"], 2)
        self.assertEqual(status["stats"]["skipped"], 0)
        with Queue(self.db, destination="/Archive") as queue:
            self.assertEqual(queue.get_metadata("extensions"), [".cr3", ".jpg"])
            self.assertIs(queue.get_metadata('extensions_custom'), True)
            self.assertEqual({row["relative_path"] for row in queue.list_jobs("pending")}, {"one.jpg", "three.CR3"})
        self.success("scan", "--extensions", "png")
        self.success("scan")
        with Queue(self.db, destination="/Archive") as queue:
            self.assertEqual(queue.get_metadata("extensions"), [".png"])
            self.assertEqual([row["relative_path"] for row in queue.list_jobs("pending")], ["two.png"])

    def test_init_cannot_retarget_an_existing_queue(self):
        self.init()
        self.success("init", "--destination", "/Archive")
        code, _, err = self.invoke("init", "--destination", "/Different")
        self.assertEqual(code, 2)
        self.assertIn("--state", err)
        self.assertEqual(json.loads(self.success("status"))["destination"], "/Archive")

    def test_source_mapping_edit_and_removal_are_local_and_keep_originals(self):
        source = self.photo("nested/photo.jpg")
        with mock.patch.object(cli, "TokenManager", side_effect=AssertionError("local command read credentials")), \
             mock.patch.object(cli, "CloudAPI", side_effect=AssertionError("local command used network")):
            self.init()
            root = json.loads(self.success("add-folder", self.source, "--alias", "camera",
                                           "--destination", "/ExactFolder"))
            self.assertEqual(root["remote_prefix"], "/ExactFolder")
            self.success("scan")
            changed = json.loads(self.success("update-folder", root["id"], "--destination", "/NewFolder",
                                              "--alias", "Family"))
            self.assertEqual(changed["alias"], "Family")
            with Queue(self.db, destination="/Archive") as queue:
                self.assertEqual(queue.list_jobs()[0]["remote_path"], "/NewFolder/nested/photo.jpg")
                self.assertEqual(queue.get_metadata("last_scan")["reason"], "source_updated")
            removed = json.loads(self.success("remove-folder", root["id"]))
            self.assertEqual(removed, {"removed_tasks": 1})
            status = json.loads(self.success("status"))
            self.assertEqual(status["sources"], [])
            self.assertEqual(status["stats"]["total"], 0)
            self.assertEqual(status["last_scan"]["reason"], "source_removed")
        self.assertEqual(source.read_bytes(), b"offline-fixture")

    def test_source_edit_and_remove_refuse_an_active_owner_before_mutation(self):
        self.init()
        source = self.photo()
        root = json.loads(self.success("add-folder", self.source, "--alias", "camera"))
        self.success("scan")
        with Queue(self.db, destination="/Archive") as queue, queue.exclusive_run():
            self.assertEqual(self.invoke("update-folder", root["id"], "--destination", "/Other")[0], 2)
            self.assertEqual(self.invoke("remove-folder", root["id"])[0], 2)
            self.assertEqual(queue.roots()[0].remote_prefix, "/Archive/camera")
            self.assertEqual(queue.stats()["total"], 1)
        self.assertTrue(source.exists())

    def test_local_errors_and_retry_preserve_permanent_failure_until_requested(self):
        self.init()
        self.photo()
        self.success("add-folder", self.source, "--alias", "camera")
        self.success("scan")
        with Queue(self.db, destination="/Archive") as queue:
            queue.mark_failed(queue.claim(), "unsupported media fixture", retry_delay=None)
        errors = json.loads(self.success("errors", "--limit", "1"))
        self.assertEqual(len(errors), 1)
        self.assertIn("unsupported media", errors[0]["last_error"])
        self.assertEqual(json.loads(self.success("retry-failed")), {"queued_again": 0})
        self.assertEqual(json.loads(self.success("retry-failed", "--include-permanent")), {"queued_again": 1})
        self.assertEqual(json.loads(self.success("status"))["stats"]["pending"], 1)

    def test_source_changes_do_not_invalidate_a_live_run(self):
        self.init()
        self.photo()
        self.success("add-folder", self.source, "--alias", "Photos")
        self.success("scan")
        another = self.base / "another"
        another.mkdir()
        with Queue(self.db, destination="/Archive") as owner:
            with owner.exclusive_run():
                self.assertEqual(self.invoke("scan")[0], 2)
                self.assertEqual(self.invoke("add-folder", another, "--alias", "Another")[0], 2)
                self.assertTrue(owner.get_metadata("last_scan")["complete"])
                self.assertEqual(len(owner.roots()), 1)

    def test_doctor_refuses_busy_queue_before_loading_or_refreshing_tokens(self):
        self.init()
        with Queue(self.db, destination="/Archive") as queue, queue.exclusive_run(), \
             mock.patch.object(cli, "TokenManager") as manager, \
             mock.patch.object(cli, "CloudAPI") as api:
            code, _, err = self.invoke("doctor")
        self.assertEqual(code, 2)
        self.assertTrue(err)
        manager.assert_not_called()
        api.assert_not_called()

    def test_doctor_holds_lock_for_token_loading_requests_and_cleanup(self):
        self.init()
        account = SimpleNamespace(email="fixture@example.test", unlimited_enabled=True, upload_limit=2147483648)

        def check_lock():
            with Queue(self.db, destination="/Archive") as other:
                with self.assertRaises(QueueBusyError):
                    with other.exclusive_run():
                        pass

        def load_tokens(_):
            check_lock()
            return object()

        def account_info():
            check_lock()
            return account

        def quota():
            check_lock()
            return {"quota": 256000000000, "used": {"cloud": 42, "total": 42, "mail": 0}}

        api = mock.Mock()
        api.account_info.side_effect = account_info
        api.quota.side_effect = quota
        api.close_thread.side_effect = check_lock
        with mock.patch.object(cli, "TokenManager", side_effect=load_tokens), \
             mock.patch.object(cli, "CloudAPI", return_value=api):
            result = json.loads(self.success("doctor"))
        self.assertEqual(result["account"], account.email)
        self.assertTrue(result["unlimited_upload_enabled"])
        api.close_thread.assert_called_once()
        with Queue(self.db, destination="/Archive") as queue, queue.exclusive_run():
            self.assertEqual(queue.account_id, account.email)

    def test_login_verifies_account_before_saving_and_preserves_refreshed_tokens(self):
        self.init()
        account = SimpleNamespace(email="owner@example.test", unlimited_enabled=True, upload_limit=2147483648)
        initial = Tokens("INITIAL-ACCESS", "INITIAL-REFRESH", "Owner@Example.Test", client_id="cloud-win")
        refreshed = Tokens("UPDATED-ACCESS", "UPDATED-REFRESH", initial.username, client_id="cloud-win")
        api = mock.Mock()

        def build_api(manager):
            def account_info():
                self.assertFalse((self.state / "auth.dat").exists())
                # A rejected access token can cause a rotation during user/info.
                # Login must persist that updated in-memory pair after verification.
                manager.store.save(refreshed)
                return account
            api.account_info.side_effect = account_info
            return api

        with mock.patch.object(cli.getpass, "getpass", return_value="FAKE-APP-PASSWORD"), \
             mock.patch.object(cli, "AuthClient") as factory, \
             mock.patch.object(cli, "CloudAPI", side_effect=build_api):
            factory.return_value.login.return_value = initial
            result = json.loads(self.success("login", "--email", "Owner@Example.Test"))
        self.assertEqual(result["account"], account.email)
        self.assertEqual(result["oauth_client_id"], "cloud-win")
        self.assertFalse(result["password_saved"])
        self.assertTrue(result["unlimited_upload_enabled"])
        self.assertEqual(TokenStore(self.state / "auth.dat").load(), refreshed)
        self.assertNotIn("FAKE-APP-PASSWORD", (self.state / "auth.dat").read_text(encoding="utf-8"))
        api.account_info.assert_called_once()
        api.close_thread.assert_called_once()
        with Queue(self.db, destination="/Archive") as queue:
            self.assertEqual(queue.account_id, account.email)

    def test_login_rejects_another_account_without_saving_tokens_or_binding_queue(self):
        self.init()
        tokens = Tokens("DO-NOT-LEAK-ACCESS", "DO-NOT-LEAK-REFRESH", "owner@example.test", client_id="cloud-win")
        api = mock.Mock()
        api.account_info.return_value = SimpleNamespace(email="another@example.test", unlimited_enabled=True)
        with mock.patch.object(cli.getpass, "getpass", return_value="DO-NOT-LEAK-PASSWORD"), \
             mock.patch.object(cli, "AuthClient") as factory, \
             mock.patch.object(cli, "CloudAPI", return_value=api):
            factory.return_value.login.return_value = tokens
            code, out, err = self.invoke("login", "--email", "owner@example.test")
        self.assertEqual(code, 2)
        self.assertFalse((self.state / "auth.dat").exists())
        self.assertNotIn("DO-NOT-LEAK", out + err)
        api.close_thread.assert_called_once()
        with Queue(self.db, destination="/Archive") as queue:
            self.assertIsNone(queue.account_id)

    def test_login_user_info_failure_is_distinct_and_does_not_overwrite_saved_auth(self):
        self.init()
        existing = Tokens("EXISTING-ACCESS", "EXISTING-REFRESH", "owner@example.test", client_id="cloud-android")
        store = TokenStore(self.state / "auth.dat")
        store.save(existing)
        existing_bytes = (self.state / "auth.dat").read_bytes()
        new = Tokens("DO-NOT-LEAK-ACCESS", "DO-NOT-LEAK-REFRESH", "owner@example.test", client_id="cloud-win")
        api = mock.Mock()
        api.account_info.side_effect = ProtocolError("DO-NOT-LEAK-SERVER-DETAILS")
        with mock.patch.object(cli.getpass, "getpass", return_value="DO-NOT-LEAK-PASSWORD"), \
             mock.patch.object(cli, "AuthClient") as factory, \
             mock.patch.object(cli, "CloudAPI", return_value=api):
            factory.return_value.login.return_value = new
            code, out, err = self.invoke("login", "--email", "owner@example.test")
        self.assertEqual(code, 2)
        self.assertIn("account_info_failed", err)
        self.assertIn("user_info", err)
        self.assertNotIn("DO-NOT-LEAK", out + err)
        self.assertEqual((self.state / "auth.dat").read_bytes(), existing_bytes)
        self.assertEqual(store.load(), existing)
        api.close_thread.assert_called_once()
        with Queue(self.db, destination="/Archive") as queue:
            self.assertIsNone(queue.account_id)

    def test_login_auth_failure_prints_safe_json_and_stops_before_user_info(self):
        self.init()
        failure = AuthError("Тестовый отказ авторизации.", code="auth_rejected", status=200,
                            server_error_code=3, client_id="cloud-win", grant_type="password", stage="token")
        with mock.patch.object(cli.getpass, "getpass", return_value="DO-NOT-LEAK-PASSWORD"), \
             mock.patch.object(cli, "AuthClient") as factory, \
             mock.patch.object(cli, "CloudAPI") as api:
            factory.return_value.login.side_effect = failure
            code, out, err = self.invoke("login", "--email", "owner@example.test")
        self.assertEqual(code, 2)
        self.assertNotIn("DO-NOT-LEAK", out + err)
        self.assertIn("{", err, "AuthError must include structured safe diagnostic JSON")
        diagnostic, _ = json.JSONDecoder().raw_decode(err[err.index("{"):])
        self.assertEqual(diagnostic["code"], "auth_rejected")
        self.assertEqual(diagnostic["server_error_code"], 3)
        self.assertEqual(diagnostic["client_id"], "cloud-win")
        self.assertEqual(diagnostic["stage"], "token")
        api.assert_not_called()
        self.assertFalse((self.state / "auth.dat").exists())
        with Queue(self.db, destination="/Archive") as queue:
            saved = queue.get_metadata("last_auth")
        self.assertEqual(saved["result"], "failed")
        self.assertEqual(saved["server_error_code"], 3)
        self.assertNotIn("DO-NOT-LEAK", json.dumps(saved))

    def test_new_login_preserves_queue_upload_history_and_legacy_metadata(self):
        self.init()
        self.photo("already_uploaded.jpg")
        self.photo("waiting.jpg")
        self.success("add-folder", self.source, "--alias", "camera")
        self.success("scan")
        with Queue(self.db, destination="/Archive") as queue:
            queue.bind_account("owner@example.test")
            job = queue.claim()
            queue.mark_uploaded(job, remote_hash="A" * 40, remote_id="offline-node",
                                verified_metadata={"fixture": "previously_verified"})
            queue.set_metadata("probe", {"result": "provisional_pass", "sample_count": 1, "fixture": "preserve-evidence"})
            original_jobs = queue.list_jobs()
            original_roots = queue.roots()
            original_history = queue.upload_history(job.id)
            original_scan = queue.get_metadata("last_scan")
        tokens = Tokens("ACCESS", "REFRESH", "owner@example.test", client_id="cloud-win")
        api = mock.Mock()
        api.account_info.return_value = SimpleNamespace(email="owner@example.test", unlimited_enabled=True)
        with mock.patch.object(cli.getpass, "getpass", return_value="FAKE-PASSWORD"), \
             mock.patch.object(cli, "AuthClient") as factory, \
             mock.patch.object(cli, "CloudAPI", return_value=api):
            factory.return_value.login.return_value = tokens
            self.success("login", "--email", "owner@example.test")
        with Queue(self.db, destination="/Archive") as queue:
            self.assertEqual(queue.get_metadata("probe")["result"], "provisional_pass")
            self.assertEqual(queue.get_metadata("probe")["fixture"], "preserve-evidence")
            self.assertEqual(queue.list_jobs(), original_jobs)
            self.assertEqual(queue.roots(), original_roots)
            self.assertEqual(queue.upload_history(job.id), original_history)
            self.assertEqual(queue.get_metadata("last_scan"), original_scan)
            self.assertEqual(queue.get_metadata("last_auth")["result"], "success")
            self.assertEqual(queue.stats()["uploaded"], 1)
            self.assertEqual(queue.stats()["pending"], 1)

    def test_run_options_have_conservative_defaults_without_a_probe_command(self):
        args = cli.parser().parse_args(["run"])
        self.assertEqual(args.regular_policy, "skip")
        self.assertEqual(args.unexpected_unlimited_action, "trash")
        self.assertIsNone(args.job_id)
        self.assertNotIn("probe", cli.parser().format_help())

    def test_run_refuses_busy_queue_before_loading_tokens(self):
        self.init()
        with Queue(self.db, destination="/Archive") as owner, owner.exclusive_run(), \
             mock.patch.object(cli, "TokenManager") as manager:
            code, _, _ = self.invoke("run")
        self.assertEqual(code, 2)
        manager.assert_not_called()

    def test_single_task_resumes_only_selected_task_and_forwards_storage_policy(self):
        self.init()
        self.photo("one.jpg")
        self.photo("two.jpg")
        self.success("add-folder", self.source, "--alias", "camera")
        self.success("scan")
        with Queue(self.db, destination="/Archive") as queue:
            tasks = queue.list_jobs()
            selected, other = tasks[0]["id"], tasks[1]["id"]
            queue.request_control(selected, "pause")
            queue.request_control(other, "pause")
        def upload(queue, api, **kwargs):
            self.assertEqual(kwargs["job_id"], selected)
            self.assertEqual(kwargs["regular_policy"], "upload")
            self.assertEqual(kwargs["unexpected_unlimited_action"], "keep")
            self.assertFalse(queue.get_job(selected)["paused"])
            self.assertTrue(queue.get_job(other)["paused"])
            job = queue.claim(job_id=selected)
            queue.mark_uploaded(job, remote_hash="A" * 40,
                                verified_metadata={"is_unlim_upload": True})
            return {"stopped": False, "stats": queue.stats(), "job_id": selected}
        with mock.patch.object(cli, "TokenManager"), mock.patch.object(cli, "CloudAPI"), \
             mock.patch.object(cli, "run", side_effect=upload):
            self.success("run", "--job-id", selected, "--regular-policy", "upload",
                         "--unexpected-unlimited-action", "keep")
        with Queue(self.db, destination="/Archive") as queue:
            self.assertEqual(queue.get_job(selected)["status"], "uploaded")
            self.assertEqual(queue.get_job(other)["status"], "pending")
            self.assertTrue(queue.get_job(other)["paused"])

    def test_run_rejects_invalid_single_task_id_before_loading_tokens(self):
        self.init()
        with mock.patch.object(cli, "TokenManager") as manager:
            code, _, err = self.invoke("run", "--job-id", "0")
        self.assertEqual(code, 2)
        self.assertIn("--job-id", err)
        manager.assert_not_called()


if __name__ == "__main__":
    unittest.main()
