"""Offline behavior checks for the upload transaction, never real account traffic."""

import io
import json
import os
import tempfile
import unittest
import uuid
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from mailru_photos.api import (
    API_BASE, UPLOAD_BASE, CloudAPI, FileIdentity, ProtocolError,
    RemoteConflict, RemoteError, SourceChanged, UnlimitedNotConfirmed, cloud_hash, _retry_after,
)
from mailru_photos.auth import Tokens
from mailru_photos.transport import HTTP, Response, TransportError, validate_url


def response(status=200, data=None, headers=None):
    raw = data if isinstance(data, bytes) else json.dumps(data if data is not None else {}).encode()
    return Response(status, headers or {}, raw)


class TokensStub:
    def __init__(self):
        self.tokens = Tokens("OLD-TOKEN", "REFRESH-TOKEN", "owner@example.ru")
        self.refresh_calls = 0

    def access_token(self, *, force_refresh=False, rejected_token=None):
        if force_refresh:
            self.refresh_calls += 1
            self.tokens = Tokens("NEW-TOKEN", "NEW-REFRESH", self.tokens.username)
        return self.tokens.access_token


class FakeHTTP:
    def __init__(self, sequence):
        self.sequence = list(sequence)
        self.requests = []

    def request(self, method, url, **kwargs):
        validate_url(url)
        record = {"method": method, "url": url, "headers": kwargs.get("headers", {}),
                  "body": kwargs.get("body", b""), "stream_body": None}
        stream = kwargs.get("stream")
        if stream is not None:
            record["stream_body"] = stream.read(kwargs["length"])
            callback = kwargs.get("progress")
            if callback:
                callback(len(record["stream_body"]))
        self.requests.append(record)
        if not self.sequence:
            raise AssertionError("Unexpected network operation")
        result = self.sequence.pop(0)
        if callable(result):
            result = result(record)
        if isinstance(result, Exception):
            raise result
        return result

    def close(self):
        pass


class UploadBehaviorTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.local = Path(self.folder.name) / "снимок #1.jpg"
        self.data = b"\xff\xd8\xff" + bytes(range(256)) * 16 + b"\xff\xd9"
        self.local.write_bytes(self.data)
        self.identity = FileIdentity.from_stat(self.local.stat())
        self.digest, _ = cloud_hash(self.local)
        self.remote = "/Import/снимок #1.jpg"
        self.saved = []
        self.confirmed = []

    def tearDown(self):
        self.folder.cleanup()

    def api(self, sequence, *, parent_known=True):
        fake = FakeHTTP(sequence)
        tokens = TokensStub()
        api = CloudAPI(tokens, transport_factory=lambda: fake)
        if parent_known:
            api._directories.add("/Import")
        return api, fake, tokens

    def stat_response(self, digest=None, size=None):
        return response(data={"hash": self.digest if digest is None else digest,
                              "size": self.identity.size if size is None else size})

    def upload(self, api, *, prior_intent=None, is_retried=False):
        return api.upload_photo(str(self.local), self.remote, expected=self.identity,
                                prior_intent=prior_intent, save_intent=self.saved.append,
                                save_confirmation=self.confirmed.append,
                                is_retried=is_retried)

    def test_quota_uses_cloud_component_from_canonical_host(self):
        api, fake, _ = self.api([response(data={"quota": 1000,
                                               "used": {"cloud": 123, "mail": 400, "total": 523}})])
        self.assertEqual(api.quota(), {"used_bytes": 123, "total_bytes": 1000,
                                      "mail_bytes": 400, "combined_used_bytes": 523})
        request = fake.requests[0]
        self.assertEqual(request["method"], "GET")
        self.assertEqual(request["url"], "https://cloud.mail.ru/api/v4/user/quota")
        self.assertEqual(request["headers"]["Authorization"], "Bearer OLD-TOKEN")

    def test_quota_requires_explicit_cloud_component(self):
        api, fake, _ = self.api([response(data={"quota": 1000, "used": {"mail": 400, "total": 523}})])
        with self.assertRaises(ProtocolError):
            api.quota()
        self.assertEqual(len(fake.requests), 1)

    def test_new_upload_creates_parent_then_commits_verified_original(self):
        def accepted_commit(request):
            self.assertEqual(self.saved, [self.digest], "Durable intent must precede finalization")
            payload = json.loads(request["body"])
            self.assertTrue(payload["unlimited_upload"])
            self.assertEqual(payload["hash"], self.digest)
            self.assertEqual(payload["size"], len(self.data))
            self.assertEqual(payload["path"], self.remote)
            self.assertEqual(payload["platform"], "cloud-android")
            self.assertEqual(payload["upload_type"], "AUTO")
            self.assertFalse(payload["overwrite"])
            self.assertFalse(payload["options"]["overwrite"])
            return response(data={"name": self.local.name, "path": self.remote, "nodeid": "123", "is_unlim_upload": True})

        api, fake, _ = self.api([
            response(404),
            response(data=b"https://cloclo1.datacloudmail.ru 192.0.2.1 500\n"),
            response(data=b"\x00\x01"),
            response(404, headers={"x-upload-limit": str(10_000_000)}),
            response(201, headers={"x-valid-for": "3600"}),
            accepted_commit,
            self.stat_response(),
        ], parent_known=False)
        result = self.upload(api)
        self.assertTrue(result["commit_accepted"])
        self.assertTrue(result["stat_matched"])
        self.assertFalse(fake.sequence)
        self.assertEqual([item["method"] for item in fake.requests], ["GET", "GET", "POST", "PUT", "PUT", "POST", "GET"])
        body_request = next(item for item in fake.requests if item["stream_body"] is not None)
        self.assertEqual(body_request["stream_body"], self.data)
        self.assertNotIn("token=", body_request["url"])
        self.assertEqual(body_request["headers"]["Authorization"], "Bearer OLD-TOKEN")
        upload_requests = [r for r in fake.requests if r["method"] == "PUT"]
        queries = [parse_qs(urlsplit(r["url"]).query, keep_blank_values=True) for r in upload_requests]
        self.assertEqual(queries[0], queries[1])
        self.assertEqual(queries[0]["platform"], ["cloud-android"])
        self.assertEqual(queries[0]["gaid"], [""])
        self.assertEqual(queries[0]["is_background"], ["true"])
        self.assertEqual(queries[0]["upload_type"], ["AUTO"])
        self.assertEqual(queries[0]["network"], ["wi-fi"])
        self.assertEqual(queries[0]["is_retried"], ["false"])
        self.assertEqual(str(uuid.UUID(queries[0]["session_id"][0])), queries[0]["session_id"][0])

    def test_resume_transmits_only_unreceived_suffix(self):
        offset = 37
        target = "https://cloclo8.datacloudmail.ru/resumable/" + self.digest + "?upload_id=abc%2B123"
        api, fake, _ = self.api([
            response(404),
            response(200, headers={"x-received": str(offset), "x-location": target,
                                  "x-valid-for": "3600", "x-upload-limit": "10000000"}),
            response(201, headers={"x-valid-for": "3600"}),
            response(data={"name": self.local.name, "path": self.remote, "is_unlim_upload": True}),
            self.stat_response(),
        ])
        self.upload(api, is_retried=True)
        upload = next(item for item in fake.requests if item["stream_body"] is not None)
        self.assertTrue(upload["url"].startswith(target + "&"))
        query = parse_qs(urlsplit(upload["url"]).query, keep_blank_values=True)
        self.assertEqual(query["upload_id"], ["abc+123"])
        self.assertEqual(query["is_retried"], ["true"])
        self.assertEqual(upload["stream_body"], self.data[offset:])
        self.assertEqual(upload["headers"]["Content-Range"], f"bytes {offset}-{len(self.data)-1}/{len(self.data)}")

    def test_lost_commit_response_cannot_infer_unlimited_from_matching_hash(self):
        api, fake, _ = self.api([
            response(404), response(201, headers={"x-valid-for": "3600"}),
            TransportError("Simulated lost finalization response"),
        ])
        with self.assertRaises(TransportError):
            self.upload(api)
        self.assertEqual(self.saved, [self.digest])
        intent = {"cloud_hash": self.digest, "remote_path": self.remote}
        api2, second, _ = self.api([self.stat_response()])
        with self.assertRaises(UnlimitedNotConfirmed):
            self.upload(api2, prior_intent=intent)
        self.assertEqual(self.confirmed, [])
        self.assertEqual(len(second.requests), 1)
        self.assertEqual(second.requests[0]["method"], "GET")

    def test_saved_positive_confirmation_recovers_after_stat_disconnect(self):
        api, _, _ = self.api([
            response(404), response(201, headers={"x-valid-for": "0"}),
            response(data={"name": self.local.name, "path": self.remote, "is_unlim_upload": True}),
            TransportError("Simulated stat disconnect"),
            TransportError("Simulated stat disconnect"),
            TransportError("Simulated stat disconnect"),
        ])
        with self.assertRaises(TransportError):
            self.upload(api)
        self.assertEqual(self.confirmed, [self.digest])
        intent = {"cloud_hash": self.digest, "remote_path": self.remote, "unlimited_confirmed": True}
        api2, second, _ = self.api([self.stat_response()])
        result = self.upload(api2, prior_intent=intent)
        self.assertTrue(result["recovered_commit"])
        self.assertFalse(result["commit_accepted"])
        self.assertIs(result["is_unlim_upload"], True)
        self.assertEqual(result["unlimited_evidence"], "saved_commit_response")
        self.assertEqual(len(second.requests), 1)
        self.assertEqual(second.requests[0]["method"], "GET")

    def test_confirmation_is_persisted_before_verification_stat(self):
        def checked_stat(_):
            self.assertEqual(self.confirmed, [self.digest])
            return self.stat_response()
        api, fake, _ = self.api([
            response(404), response(201, headers={"x-valid-for": "0"}),
            response(data={"name": self.local.name, "path": self.remote, "is_unlim_upload": True}), checked_stat,
        ])
        result = self.upload(api)
        self.assertIs(result["is_unlim_upload"], True)
        self.assertEqual(result["quota_status"], "server_confirmed_unlimited")
        self.assertFalse(fake.sequence)

    def test_missing_or_nontrue_unlimited_flag_never_finishes_or_falls_back(self):
        missing = object()
        for flag in (missing, False, 0, 1, "true", None):
            with self.subTest(flag="missing" if flag is missing else flag):
                self.saved.clear()
                self.confirmed.clear()
                added = {"name": self.local.name, "path": self.remote}
                if flag is not missing:
                    added["is_unlim_upload"] = flag
                api, fake, _ = self.api([
                    response(404), response(201, headers={"x-valid-for": "0"}), response(data=added),
                ])
                with self.assertRaises(UnlimitedNotConfirmed):
                    self.upload(api)
                self.assertEqual(self.confirmed, [])
                self.assertEqual([r["method"] for r in fake.requests], ["GET", "PUT", "POST"])
                self.assertTrue(json.loads(fake.requests[-1]["body"])["unlimited_upload"])

    def test_matching_existing_file_without_our_intent_is_not_overwritten(self):
        api, fake, _ = self.api([self.stat_response()])
        with self.assertRaises(RemoteConflict):
            self.upload(api)
        self.assertEqual(len(fake.requests), 1)
        self.assertEqual(self.saved, [])

    def test_unlimited_rejection_does_not_fall_back_to_ordinary_add(self):
        api, fake, _ = self.api([
            response(404), response(201, headers={"x-valid-for": "3600"}),
            response(400, data={"error": "CONDITION/NOT_ACCEPTABLE/UNLIM_UPL_TOTAL"}),
        ])
        with self.assertRaises(RemoteError) as caught:
            self.upload(api)
        self.assertTrue(caught.exception.fatal)
        commits = [r for r in fake.requests if r["method"] == "POST"]
        self.assertEqual(len(commits), 1)
        self.assertTrue(json.loads(commits[0]["body"])["unlimited_upload"])
        self.assertEqual(urlsplit(commits[0]["url"]).path, "/api/v1/private/add")

    def test_disabled_entitlement_stops_before_writes(self):
        api, fake, _ = self.api([response(data={"email": "owner@example.ru", "unlimited_upload_enabled": False})])
        with self.assertRaises(ProtocolError):
            api.require_account("owner@example.ru")
        self.assertEqual([r["method"] for r in fake.requests], ["GET"])

    def test_missing_entitlement_or_wrong_account_fails_closed(self):
        for data in ({"email": "owner@example.ru"}, {"email": "different@example.ru", "unlimited_upload_enabled": True}):
            with self.subTest(data=data):
                api, _, _ = self.api([response(data=data)])
                with self.assertRaises(ProtocolError):
                    api.require_account("owner@example.ru")

    def test_rejected_token_refreshed_once_then_retried(self):
        api, fake, tokens = self.api([response(403), response(data={"email": "owner@example.ru", "unlimited_upload_enabled": True})])
        info = api.require_account("owner@example.ru")
        self.assertTrue(info.unlimited_enabled)
        self.assertEqual(tokens.refresh_calls, 1)
        self.assertEqual([r["headers"]["Authorization"] for r in fake.requests], ["Bearer OLD-TOKEN", "Bearer NEW-TOKEN"])

    def test_repeated_unauthorized_does_not_loop(self):
        api, fake, tokens = self.api([response(403), response(403)])
        with self.assertRaises(RemoteError):
            api.require_account()
        self.assertEqual(tokens.refresh_calls, 1)
        self.assertEqual(len(fake.requests), 2)

    def test_redirect_is_not_followed_with_credentials(self):
        api, fake, _ = self.api([response(307, headers={"location": "https://other.example/collect"})])
        with self.assertRaises(RemoteError):
            self.upload(api)
        self.assertEqual(len(fake.requests), 1)

    def test_untrusted_resume_location_gets_no_bytes(self):
        api, fake, _ = self.api([
            response(404), response(200, headers={"x-received": "10", "x-location": "https://cloud.mail.ru.evil.example/collect",
                                                "x-valid-for": "3600"}),
        ])
        with self.assertRaises(TransportError):
            self.upload(api)
        self.assertFalse(any(r["stream_body"] is not None for r in fake.requests))
        self.assertEqual(self.saved, [])

    def test_modified_source_during_streaming_is_never_committed(self):
        def mutate_after_body(request):
            with self.local.open("ab") as changed:
                changed.write(b"changed")
            return response(201, headers={"x-valid-for": "3600"})

        api, fake, _ = self.api([response(404), response(404, headers={"x-upload-limit": "10000000"}), mutate_after_body])
        with self.assertRaises(SourceChanged):
            self.upload(api)
        self.assertFalse(any(r["method"] == "POST" for r in fake.requests))
        self.assertEqual(self.saved, [])

    def test_wrong_hash_after_commit_is_not_reported_as_success(self):
        api, fake, _ = self.api([
            response(404), response(201, headers={"x-valid-for": "3600"}),
            response(data={"name": self.local.name, "is_unlim_upload": True}), self.stat_response(digest="0" * 40),
        ])
        with self.assertRaises(ProtocolError):
            self.upload(api)
        self.assertFalse(fake.sequence)

    def test_missing_blob_confirmation_stops_before_commit(self):
        api, fake, _ = self.api([response(404), response(201)])
        with self.assertRaises(ProtocolError):
            self.upload(api)
        self.assertFalse(any(r["method"] == "POST" for r in fake.requests))

    def test_malformed_json_or_size_does_not_succeed(self):
        malformed = [b"<html>unexpected</html>", {"size": len(self.data), "hash": "no hash"}]
        malformed.extend({"size": size, "hash": self.digest} for size in (
            True, -1, "-1", float("inf"), float("nan"), "inf", "nan", None, 1 << 65, "9" * 5000,
        ))
        for data in malformed:
            with self.subTest(data=data):
                api, fake, _ = self.api([response(data=data)])
                with self.assertRaises(ProtocolError):
                    self.upload(api)
                self.assertEqual(len(fake.requests), 1)

    def test_invalid_blob_lifetime_never_reaches_commit(self):
        for lifetime in (None, "", "-1", "nan", "inf", "1.5", "9223372036854775808"):
            for resume_status in (200, 201):
                with self.subTest(lifetime=lifetime, resume_status=resume_status):
                    headers = {"x-received": "10", "x-location": UPLOAD_BASE + "/resumable/" + self.digest}
                    if lifetime is not None:
                        headers["x-valid-for"] = lifetime
                    api, fake, _ = self.api([response(404), response(resume_status, headers=headers)])
                    with self.assertRaises(ProtocolError):
                        self.upload(api)
                    self.assertFalse(any(r["method"] == "POST" or r["stream_body"] is not None for r in fake.requests))

    def test_invalid_lifetime_after_sending_bytes_never_commits(self):
        for lifetime in (None, "", "-1", "nan", "inf", "1.5", "9223372036854775808"):
            with self.subTest(lifetime=lifetime):
                headers = {} if lifetime is None else {"x-valid-for": lifetime}
                api, fake, _ = self.api([
                    response(404), response(404, headers={"x-upload-limit": "10000000"}),
                    response(201, headers=headers),
                ])
                with self.assertRaises(ProtocolError):
                    self.upload(api)
                self.assertFalse(any(r["method"] == "POST" for r in fake.requests))
                self.assertEqual(self.saved, [])

    def test_zero_lifetime_available_blob_can_finalize_verified_file(self):
        api, fake, _ = self.api([
            response(404), response(201, headers={"x-valid-for": "0"}),
            response(data={"name": self.local.name, "path": self.remote, "is_unlim_upload": True}),
            self.stat_response(),
        ])
        result = self.upload(api)
        self.assertTrue(result["commit_accepted"])
        self.assertTrue(result["stat_matched"])
        self.assertEqual(self.saved, [self.digest])
        self.assertFalse(any(r["stream_body"] is not None for r in fake.requests))
        self.assertFalse(fake.sequence)

    def test_zero_lifetime_partial_blob_resumes_remaining_bytes_then_finalizes(self):
        offset = 37
        api, fake, _ = self.api([
            response(404),
            response(200, headers={"x-valid-for": "0", "x-received": str(offset),
                                  "x-location": UPLOAD_BASE + "/resumable/" + self.digest}),
            response(201, headers={"x-valid-for": "3600"}),
            response(data={"name": self.local.name, "path": self.remote, "is_unlim_upload": True}),
            self.stat_response(),
        ])
        result = self.upload(api)
        self.assertTrue(result["commit_accepted"])
        self.assertTrue(result["stat_matched"])
        upload = next(r for r in fake.requests if r["stream_body"] is not None)
        self.assertEqual(upload["stream_body"], self.data[offset:])
        self.assertEqual(upload["headers"]["Content-Range"], f"bytes {offset}-{len(self.data)-1}/{len(self.data)}")
        self.assertFalse(fake.sequence)

    def test_zero_lifetime_after_body_can_finalize_verified_file(self):
        api, fake, _ = self.api([
            response(404), response(404, headers={"x-upload-limit": "10000000"}),
            response(201, headers={"x-valid-for": "0"}),
            response(data={"name": self.local.name, "path": self.remote, "is_unlim_upload": True}),
            self.stat_response(),
        ])
        result = self.upload(api)
        self.assertTrue(result["commit_accepted"])
        self.assertTrue(result["stat_matched"])
        self.assertEqual(self.saved, [self.digest])
        self.assertEqual(next(r["stream_body"] for r in fake.requests if r["stream_body"] is not None), self.data)
        self.assertFalse(fake.sequence)


class TransportBoundaryTests(unittest.TestCase):
    def test_only_known_tls_service_locations_are_accepted(self):
        for address in (
            "https://android-api.cloud.mail.ru/api/v1/user/info",
            "https://uploader.cloud.mail.ru/resumable/ABC",
            "https://cloclo10.datacloudmail.ru/upload",
            "https://x.cldmail.ru/upload",
        ):
            validate_url(address)
        for address in (
            "http://uploader.cloud.mail.ru/upload",
            "https://cloud.mail.ru.evil.example/upload",
            "https://evilcloud.mail.ru/upload",
            "https://cloud.mail.ru:444/upload",
            "https://name:password@cloud.mail.ru/upload",
            "https://cloud.mail.ru/upload#fragment",
            "https://cloud.mail.ru/upload\r\nFake: header",
            " https://cloud.mail.ru/upload",
            "https://cloud.mail.ru/файл",
            "https://127.0.0.1/upload",
        ):
            with self.subTest(address=address):
                with self.assertRaises(TransportError):
                    validate_url(address)

    def test_request_timeout_must_be_finite(self):
        for value in (-1, 0, float("nan"), float("inf")):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    HTTP(timeout=value)

    def test_retry_after_cannot_create_nonfinite_or_overflowing_wait(self):
        for value in ("NaN", "nan", "inf", "Infinity", "-inf", "1e300", "not a date"):
            with self.subTest(value=value):
                self.assertIsNone(_retry_after(response(429, headers={"retry-after": value})))
        self.assertEqual(_retry_after(response(429, headers={"retry-after": "30"})), 30)
        self.assertEqual(_retry_after(response(429, headers={"retry-after": "0"})), 0)


if __name__ == "__main__":
    unittest.main()
