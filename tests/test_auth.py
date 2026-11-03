import io
import json
import os
import stat
import tempfile
import unittest
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from mailru_photos.auth import AuthClient, AuthError, TokenManager, Tokens, TokenStore, _NoRedirect
from mailru_photos.api import ProtocolError, RemoteError, is_auth_failure
from mailru_photos.transport import TransportError


class Response(io.BytesIO):
    status = 200


class Opener:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.requests = []

    def open(self, request, timeout):
        self.requests.append(request)
        result = next(self.responses)
        if isinstance(result, Exception):
            raise result
        return Response(json.dumps(result).encode())


class AuthenticationTests(unittest.TestCase):
    def test_auth_failure_classification_uses_cloud_error_types_only(self):
        for error in (AuthError('Rejected fixture credentials'), RemoteError(401), RemoteError(403)):
            with self.subTest(error=type(error).__name__, status=getattr(error, 'status', None)):
                self.assertTrue(is_auth_failure(error))
        unrelated = [PermissionError('Local source access denied'),
                     TransportError('HTTP 403 text from a proxy is not a Cloud rejection'),
                     ProtocolError('HTTP 401 in a diagnostic string'),
                     RemoteError(400), RemoteError(503), RemoteError(403, authenticated=False),
                     urllib.error.HTTPError('https://unrelated.invalid/', 403, 'Forbidden', {}, None)]
        for error in unrelated:
            if not isinstance(error, (RemoteError, urllib.error.HTTPError)):
                error.status = 403
            with self.subTest(error=type(error).__name__):
                self.assertFalse(is_auth_failure(error))

    def test_logout_removes_saved_tokens_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'auth.dat'
            store = TokenStore(path)
            store.save(Tokens('offline-access', 'offline-refresh', 'owner@example.test'))
            store.clear()
            self.assertFalse(path.exists())
            store.clear()
            with self.assertRaises(AuthError) as caught:
                store.load()
            self.assertEqual(caught.exception.code, 'not_logged_in')

    def test_login_urlencoding_and_no_password_persistence(self):
        opener = Opener([{"access_token": "ACCESS", "refresh_token": "REFRESH", "expires_in": 3600}])
        tokens = AuthClient(opener=opener, clock=lambda: 1000).login("owner@example.ru", "p&= +%😀")
        request = opener.requests[0]
        self.assertEqual(request.full_url, "https://o2.mail.ru/token")
        form = urllib.parse.parse_qs(request.data.decode())
        self.assertEqual(form, {"grant_type": ["password"], "client_id": ["cloud-win"],
                                "username": ["owner@example.ru"], "password": ["p&= +%😀"]})
        self.assertEqual(tokens.client_id, "cloud-win")
        self.assertEqual(tokens.expires_at, 4600)
        self.assertNotIn("ACCESS", repr(tokens))
        self.assertNotIn("REFRESH", repr(tokens))
        self.assertNotIn("password", tokens.to_dict())

    def test_refresh_preserves_old_refresh_token_if_server_does_not_rotate(self):
        opener = Opener([{"access_token": "NEW", "expires_in": "600"}])
        old = Tokens("OLD", "REFRESH", "owner@example.ru", client_id="cloud-android")
        new = AuthClient(opener=opener, clock=lambda: 10).refresh(old)
        form = urllib.parse.parse_qs(opener.requests[0].data.decode())
        self.assertEqual(form, {"grant_type": ["refresh_token"], "client_id": ["cloud-android"], "refresh_token": ["REFRESH"]})
        self.assertEqual(new.refresh_token, "REFRESH")
        self.assertEqual(new.access_token, "NEW")
        self.assertEqual(new.expires_at, 610)
        self.assertEqual(new.client_id, "cloud-android")

    def test_successful_tokens_accept_zero_or_null_error_code(self):
        for value in (0, "0", None):
            with self.subTest(error_code=value):
                opener = Opener([{"access_token": "ACCESS", "refresh_token": "REFRESH", "error_code": value}])
                tokens = AuthClient(opener=opener).login("owner@example.ru", "PASSWORD")
                self.assertEqual(tokens.access_token, "ACCESS")
                self.assertEqual(tokens.refresh_token, "REFRESH")

    def test_nonzero_error_code_rejects_even_if_response_contains_tokens(self):
        opener = Opener([{"access_token": "DO-NOT-LEAK-ACCESS", "refresh_token": "DO-NOT-LEAK-REFRESH", "error_code": 3}])
        with self.assertRaises(AuthError) as caught:
            AuthClient(opener=opener).login("owner@example.ru", "PASSWORD")
        error = caught.exception
        self.assertEqual(error.code, "auth_rejected")
        self.assertEqual(error.server_error_code, 3)
        self.assertNotIn("DO-NOT-LEAK", str(error) + json.dumps(error.safe_diagnostics()))

    def test_rejected_login_exposes_only_safe_provider_diagnostics(self):
        response = {
            "error_code": "3",
            "error": "SECRET-IN-ARBITRARY-ERROR",
            "error_description": "SECRET-IN-DESCRIPTION",
            "tsa_token": "SECRET-IN-CHALLENGE",
            "captcha_url": "https://example.test/SECRET-IN-URL",
        }
        with self.assertRaises(AuthError) as caught:
            AuthClient(opener=Opener([response])).login("owner@example.ru", "SECRET-PASSWORD")
        error = caught.exception
        diagnostic = error.safe_diagnostics()
        self.assertEqual(diagnostic["http_status"], 200)
        self.assertEqual(diagnostic["server_error_code"], 3)
        self.assertEqual(diagnostic["code"], "auth_rejected")
        self.assertEqual(diagnostic["client_id"], "cloud-win")
        self.assertEqual(diagnostic["grant_type"], "password")
        self.assertEqual(diagnostic["stage"], "token")
        self.assertTrue(diagnostic.get("server_error_name"))
        self.assertNotIn("SECRET", str(error) + repr(error) + json.dumps(diagnostic))
        self.assertNotIn("error_description", diagnostic)
        self.assertNotIn("tsa_token", diagnostic)
        self.assertNotIn("captcha_url", diagnostic)

    def test_diagnostics_keep_unknown_numeric_error_without_raw_description(self):
        with self.assertRaises(AuthError) as caught:
            AuthClient(opener=Opener([{"error_code": 9876, "error": "DO-NOT-LEAK"}])).login(
                "owner@example.ru", "PASSWORD")
        diagnostic = caught.exception.safe_diagnostics()
        self.assertEqual(diagnostic["server_error_code"], 9876)
        self.assertNotIn("DO-NOT-LEAK", str(caught.exception) + json.dumps(diagnostic))

    def test_both_second_factor_codes_have_actionable_safe_diagnostics(self):
        for value in (15, "15", 26, "26"):
            with self.subTest(error_code=value):
                with self.assertRaises(AuthError) as caught:
                    AuthClient(opener=Opener([{"error_code": value, "tsa_token": "DO-NOT-LEAK"}])).login(
                        "owner@example.ru", "PASSWORD")
                error = caught.exception
                self.assertEqual(error.code, "second_factor_required")
                self.assertEqual(error.server_error_code, int(value))
                self.assertEqual(error.stage, "token")
                self.assertNotIn("DO-NOT-LEAK", str(error) + json.dumps(error.safe_diagnostics()))

    def test_second_factor_rate_limit_is_distinguished_from_http_rate_limit(self):
        with self.assertRaises(AuthError) as caught:
            AuthClient(opener=Opener([{"error_code": 16, "timeout": 45, "tsa_token": "DO-NOT-LEAK"}])).login(
                "owner@example.ru", "PASSWORD")
        self.assertEqual(caught.exception.code, "second_factor_rate_limited")
        self.assertEqual(caught.exception.server_error_code, 16)
        self.assertEqual(caught.exception.retry_after_seconds, 45)

    def test_http_rate_limit_with_non_json_body_keeps_correct_classification(self):
        error = urllib.error.HTTPError("https://o2.mail.ru/token", 429, "rate", {}, io.BytesIO(b"<html>DO-NOT-LEAK</html>"))
        with self.assertRaises(AuthError) as caught:
            AuthClient(opener=Opener([error])).login("owner@example.ru", "PASSWORD")
        diagnostic = caught.exception.safe_diagnostics()
        self.assertEqual(caught.exception.code, "auth_rate_limited")
        self.assertEqual(diagnostic["http_status"], 429)
        self.assertEqual(diagnostic["stage"], "token")
        self.assertNotIn("DO-NOT-LEAK", str(caught.exception) + json.dumps(diagnostic))

    def test_provider_server_failure_is_not_reported_as_bad_credentials(self):
        for status, body in ((500, b'{"error":"DO-NOT-LEAK"}'), (502, b"Bad gateway"), (503, b"")):
            with self.subTest(status=status):
                error = urllib.error.HTTPError("https://o2.mail.ru/token", status, "server", {}, io.BytesIO(body))
                with self.assertRaises(AuthError) as caught:
                    AuthClient(opener=Opener([error])).login("owner@example.ru", "PASSWORD")
                self.assertEqual(caught.exception.code, "auth_server_unavailable")
                self.assertEqual(caught.exception.status, status)
                self.assertEqual(caught.exception.client_id, "cloud-win")
                self.assertNotIn("DO-NOT-LEAK", str(caught.exception))

    def test_refresh_diagnostics_use_the_token_client_not_new_default(self):
        old = Tokens("OLD", "REFRESH", "owner@example.ru", client_id="cloud-android")
        opener = Opener([{"error_code": 3, "error_description": "DO-NOT-LEAK"}])
        with self.assertRaises(AuthError) as caught:
            AuthClient(opener=opener).refresh(old)
        diagnostic = caught.exception.safe_diagnostics()
        self.assertEqual(diagnostic["client_id"], "cloud-android")
        self.assertEqual(diagnostic["grant_type"], "refresh_token")
        self.assertEqual(diagnostic["stage"], "token")
        self.assertNotIn("DO-NOT-LEAK", str(caught.exception) + json.dumps(diagnostic))

    def test_network_error_diagnostics_are_contextual_and_sanitized(self):
        opener = Opener([urllib.error.URLError("DO-NOT-LEAK-network-detail")])
        with self.assertRaises(AuthError) as caught:
            AuthClient(opener=opener, client_id="cloud-android").login("owner@example.ru", "PASSWORD")
        diagnostic = caught.exception.safe_diagnostics()
        self.assertEqual(caught.exception.code, "auth_network_error")
        self.assertEqual(diagnostic["client_id"], "cloud-android")
        self.assertEqual(diagnostic["grant_type"], "password")
        self.assertEqual(diagnostic["stage"], "token")
        self.assertNotIn("DO-NOT-LEAK", str(caught.exception) + json.dumps(diagnostic))

    def test_challenge_or_rate_limit_never_automatically_retries_password_grant(self):
        for value in (15, 16, 26):
            with self.subTest(error_code=value):
                opener = Opener([{"error_code": value}])
                with self.assertRaises(AuthError):
                    AuthClient(opener=opener).login("owner@example.ru", "PASSWORD")
                self.assertEqual(len(opener.requests), 1)

    def test_rotated_refresh_token_is_saved(self):
        opener = Opener([{"access_token": "NEW", "refresh_token": "ROTATED"}])
        new = AuthClient(opener=opener).refresh(Tokens("OLD", "REFRESH", "owner@example.ru"))
        self.assertEqual(new.refresh_token, "ROTATED")
        self.assertIsNone(new.expires_at)

    def test_two_factor_error_is_sanitized(self):
        opener = Opener([{"error": "need_2fa", "error_code": 15, "tsa_token": "DO-NOT-LEAK", "error_description": "DO-NOT-LEAK"}])
        with self.assertRaises(AuthError) as caught:
            AuthClient(opener=opener).login("owner@example.ru", "PASSWORD")
        self.assertEqual(caught.exception.code, "second_factor_required")
        self.assertNotIn("DO-NOT-LEAK", str(caught.exception))
        self.assertNotIn("PASSWORD", str(caught.exception))

    def test_http_auth_error_is_not_a_success(self):
        error = urllib.error.HTTPError("https://o2.mail.ru/token", 429, "rate", {}, io.BytesIO(b'{"error":"limited"}'))
        with self.assertRaises(AuthError) as caught:
            AuthClient(opener=Opener([error])).login("owner@example.ru", "PASSWORD")
        self.assertEqual(caught.exception.code, "auth_rate_limited")

    def test_credentials_are_never_redirected(self):
        request = urllib.request.Request("https://o2.mail.ru/token", data=b"password=SECRET")
        with self.assertRaises(AuthError) as caught:
            _NoRedirect().redirect_request(request, None, 302, "redirect", {}, "https://other.example/token")
        self.assertEqual(caught.exception.code, "auth_redirect")

    def test_invalid_expiry_rejected(self):
        for duration in ("nan", "inf", -1, 0, True, []):
            with self.subTest(duration=duration):
                opener = Opener([{"access_token": "ACCESS", "refresh_token": "REFRESH", "expires_in": duration}])
                with self.assertRaises(AuthError):
                    AuthClient(opener=opener).login("owner@example.ru", "PASSWORD")

    def test_atomic_local_storage_and_permissions(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "credentials.json"
            store = TokenStore(path)
            tokens = Tokens("ACCESS", "REFRESH", "owner@example.ru")
            store.save(tokens)
            self.assertEqual(store.load(), tokens)
            self.assertEqual([p.name for p in Path(folder).iterdir()], ["credentials.json"])
            if os.name != "nt":
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
                path.chmod(0o644)
                with self.assertRaises(AuthError) as caught:
                    store.load()
                self.assertEqual(caught.exception.code, "unsafe_token_permissions")

    @unittest.skipIf(os.name == "nt", "POSIX symlink check")
    def test_load_refuses_symlink(self):
        with tempfile.TemporaryDirectory() as folder:
            original = Path(folder) / "real.json"
            TokenStore(original).save(Tokens("ACCESS", "REFRESH", "owner@example.ru"))
            link = Path(folder) / "link.json"
            link.symlink_to(original)
            with self.assertRaises(AuthError):
                TokenStore(link).load()

    def test_refresh_manager_does_not_repeat_refresh_for_same_rejection(self):
        with tempfile.TemporaryDirectory() as folder:
            store = TokenStore(Path(folder) / "credentials.json")
            store.save(Tokens("OLD", "REFRESH", "owner@example.ru"))
            opener = Opener([{"access_token": "NEW", "refresh_token": "ROTATED"}])
            manager = TokenManager(store, AuthClient(opener=opener))
            self.assertEqual(manager.access_token(force_refresh=True, rejected_token="OLD"), "NEW")
            self.assertEqual(manager.access_token(force_refresh=True, rejected_token="OLD"), "NEW")
            self.assertEqual(len(opener.requests), 1)
            self.assertEqual(store.load().refresh_token, "ROTATED")


if __name__ == "__main__":
    unittest.main()
