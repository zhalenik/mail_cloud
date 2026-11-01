"""Bounded read retries; no real account or network operations."""

import io
import unittest
from unittest import mock

from mailru_photos.api import CloudAPI
from mailru_photos.auth import Tokens
from mailru_photos.transport import Cancelled, Response, TransportError


class TokensStub:
    def __init__(self):
        self.tokens = Tokens("OLD", "REFRESH", "owner@example.test")
        self.refreshes = 0

    def access_token(self, *, force_refresh=False, rejected_token=None):
        if force_refresh:
            self.refreshes += 1
            self.tokens = Tokens("NEW", "REFRESH", self.tokens.username)
        return self.tokens.access_token


class Responses:
    def __init__(self, items):
        self.items = list(items)
        self.requests = []

    def request(self, method, url, **kwargs):
        self.requests.append({"method": method, "url": url, **kwargs})
        if not self.items:
            raise AssertionError("Unexpected extra network attempt")
        item = self.items.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    def close(self):
        pass


def response(status, retry_after=None):
    return Response(status, {} if retry_after is None else {"retry-after": retry_after}, b"{}")


class ReadRetryTests(unittest.TestCase):
    url = "https://cloud.mail.ru/api/v4/user/quota"

    def api(self, items):
        wire = Responses(items)
        tokens = TokensStub()
        api = CloudAPI(tokens, transport_factory=lambda: wire)
        api._wait = mock.Mock()
        return api, wire, tokens

    def test_read_retries_two_transient_network_failures_with_bounded_backoff(self):
        api, wire, _ = self.api([TransportError("timeout", category="timeout"),
                                 TransportError("reset", category="connection_reset"), response(200)])
        self.assertEqual(api._request("GET", self.url).status, 200)
        self.assertEqual(len(wire.requests), 3)
        self.assertEqual(api._wait.call_args_list, [mock.call(1), mock.call(2)])

    def test_read_network_retries_stop_after_third_attempt(self):
        last = TransportError("third failure", category="dns_error")
        api, wire, _ = self.api([TransportError("legacy failure"),
                                 TransportError("HTTP interruption", category="http_protocol"), last])
        with self.assertRaises(TransportError) as caught:
            api._request("GET", self.url)
        self.assertIs(caught.exception, last)
        self.assertEqual(len(wire.requests), 3)
        self.assertEqual(api._wait.call_args_list, [mock.call(1), mock.call(2)])

    def test_tls_configuration_and_encoding_errors_are_not_retried(self):
        for category in ("tls_certificate", "tls_error", "proxy_configuration", "invalid_url", "encoding_error"):
            with self.subTest(category=category):
                failure = TransportError("nonretryable", category=category)
                api, wire, _ = self.api([failure, response(200)])
                with self.assertRaises(TransportError) as caught:
                    api._request("GET", self.url)
                self.assertIs(caught.exception, failure)
                self.assertEqual(len(wire.requests), 1)
                api._wait.assert_not_called()

    def test_read_retries_only_selected_gateway_statuses(self):
        for status in (502, 503, 504):
            with self.subTest(status=status):
                api, wire, _ = self.api([response(status), response(status), response(200)])
                self.assertEqual(api._request("GET", self.url).status, 200)
                self.assertEqual(len(wire.requests), 3)
                self.assertEqual(api._wait.call_args_list, [mock.call(1), mock.call(2)])
        for status in (400, 404, 500):
            with self.subTest(status=status):
                api, wire, _ = self.api([response(status), response(200)])
                self.assertEqual(api._request("GET", self.url).status, status)
                self.assertEqual(len(wire.requests), 1)
                api._wait.assert_not_called()

    def test_read_gateway_retries_return_third_unsuccessful_response(self):
        api, wire, _ = self.api([response(503), response(503), response(503)])
        self.assertEqual(api._request("GET", self.url).status, 503)
        self.assertEqual(len(wire.requests), 3)
        self.assertEqual(api._wait.call_args_list, [mock.call(1), mock.call(2)])

    def test_gateway_retry_after_is_respected_with_maximum_inline_wait_30_seconds(self):
        api, wire, _ = self.api([response(503, "7"), response(504, "30"), response(200)])
        self.assertEqual(api._request("GET", self.url).status, 200)
        self.assertEqual(api._wait.call_args_list, [mock.call(7), mock.call(30)])
        for delay in ("31", "100000000000000000000", "not-a-delay", "inf"):
            with self.subTest(delay=delay):
                api, wire, _ = self.api([response(503, delay), response(200)])
                self.assertEqual(api._request("GET", self.url).status, 503)
                self.assertEqual(len(wire.requests), 1)
                api._wait.assert_not_called()

    def test_post_put_and_get_with_payload_never_retry_transient_failures(self):
        cases = (("POST", {"body": b"commit"}), ("PUT", {"body": b"blob"}),
                 ("PUT", {}), ("GET", {"body": b"payload"}),
                 ("GET", {"stream": io.BytesIO(b"payload"), "length": 7}))
        for method, kwargs in cases:
            for item in (response(503), TransportError("interrupted", category="timeout")):
                with self.subTest(method=method, payload_kind=tuple(kwargs), failure_type=type(item).__name__):
                    api, wire, _ = self.api([item, response(200)])
                    if isinstance(item, TransportError):
                        with self.assertRaises(TransportError):
                            api._request(method, self.url, **kwargs)
                    else:
                        self.assertEqual(api._request(method, self.url, **kwargs).status, 503)
                    self.assertEqual(len(wire.requests), 1)
                    api._wait.assert_not_called()

    def test_authentication_refresh_happens_once_around_read_retries(self):
        api, wire, tokens = self.api([response(503), response(401), response(503), response(403)])
        self.assertEqual(api._request("GET", self.url).status, 403)
        self.assertEqual(tokens.refreshes, 1)
        self.assertEqual([r["headers"]["Authorization"] for r in wire.requests],
                         ["Bearer OLD", "Bearer OLD", "Bearer NEW", "Bearer NEW"])
        self.assertEqual(api._wait.call_args_list, [mock.call(1), mock.call(1)])

    def test_429_sets_shared_delay_without_retrying_that_response(self):
        api, wire, tokens = self.api([response(429, "12"), response(200)])
        with mock.patch("mailru_photos.api.time.monotonic", return_value=100):
            self.assertEqual(api._request("GET", self.url).status, 429)
            api._wait.assert_not_called()
            self.assertEqual(api._not_before, 112)
            self.assertEqual(api._request("GET", self.url).status, 200)
        self.assertEqual(len(wire.requests), 2)
        api._wait.assert_called_once_with(12)
        self.assertEqual(tokens.refreshes, 0)

    def test_backoff_uses_cancellable_wait_and_stops_before_second_request(self):
        api, wire, _ = self.api([response(503), response(200)])
        api._wait.side_effect = Cancelled("stopped during read retry")
        with self.assertRaises(Cancelled):
            api._request("GET", self.url)
        self.assertEqual(len(wire.requests), 1)


if __name__ == "__main__":
    unittest.main()
