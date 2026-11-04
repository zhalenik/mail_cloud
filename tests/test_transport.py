"""HTTP boundary tests using mocks or loopback sockets, never real accounts."""

import base64
import io
import json
import socket
import ssl
import threading
import tempfile
import unicodedata
import unittest
from unittest import mock

from mailru_photos import transport


class ResponseLimitTests(unittest.TestCase):
    def test_folder_response_is_bounded_before_json_and_discarded_connection_is_closed(self):
        from types import SimpleNamespace
        from mailru_photos.api import CloudAPI

        class UnboundedIncoming:
            status = 200
            will_close = False

            def __init__(self):
                self.read_sizes = []
                self.bytes_produced = 0

            def read(self, amount=None):
                if amount is None or amount < 0:
                    raise AssertionError("A cloud listing must not read an unbounded response")
                self.read_sizes.append(amount)
                self.bytes_produced += amount
                # Lazily stand in for a server ignoring limit=500 and sending
                # gigabytes. Only the explicitly requested bounded prefix exists.
                return b" " * amount

            def getheaders(self):
                return [("Content-Length", str(8 * 1024 ** 3)), ("Content-Type", "application/json")]

        incoming = UnboundedIncoming()
        connection = mock.Mock()
        connection.getresponse.return_value = incoming
        client = transport.HTTP()
        self.addCleanup(client.close)
        tokens = SimpleNamespace(access_token=lambda **_: "OFFLINE-FIXTURE-TOKEN",
                                 tokens=SimpleNamespace(client_id="cloud-android"))
        api = CloudAPI(tokens, transport_factory=lambda **_: client)
        self.addCleanup(api.close_thread)
        with mock.patch.object(client, "_new_connection", return_value=connection), \
                mock.patch.object(api, "_wait"), \
                mock.patch("mailru_photos.api._json") as parse:
            with self.assertRaises(transport.TransportError):
                api.list_folder("/", limit=500)
        self.assertTrue(incoming.read_sizes)
        self.assertLessEqual(len(incoming.read_sizes), 3, "A read-only retry must also be bounded")
        self.assertTrue(all(size == 2 * 1024 * 1024 + 1 for size in incoming.read_sizes))
        self.assertEqual(incoming.bytes_produced, len(incoming.read_sizes) * (2 * 1024 * 1024 + 1))
        self.assertEqual(connection.close.call_count, len(incoming.read_sizes))
        self.assertFalse(client._connections)
        parse.assert_not_called()

    def test_response_exactly_at_cap_remains_usable(self):
        incoming = mock.Mock()
        incoming.status = 200
        incoming.will_close = False
        incoming.read.return_value = b" " * (2 * 1024 * 1024)
        incoming.getheaders.return_value = []
        connection = mock.Mock()
        connection.getresponse.return_value = incoming
        client = transport.HTTP()
        self.addCleanup(client.close)
        with mock.patch.object(client, "_new_connection", return_value=connection):
            result = client.request("POST", "https://cloud.mail.ru/api/m1/folder?limit=500", body=b"home=%2F")
        self.assertEqual(len(result.body), 2 * 1024 * 1024)
        incoming.read.assert_called_once_with(2 * 1024 * 1024 + 1)
        connection.close.assert_not_called()


class UnicodeLoopbackTests(unittest.TestCase):
    def test_cyrillic_folders_and_nfd_names_cross_real_http_encoder_in_utf8(self):
        # Exercise stdlib request-line/header serialization and actual socket
        # traffic. The only connection factory is redirected to loopback;
        # production HTTPS domain validation stays intact and is still called.
        import http.client
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        from pathlib import Path
        from types import SimpleNamespace
        from urllib.parse import unquote, urlsplit
        from mailru_photos.api import CloudAPI, FileIdentity, cloud_hash
        from mailru_photos.metadata import encode_mkdir

        for remote_input in ('/Семья/Отпуск/ёж #?%.jpg', '/Семья/Отпуск/е\u0308ж #?%.jpg'):
            with self.subTest(remote=remote_input), tempfile.TemporaryDirectory() as directory:
                local = Path(directory) / 'Папка на диске' / 'е\u0308ж #?%.jpg'
                local.parent.mkdir()
                local.write_bytes(b'\xff\xd8\xff' + bytes(range(256)) * 4)
                digest, identity = cloud_hash(local)
                remote = unicodedata.normalize('NFC', remote_input)
                requests = []
                state = {'committed': False}

                class Handler(BaseHTTPRequestHandler):
                    def log_message(self, *_):
                        pass

                    def handle_request(self):
                        raw = self.rfile.read(int(self.headers.get('Content-Length', '0')))
                        requests.append((self.command, self.path, dict(self.headers), raw))
                        target = urlsplit(self.path).path
                        status, headers = 200, {}
                        if target == '/m':
                            body = b'https://metadata.cloud.mail.ru/'
                        elif self.command == 'GET' and target.startswith('/api/v1/private/stat/'):
                            if state['committed']:
                                body = json.dumps({'hash': digest, 'size': identity.size}).encode('utf-8')
                            else:
                                status, body = 404, b'{}'
                        elif self.command == 'POST' and target == '/':
                            body = b'\x00\x01'
                        elif self.command == 'PUT':
                            status, body, headers = 201, b'', {'X-Valid-for': '0'}
                        elif self.command == 'POST' and target == '/api/v1/private/add':
                            payload = json.loads(raw.decode('utf-8'))
                            state['committed'] = True
                            body = json.dumps({'name': payload['path'].rsplit('/', 1)[1],
                                               'path': payload['path'], 'is_unlim_upload': True},
                                              ensure_ascii=False).encode('utf-8')
                        else:
                            status, body = 500, b'{}'
                        self.send_response(status)
                        for name, value in headers.items():
                            self.send_header(name, value)
                        self.send_header('Content-Length', str(len(body)))
                        self.end_headers()
                        self.wfile.write(body)

                    do_GET = do_POST = do_PUT = handle_request

                server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()

                class LoopbackHTTP(transport.HTTP):
                    def _new_connection(self, _host):
                        return http.client.HTTPConnection(*server.server_address, timeout=5)

                tokens = SimpleNamespace(access_token=lambda **_: 'LOOPBACK-TEST-TOKEN',
                                         tokens=SimpleNamespace(client_id='cloud-android'))
                api = CloudAPI(tokens, transport_factory=LoopbackHTTP)
                try:
                    result = api.upload_photo(str(local), remote_input, expected=FileIdentity.from_stat(local.stat()),
                                              save_intent=lambda _: None, save_confirmation=lambda _: None)
                finally:
                    api.close_thread()
                    server.shutdown()
                    server.server_close()
                    thread.join(timeout=5)
                self.assertEqual(result['path'], remote)
                self.assertIs(result['verified'], True)
                self.assertTrue(all(target.isascii() for _, target, _, _ in requests))
                stats = [unquote(urlsplit(target).path) for method, target, _, _ in requests
                         if method == 'GET' and '/private/stat' in target]
                self.assertEqual(stats, ['/api/v1/private/stat' + remote] * 2)
                metadata = [body for method, target, _, body in requests
                            if method == 'POST' and urlsplit(target).path == '/']
                self.assertEqual(metadata, [encode_mkdir('/Семья'), encode_mkdir('/Семья/Отпуск')])
                commit = next(body for method, target, _, body in requests if '/private/add' in target)
                self.assertIn('/Семья/Отпуск/'.encode('utf-8'), commit)
                self.assertEqual(json.loads(commit)['path'], remote)


class ProxyTransportTests(unittest.TestCase):
    def response(self):
        response = mock.Mock()
        response.status = 200
        response.read.return_value = b"{}"
        response.getheaders.return_value = [("Content-Type", "application/json")]
        response.will_close = False
        return response

    def test_no_https_proxy_connects_directly_and_keeps_origin_tls_context(self):
        with mock.patch.object(transport, "getproxies", return_value={"http": "http://unused.example:8080"}), \
             mock.patch.object(transport, "proxy_bypass") as bypass, \
             mock.patch.object(transport.http.client, "HTTPSConnection") as constructor:
            constructor.return_value.getresponse.return_value = self.response()
            client = transport.HTTP(timeout=37)
            self.addCleanup(client.close)
            client.request("GET", "https://android-api.cloud.mail.ru/api/v1/user/info")
        constructor.assert_called_once_with("android-api.cloud.mail.ru", timeout=37, context=client._context)
        constructor.return_value.set_tunnel.assert_not_called()
        bypass.assert_not_called()
        self.assertTrue(client._context.check_hostname)
        self.assertEqual(client._context.verify_mode, ssl.CERT_REQUIRED)

    def test_no_proxy_bypass_uses_direct_connection(self):
        with mock.patch.object(transport, "getproxies", return_value={"https": "http://proxy.example:8080"}), \
             mock.patch.object(transport, "proxy_bypass", return_value=True) as bypass, \
             mock.patch.object(transport.http.client, "HTTPSConnection") as constructor:
            constructor.return_value.getresponse.return_value = self.response()
            client = transport.HTTP()
            self.addCleanup(client.close)
            client.request("GET", "https://android-api.cloud.mail.ru/api/v1/user/info")
        bypass.assert_called_once_with("android-api.cloud.mail.ru")
        constructor.assert_called_once_with("android-api.cloud.mail.ru", timeout=90, context=client._context)
        constructor.return_value.set_tunnel.assert_not_called()

    def test_connect_keeps_origin_sni_and_proxy_credentials_out_of_origin_request(self):
        # Exercise actual stdlib HTTPSConnection.connect() with a fake TCP socket,
        # so the assertion covers tunnel/SNI behavior, not only constructor mocks.
        sock = mock.Mock()
        sock.makefile.side_effect = [
            io.BytesIO(b"HTTP/1.1 200 Connection Established\r\n\r\n"),
            io.BytesIO(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}"),
        ]
        payload = b"Z" * (1024 * 1024 + 7)
        progress = []
        client = transport.HTTP(timeout=37)
        self.addCleanup(client.close)
        with mock.patch.object(transport, "getproxies", return_value={"https": "http://fixture-user:p%40ss%3Aword@proxy.example:8080"}), \
             mock.patch.object(transport, "proxy_bypass", return_value=False), \
             mock.patch.object(transport.http.client.socket, "create_connection", return_value=sock) as connect, \
             mock.patch.object(client._context, "wrap_socket", return_value=sock) as tls:
            result = client.request("PUT", "https://uploader.cloud.mail.ru/resumable/fixture?part=1",
                                    headers={"Authorization": "Bearer ORIGIN-TOKEN"},
                                    stream=io.BytesIO(payload), length=len(payload), progress=progress.append)
        self.assertEqual(result.status, 200)
        self.assertEqual(result.body, b"{}")
        self.assertEqual(connect.call_args.args[0], ("proxy.example", 8080))
        tls.assert_called_once_with(sock, server_hostname="uploader.cloud.mail.ru")
        sent = [call.args[0] for call in sock.sendall.call_args_list]
        self.assertTrue(sent[0].startswith(b"CONNECT uploader.cloud.mail.ru:443 HTTP/1."))
        expected = b"Basic " + base64.b64encode(b"fixture-user:p@ss:word")
        self.assertIn(b"Proxy-Authorization: " + expected, sent[0])
        self.assertNotIn(b"ORIGIN-TOKEN", sent[0])
        self.assertTrue(sent[1].startswith(b"PUT /resumable/fixture?part=1 HTTP/1.1\r\n"))
        self.assertIn(b"Host: uploader.cloud.mail.ru\r\n", sent[1])
        self.assertIn(b"Authorization: Bearer ORIGIN-TOKEN\r\n", sent[1])
        self.assertNotIn(b"Proxy-Authorization", b"".join(sent[1:]))
        self.assertNotIn(expected, b"".join(sent[1:]))
        self.assertEqual(b"".join(sent[2:]), payload)
        self.assertEqual(progress, [1024 * 1024, 7])

    def test_proxy_tunnels_are_persistent_per_origin_and_use_default_http_port(self):
        with mock.patch.object(transport, "getproxies", return_value={"https": "http://proxy.example"}), \
             mock.patch.object(transport, "proxy_bypass", return_value=False), \
             mock.patch.object(transport.http.client, "HTTPSConnection") as constructor:
            first = mock.Mock()
            second = mock.Mock()
            first.getresponse.return_value = self.response()
            second.getresponse.return_value = self.response()
            constructor.side_effect = [first, second]
            client = transport.HTTP()
            client.request("GET", "https://android-api.cloud.mail.ru/api/v1/user/info")
            client.request("GET", "https://android-api.cloud.mail.ru/api/v1/private/stat/photo.jpg")
            client.request("GET", "https://alt-cloud.mail.ru/api/v4/user/quota")
            self.assertEqual(constructor.call_count, 2)
            for call in constructor.call_args_list:
                self.assertEqual(call.args, ("proxy.example", 80))
                self.assertIs(call.kwargs["context"], client._context)
            first.set_tunnel.assert_called_once_with("android-api.cloud.mail.ru", 443, headers={})
            second.set_tunnel.assert_called_once_with("alt-cloud.mail.ru", 443, headers={})
            self.assertEqual(first.putrequest.call_count, 2)
            client.close()
            first.close.assert_called_once()
            second.close.assert_called_once()

    def test_invalid_or_unsupported_proxy_cannot_silently_fall_back_to_direct(self):
        proxies = (
            "https://fixture-user:DO-NOT-LEAK@proxy.example:8443",
            "socks5://fixture-user:DO-NOT-LEAK@proxy.example:1080",
            "proxy.example:8080",
            "http://fixture-user:DO-NOT-LEAK@proxy.example:0",
            "http://fixture-user:DO-NOT-LEAK@proxy.example:70000",
            "http://fixture-user:DO-NOT-LEAK@proxy.example:bad",
            "http://proxy.example/unexpected/path",
            "http://proxy.example?secret=DO-NOT-LEAK",
            "http://proxy.example#DO-NOT-LEAK",
            "http://fixture-user:DO-NOT-LEAK%0d%0a@proxy.example",
            "http://proxy.example\nDO-NOT-LEAK",
            "http://[broken",
        )
        for proxy in proxies:
            with self.subTest(proxy_scheme=proxy.split(":", 1)[0]), \
                 mock.patch.object(transport, "getproxies", return_value={"https": proxy}), \
                 mock.patch.object(transport, "proxy_bypass", return_value=False), \
                 mock.patch.object(transport.http.client, "HTTPSConnection") as constructor:
                client = transport.HTTP()
                with self.assertRaises(transport.TransportError) as caught:
                    client.request("GET", "https://android-api.cloud.mail.ru/api/v1/user/info")
                self.assertNotIn("DO-NOT-LEAK", str(caught.exception))
                constructor.assert_not_called()
                client.close()

    def test_tunnel_failure_never_leaks_proxy_credentials_or_raw_proxy_response(self):
        sock = mock.Mock()
        sock.makefile.return_value = io.BytesIO(b"HTTP/1.1 407 DO-NOT-LEAK-REASON\r\n\r\n")
        client = transport.HTTP()
        self.addCleanup(client.close)
        with mock.patch.object(transport, "getproxies", return_value={"https": "http://fixture-user:DO-NOT-LEAK@proxy.example:8080"}), \
             mock.patch.object(transport, "proxy_bypass", return_value=False), \
             mock.patch.object(transport.http.client.socket, "create_connection", return_value=sock), \
             mock.patch.object(client._context, "wrap_socket", return_value=sock) as tls:
            with self.assertRaises(transport.TransportError) as caught:
                client.request("GET", "https://android-api.cloud.mail.ru/api/v1/user/info",
                               headers={"Authorization": "Bearer ORIGIN-TOKEN"})
        self.assertNotIn("DO-NOT-LEAK", str(caught.exception))
        self.assertNotIn("fixture-user", str(caught.exception))
        tls.assert_not_called()
        self.assertNotIn(b"ORIGIN-TOKEN", b"".join(call.args[0] for call in sock.sendall.call_args_list))
        self.assertFalse(client._connections)

    def test_explicit_proxy_header_is_never_forwarded_to_origin(self):
        with mock.patch.object(transport.http.client, "HTTPSConnection") as constructor:
            client = transport.HTTP()
            with self.assertRaises(transport.TransportError) as caught:
                client.request("GET", "https://android-api.cloud.mail.ru/api/v1/user/info",
                               headers={"pRoXy-AuThOrIzAtIoN": "Basic DO-NOT-LEAK"})
        self.assertNotIn("DO-NOT-LEAK", str(caught.exception))
        constructor.assert_not_called()
        client.close()


class NetworkDiagnosticTests(unittest.TestCase):
    def test_task_cancel_closes_current_body_and_a_new_signal_can_reuse_worker(self):
        import threading
        class TaskEvent(threading.Event):
            def check(self):
                if self.is_set():
                    raise transport.TaskCancelled('Offline task pause')
        task = TaskEvent()
        with mock.patch.object(transport, 'getproxies', return_value={}), \
             mock.patch.object(transport.http.client, 'HTTPSConnection') as constructor:
            connection = constructor.return_value
            client = transport.HTTP(stop=task)
            with self.assertRaises(transport.TaskCancelled):
                client.request('PUT', 'https://uploader.cloud.mail.ru/resumable/TEST',
                               stream=io.BytesIO(b'a' * (2 * 1024 * 1024)), length=2 * 1024 * 1024,
                               progress=lambda count: task.set())
            connection.send.assert_called_once()
            connection.getresponse.assert_not_called()
            connection.close.assert_called_once()
            self.assertFalse(client._connections)
            incoming = connection.getresponse.return_value
            incoming.status, incoming.will_close = 200, False
            incoming.read.return_value = b'{}'
            incoming.getheaders.return_value = []
            client.stop = threading.Event()
            self.assertEqual(client.request('GET', 'https://cloud.mail.ru/api/v4/user/quota').status, 200)
            self.assertEqual(constructor.call_count, 2)

    def test_local_stream_read_error_is_not_misclassified_as_network_failure(self):
        with mock.patch.object(transport, 'getproxies', return_value={}), \
             mock.patch.object(transport.http.client, 'HTTPSConnection') as constructor:
            stream = mock.Mock()
            stream.read.side_effect = PermissionError('private-source-detail')
            client = transport.HTTP()
            with self.assertRaises(transport.SourceReadError) as caught:
                client.request('PUT', 'https://uploader.cloud.mail.ru/resumable/TEST', stream=stream, length=100)
            self.assertNotIn('private-source-detail', str(caught.exception))
            constructor.return_value.close.assert_called_once()
            constructor.return_value.getresponse.assert_not_called()
            self.assertFalse(client._connections)

    def failed_request(self, error, *, method="GET",
                       url="https://cloud.mail.ru/api/v4/user/quota?access_token=DO-NOT-LEAK-URL",
                       stream=None, length=None):
        with mock.patch.object(transport, "getproxies", return_value={}), \
             mock.patch.object(transport.http.client, "HTTPSConnection") as constructor:
            connection = constructor.return_value
            connection.getresponse.side_effect = error
            client = transport.HTTP()
            with self.assertRaises(transport.TransportError) as caught:
                client.request(method, url, headers={"Authorization": "Bearer DO-NOT-LEAK-AUTH",
                               "Cookie": "DO-NOT-LEAK-COOKIE"}, stream=stream, length=length)
            self.assertFalse(client._connections)
            connection.close.assert_called_once()
            constructor.assert_called_once()
            connection.getresponse.assert_called_once()
        rendered = str(caught.exception) + repr(caught.exception) + json.dumps(caught.exception.safe_diagnostics())
        self.assertNotIn("DO-NOT-LEAK", rendered)
        self.assertNotIn("access_token", rendered)
        self.assertNotIn("/api/", rendered)
        self.assertTrue(caught.exception.__suppress_context__)
        return caught.exception

    def test_network_exception_categories_are_safe_and_distinct(self):
        cases = (
            (TimeoutError("DO-NOT-LEAK-TIMEOUT"), "timeout", None),
            (ssl.SSLCertVerificationError(1, "DO-NOT-LEAK-CERT"), "tls_certificate", 1),
            (ssl.SSLError(1, "DO-NOT-LEAK-TLS"), "tls_error", 1),
            (socket.gaierror(-2, "DO-NOT-LEAK-DNS"), "dns_error", -2),
            (ConnectionResetError(104, "DO-NOT-LEAK-RESET"), "connection_reset", 104),
            (ConnectionAbortedError(103, "DO-NOT-LEAK-ABORT"), "connection_reset", 103),
            (BrokenPipeError(32, "DO-NOT-LEAK-PIPE"), "connection_reset", 32),
            (OSError(5, "DO-NOT-LEAK-OS"), "network_error", 5),
            (transport.http.client.BadStatusLine("DO-NOT-LEAK-HTTP"), "http_protocol", None),
            (UnicodeError("DO-NOT-LEAK-ENCODING"), "encoding_error", None),
        )
        for error, category, errno in cases:
            with self.subTest(category=category, exception_type=type(error).__name__):
                failure = self.failed_request(error)
                self.assertEqual(failure.category, category)
                self.assertEqual(failure.errno, errno)
                self.assertEqual(failure.stage, "quota")
                self.assertEqual(failure.method, "GET")
                self.assertEqual(failure.host, "cloud.mail.ru")
                self.assertIn("stage=quota", str(failure))
                self.assertIn("category=" + category, str(failure))

    def test_stage_is_a_fixed_enum_not_a_remote_path_or_query(self):
        cases = (
            ("GET", "https://android-api.cloud.mail.ru/api/v1/user/info", "user_info", False),
            ("GET", "https://cloud.mail.ru/api/v4/user/quota", "quota", False),
            ("GET", "https://dispatcher.cloud.mail.ru/m", "metadata", False),
            ("POST", "https://cld-extapi.datacloudmail.ru/meta/", "metadata", False),
            ("GET", "https://android-api.cloud.mail.ru/api/v1/private/stat/DO-NOT-LEAK-PATH.jpg", "stat", False),
            ("PUT", "https://uploader.cloud.mail.ru/resumable/DO-NOT-LEAK-HASH", "blob_status", False),
            ("PUT", "https://cloclo1.datacloudmail.ru/DO-NOT-LEAK-UPLOAD", "blob_upload", True),
            ("POST", "https://android-api.cloud.mail.ru/api/v1/private/add", "unlimited_add", False),
        )
        for method, url, stage, sending in cases:
            with self.subTest(stage=stage, method=method):
                failure = self.failed_request(TimeoutError("DO-NOT-LEAK-ERROR"), method=method,
                    url=url + "?token=DO-NOT-LEAK-TOKEN", stream=io.BytesIO(b"DO-NOT-LEAK-BODY") if sending else None,
                    length=len(b"DO-NOT-LEAK-BODY") if sending else None)
                self.assertEqual(failure.stage, stage)
                self.assertEqual(failure.method, method)

    def test_failure_during_connection_creation_is_classified_without_details(self):
        with mock.patch.object(transport, "getproxies", return_value={}), \
             mock.patch.object(transport.http.client, "HTTPSConnection", side_effect=socket.gaierror(-2, "DO-NOT-LEAK")):
            client = transport.HTTP()
            with self.assertRaises(transport.TransportError) as caught:
                client.request("GET", "https://android-api.cloud.mail.ru/api/v1/user/info?token=DO-NOT-LEAK")
        self.assertEqual(caught.exception.category, "dns_error")
        self.assertEqual(caught.exception.stage, "user_info")
        self.assertNotIn("DO-NOT-LEAK", str(caught.exception))
        self.assertFalse(client._connections)

    def test_interrupted_body_is_not_automatically_retried(self):
        with mock.patch.object(transport, "getproxies", return_value={}), \
             mock.patch.object(transport.http.client, "HTTPSConnection") as constructor:
            connection = constructor.return_value
            connection.send.side_effect = BrokenPipeError(32, "DO-NOT-LEAK")
            client = transport.HTTP()
            with self.assertRaises(transport.TransportError) as caught:
                client.request("PUT", "https://uploader.cloud.mail.ru/resumable/DO-NOT-LEAK-HASH",
                               stream=io.BytesIO(b"DO-NOT-LEAK-BODY"), length=16)
        self.assertEqual(caught.exception.category, "connection_reset")
        self.assertEqual(caught.exception.stage, "blob_upload")
        self.assertNotIn("DO-NOT-LEAK", str(caught.exception))
        constructor.assert_called_once()
        connection.send.assert_called_once()
        connection.getresponse.assert_not_called()
        connection.close.assert_called_once()
        self.assertFalse(client._connections)

    def test_simple_errors_stay_compatible_and_untrusted_context_is_not_echoed(self):
        plain = transport.TransportError("Local fixed message")
        self.assertEqual(str(plain), "Local fixed message")
        self.assertEqual(plain.safe_diagnostics(), {})
        error = transport.TransportError("Local fixed message", category="DO-NOT-LEAK", method="DO-NOT-LEAK",
                                         stage="DO-NOT-LEAK", host="user:DO-NOT-LEAK@proxy.example/?secret=x", errno=True)
        self.assertEqual(str(error), "Local fixed message")
        self.assertEqual(error.safe_diagnostics(), {})


if __name__ == "__main__":
    unittest.main()
