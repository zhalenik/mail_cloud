"""Disposable real HTTP/UI fixture; remote cloud I/O and SMTP are offline.

This server binds only to loopback, owns its temporary state, and is used by
tests/browser_plans_real_v090.cjs. It must never serve a real user state folder.
"""
from __future__ import annotations

import json
from pathlib import Path
import signal
import sys
import tempfile
import threading
from socketserver import ThreadingMixIn
from unittest.mock import patch
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

from test_planning_service import EMAIL, PlannedCloud  # Only the offline remote boundary.
from mailru_photos.auth import TokenStore, Tokens
from mailru_photos.notifications import NotificationManager
from mailru_photos.web import WebApplication
from mailru_photos.web_service import WebService


class ThreadedServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True


class QuietHandler(WSGIRequestHandler):
    def log_message(self, _format, *args):
        pass


def main():
    with tempfile.TemporaryDirectory(prefix="mail-cloud-browser-") as temp:
        base = Path(temp)
        library = base / "Медиатека"
        archive = library / "Архив"
        manual = [library / "Ручные фото", library / "Ручные видео"]
        for relative in ("Лето/первый.jpg", "Лето/второй.jpg", "Зима/кадр.jpg", "корень.jpg", "Исключить/лишний.jpg"):
            file = archive / relative
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_bytes(relative.encode("utf-8") * 4 + b"-offline-browser-fixture")
        for directory in manual:
            directory.mkdir(parents=True)
            (directory / "ручной.jpg").write_bytes(directory.name.encode("utf-8") * 4)
        paged_sources = [library / "Проверка страниц" / f"Каталог {number:02d}" for number in range(1, 56)]
        for directory in paged_sources:
            directory.mkdir(parents=True)
        state = base / "state"
        state.mkdir()
        TokenStore(state / "auth.dat").save(Tokens("offline-browser-access", "offline-browser-refresh", EMAIL))
        cloud = PlannedCloud()
        cloud.block_after = 1

        def factory(tokens, *, stop):
            cloud.stop = stop
            return cloud

        with patch.object(NotificationManager, "start", return_value=None):
            service = WebService(state, [library], api_factory=factory, event_factory=threading.Event)
            service.setup("/Archive")
            service.settings(workers=1)
            production = WebApplication(service, state)

            def application(environ, start_response):
                pathname = environ.get("PATH_INFO", "")
                if pathname == "/__fixture/control" and environ.get("REQUEST_METHOD") == "POST":
                    value = json.loads(environ["wsgi.input"].read(int(environ.get("CONTENT_LENGTH") or "0")))
                    if "block_after" in value:
                        cloud.block_after = value["block_after"]
                    payload = {"ok": True}
                elif pathname == "/__fixture/status":
                    payload = {
                        "remote_count": len(cloud.remote),
                        "commit_count": len(cloud.commits),
                        "remote_paths": list(cloud.remote),
                        "notifications": service.notifications.status(),
                    }
                else:
                    return production(environ, start_response)
                encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                start_response("200 OK", [("Content-Type", "application/json; charset=utf-8"), ("Content-Length", str(len(encoded)))])
                return [encoded]

            server = make_server("127.0.0.1", 0, application, server_class=ThreadedServer, handler_class=QuietHandler)
            stop = threading.Event()

            def terminate(signum, frame):
                stop.set()
                threading.Thread(target=server.shutdown, daemon=True).start()

            signal.signal(signal.SIGTERM, terminate)
            signal.signal(signal.SIGINT, terminate)
            print(json.dumps({"origin": "http://127.0.0.1:" + str(server.server_port), "archive": str(archive), "manual_paths": [str(x) for x in manual], "paged_source_paths": [str(x) for x in paged_sources]}, ensure_ascii=False), flush=True)
            try:
                server.serve_forever(poll_interval=0.1)
            finally:
                cloud.block = False
                cloud.block_after = None
                service.close(timeout=3)
                server.server_close()


if __name__ == "__main__":
    main()
