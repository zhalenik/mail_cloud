"""Exercise the real image without Mail credentials or external cloud requests.

Checks the image's default user and writable /state, then a read-only root
filesystem with host-owned bind mounts and an explicit non-root UID/GID.
"""
from __future__ import annotations

import argparse
from http.cookiejar import CookieJar
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.request import HTTPCookieProcessor, Request, build_opener
import uuid


SQLITE_SMOKE = r"""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sqlite3
import tempfile
import threading

assert sqlite3.sqlite_version == '3.54.1', sqlite3.sqlite_version
assert sqlite3.threadsafety == 3, sqlite3.threadsafety
expected_source = '2026-10-16 14:45:20 555e07ef33f69a0957d24d8e555b702464a131d6f1607f4c3c1e4551e8e6ca76'
with tempfile.TemporaryDirectory(prefix='sqlite-smoke-', dir='/state') as directory:
    database = str(Path(directory) / 'check.sqlite3')
    def connect():
        connection = sqlite3.connect(database, timeout=30, isolation_level=None)
        assert connection.execute('PRAGMA journal_mode=WAL').fetchone()[0] == 'wal'
        connection.execute('PRAGMA synchronous=FULL')
        assert connection.execute('PRAGMA synchronous').fetchone()[0] == 2
        connection.execute('PRAGMA wal_autocheckpoint=1')
        return connection
    reader = connect()
    assert reader.execute('SELECT sqlite_source_id()').fetchone()[0] == expected_source
    reader.execute('CREATE TABLE checks (id INTEGER PRIMARY KEY, value TEXT NOT NULL)')
    start = threading.Barrier(5)
    def writer(worker):
        connection = connect()
        try:
            start.wait(timeout=30)
            for offset in range(25):
                row = worker * 25 + offset
                connection.execute('INSERT INTO checks VALUES (?, ?)', (row, str(row)))
        finally:
            connection.close()
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(writer, worker) for worker in range(4)]
        start.wait(timeout=30)
        while not all(future.done() for future in futures):
            reader.execute('SELECT COUNT(*) FROM checks').fetchone()
            reader.execute('PRAGMA wal_checkpoint(PASSIVE)').fetchone()
            threading.Event().wait(0.01)
        for future in futures:
            future.result()
    assert reader.execute('SELECT COUNT(*), SUM(id) FROM checks').fetchone() == (100, 4950)
    assert reader.execute('PRAGMA quick_check').fetchone()[0] == 'ok'
    reader.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()
    reader.close()
    restored = sqlite3.connect(database)
    assert restored.execute('SELECT COUNT(*) FROM checks').fetchone()[0] == 100
    restored.close()
print('Verified loaded SQLite 3.54.1, concurrent WAL writes, FULL durability, and reopened data')
"""


def docker(*args: str) -> str:
    return subprocess.check_output(["docker", *args], text=True).strip()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def smoke(image: str, *, version: str | None = None) -> None:
    config = json.loads(docker("image", "inspect", image))[0]
    user = config["Config"].get("User", "").split(":", 1)[0]
    require(user not in {"", "0", "root"}, "The image must have a non-root default USER")
    require(config.get("Architecture") == "amd64", "Expected the linux/amd64 image")
    require(config.get("Os") == "linux", "Expected a Linux image")
    require(bool(config["Config"].get("Healthcheck")), "The image must declare a HEALTHCHECK")

    with tempfile.TemporaryDirectory(prefix="mail-cloud-smoke-") as temporary:
        base = Path(temporary)
        state, photos = base / "state", base / "photos"
        state.mkdir(mode=0o700)
        photos.mkdir(mode=0o755)
        # GitHub's Ubuntu runner has a non-root host account. This fallback also
        # allows a developer running the script as root to exercise a non-root UID.
        uid = os.getuid() or 23456
        gid = os.getgid() if os.getuid() else 23456
        if os.getuid() == 0:
            os.chown(state, uid, gid)
        for mounted in (False, True):
            name = "mail-cloud-smoke-" + uuid.uuid4().hex[:12]
            args = ["run", "--detach", "--name", name, "--init",
                    "--cap-drop=ALL", "--security-opt=no-new-privileges:true",
                    "--publish", "127.0.0.1::8787"]
            if mounted:
                args += ["--user", f"{uid}:{gid}", "--read-only",
                         "--tmpfs", "/tmp:rw,noexec,nosuid,size=64m,mode=1777",
                         "--mount", f"type=bind,src={state},dst=/state",
                         "--mount", f"type=bind,src={photos},dst=/photos,readonly"]
            try:
                docker(*args, image)
                bound = json.loads(docker("inspect", name))[0]["NetworkSettings"]["Ports"]["8787/tcp"][0]
                origin = f"http://127.0.0.1:{bound['HostPort']}"
                opener = build_opener(HTTPCookieProcessor(CookieJar()))

                def request(path, data=None, csrf=None, expected=200):
                    headers = {"Origin": origin}
                    body = None
                    if data is not None:
                        body = json.dumps(data).encode()
                        headers.update({"Content-Type": "application/json", "X-CSRF-Token": csrf or ""})
                    try:
                        response = opener.open(Request(origin + path, data=body, headers=headers), timeout=5)
                    except HTTPError as error:
                        response = error
                    with response:
                        require(response.status == expected, f"{path}: expected HTTP {expected}, got {response.status}")
                        raw = response.read()
                        value = json.loads(raw) if "application/json" in response.headers.get("Content-Type", "") else raw
                        return value, response.headers

                deadline = time.monotonic() + 60
                while True:
                    try:
                        health, _ = request("/healthz")
                        require(health.get("ok") is True, "Health check did not report success")
                        if version is not None:
                            require(health.get("version") == version, "Running image has the wrong version")
                        break
                    except (URLError, OSError):
                        if time.monotonic() >= deadline:
                            raise RuntimeError("Container did not become ready within 60 seconds") from None
                        require(json.loads(docker("inspect", name))[0]["State"]["Running"], "Container exited before becoming ready")
                        time.sleep(0.5)

                process_uid = int(docker("exec", name, "python", "-c", "import os; print(os.getuid())"))
                require(process_uid != 0, "The application process is running as root")
                if mounted:
                    require(process_uid == uid, "Explicit mounted-state UID was not honored")
                print(docker("exec", name, "python", "-c", SQLITE_SMOKE))
                for path in ("/", "/static/app.js", "/static/style.css"):
                    page, _ = request(path)
                    require(bool(page), f"Empty static asset: {path}")
                request("/api/state", expected=401)
                session, headers = request("/api/session")
                require(session["setup_required"] is True and session["authenticated"] is False,
                        "A fresh image should require panel setup")
                cookie = headers.get("Set-Cookie", "")
                require("HttpOnly" in cookie and "SameSite=Strict" in cookie, "Panel cookie flags are missing")
                # This is a generated, disposable panel test password, never a
                # Mail account password. No cloud account is configured.
                password = secrets.token_urlsafe(24)
                session, _ = request("/api/session/setup", {"password": password}, session["csrf"])
                require(session["authenticated"] is True, "Panel setup did not establish a session")
                initial, _ = request("/api/state")
                require(initial["authenticated"] is False, "A fresh image must not claim Mail account authorization")
                require(initial["settings"]["preflight_check"] is True, "Preflight must default to enabled")
                changed, _ = request("/api/settings", {"workers": 2, "auto_resume": True,
                                                        "preflight_check": False}, session["csrf"])
                require(changed["settings"]["preflight_check"] is False, "The preflight setting was not saved")
                if mounted:
                    saved = docker("exec", name, "python", "-c",
                                   "from pathlib import Path; import json, stat; "
                                   "p=Path('/state/web_access.json'); "
                                   "print(json.dumps({'mode':stat.S_IMODE(p.stat().st_mode), 'data':p.read_text()}))")
                    access = json.loads(saved)
                    require(access["mode"] == 0o600, "Panel state is not mode 0600")
                    require(password not in access["data"], "Panel password was stored in plaintext")
                    # Preserve both the session cookie and the persisted access
                    # settings across the application's real shutdown/restart.
                    docker("restart", "--time", "15", name)
                    # An automatically assigned host port can change on restart.
                    bound = json.loads(docker("inspect", name))[0]["NetworkSettings"]["Ports"]["8787/tcp"][0]
                    origin = f"http://127.0.0.1:{bound['HostPort']}"
                    deadline = time.monotonic() + 60
                    while True:
                        try:
                            session, _ = request("/api/session")
                            break
                        except (URLError, OSError):
                            if time.monotonic() >= deadline:
                                raise RuntimeError("Container did not restart within 60 seconds") from None
                            time.sleep(0.5)
                    require(session["authenticated"] is True and session["setup_required"] is False,
                            "Mounted state or panel session did not survive a restart")
                    restored, _ = request("/api/state")
                    require(restored["settings"]["preflight_check"] is False,
                            "The preflight setting did not survive a container restart")
                # Execute the Dockerfile's health command as the configured user,
                # rather than waiting for the scheduled interval.
                check = config["Config"]["Healthcheck"]["Test"]
                if check[0] == "CMD":
                    docker("exec", name, *check[1:])
                elif check[0] == "CMD-SHELL":
                    docker("exec", name, "/bin/sh", "-c", check[1])
                else:
                    raise RuntimeError("The image HEALTHCHECK is disabled")
                docker("stop", "--time", "15", name)
                status = json.loads(docker("inspect", name))[0]["State"]["ExitCode"]
                require(status == 0, f"Application did not stop cleanly: exit status {status}")
                print(f"Passed container smoke test: {'mounted state / explicit UID' if mounted else 'default USER / image state'}")
            except BaseException:
                subprocess.run(["docker", "logs", "--tail", "80", name], check=False)
                raise
            finally:
                subprocess.run(["docker", "rm", "--force", name], check=False, stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("image")
    parser.add_argument("--version")
    arguments = parser.parse_args()
    smoke(arguments.image, version=arguments.version)
