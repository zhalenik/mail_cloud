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
import struct
import zlib
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
        def png_chunk(kind, data):
            return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data) & 0xffffffff)
        photo_bytes = (b"\x89PNG\r\n\x1a\n" + png_chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 6, 0, 0, 0))
                       + png_chunk(b"IDAT", zlib.compress(b"\x00\x20\x80\xf0\xff")) + png_chunk(b"IEND", b""))
        unicode_source = photos / "Семейный архив"
        unicode_source.mkdir(mode=0o755)
        photo = unicode_source / "снимок.png"
        photo.write_bytes(photo_bytes)
        photo.chmod(0o644)
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
                if not mounted:
                    docker("cp", str(unicode_source), f"{name}:/photos/Семейный архив")
                bound = json.loads(docker("inspect", name))[0]["NetworkSettings"]["Ports"]["8787/tcp"][0]
                origin = f"http://127.0.0.1:{bound['HostPort']}"
                opener = build_opener(HTTPCookieProcessor(CookieJar()))

                def request(path, data=None, csrf=None, expected=200, *, method=None, extra_headers=None):
                    headers = {"Origin": origin, **(extra_headers or {})}
                    body = None
                    if data is not None:
                        body = json.dumps(data).encode()
                        headers.update({"Content-Type": "application/json", "X-CSRF-Token": csrf or ""})
                    try:
                        response = opener.open(Request(origin + path, data=body, headers=headers, method=method), timeout=5)
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
                docker("exec", name, "python", "-c", "from zoneinfo import ZoneInfo; assert ZoneInfo('Europe/Moscow'); assert ZoneInfo('Asia/Yekaterinburg')")
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
                require("preflight_check" not in initial["settings"], "Preflight is no longer a setting")
                require(initial["settings"]["timezone"] == "Europe/Moscow", "Scheduler timezone default is missing")
                require(initial["settings"]["regular_policy"] == "skip", "Quota uploads must be opt-in")
                require(initial["settings"]["unexpected_unlimited_action"] == "trash", "Unexpected quota uploads default to trash")
                require("upload_large_files" not in initial["settings"],
                        "The legacy large-file toggle must not be restored")
                changed, _ = request("/api/settings", {"workers": 2, "auto_resume": True,
                                                        "regular_policy": "upload",
                                                        "unexpected_unlimited_action": "keep"}, session["csrf"])
                require(changed["settings"]["regular_policy"] == "upload", "Quota policy was not saved")
                require(changed["settings"]["unexpected_unlimited_action"] == "keep", "Unexpected outcome policy was not saved")
                require(initial["settings"]["remote_reconcile"] is False, "Remote comparison must be opt-in")
                require(initial["quota"] is None, "An account-free image must not invent quota values")
                sources, _ = request("/api/sources/add", {"path":"/photos/Семейный архив", "alias":"Семья", "remote_prefix":"/Архив/Семья"}, session["csrf"])
                source_id = sources["sources"][0]["id"]
                request("/api/scan", {}, session["csrf"])
                deadline = time.monotonic() + 30
                while True:
                    scanned, _ = request("/api/state")
                    if scanned["activity"]["phase"] != "scanning":
                        break
                    require(time.monotonic() < deadline, "Fixture scan did not finish")
                    time.sleep(0.1)
                require(scanned["stats"]["total"] == 1, "Real container did not scan the mounted source")
                tasks, _ = request("/api/jobs")
                task = tasks["jobs"][0]
                require(task["status"] == "pending", "Account-free scan must not upload")
                body, headers = request(task["preview_url"])
                require(body == photo_bytes and headers.get("Content-Type", "").startswith("image/png"),
                        "Authenticated photo preview did not preserve source bytes")
                body, headers = request(task["download_url"])
                require(body == photo_bytes and headers.get("Content-Disposition", "").startswith("attachment"),
                        "Download must preserve bytes and set attachment disposition")
                body, headers = request(task["preview_url"], expected=206, extra_headers={"Range":"bytes=0-7"})
                require(body == photo_bytes[:8] and headers.get("Content-Range") == f"bytes 0-7/{len(photo_bytes)}",
                        "Byte range response is incorrect")
                body, headers = request(task["preview_url"], method="HEAD")
                require(body == b"" and int(headers["Content-Length"]) == len(photo_bytes), "HEAD must not stream bytes")
                require("снимок.png" in task["local_path"], "Cyrillic source path was lost")
                require(tasks["total"] == 1 and tasks["total_bytes"] == len(photo_bytes),
                        "Filtered task totals must cover all matched tasks")
                require(scanned["storage_plan"]["unlimited"]["count"] == 1,
                        "Scan did not classify the PNG candidate")
                regular_tasks, _ = request("/api/jobs?status=uploaded_regular&sort=desc")
                require(regular_tasks["total"] == 0 and regular_tasks["total_bytes"] == 0,
                        "Empty observed storage filter returned unexpected totals")
                schedules, _ = request("/api/schedules/save", {"name":"Ночной архив", "source_ids":[source_id],
                    "time":"23:45", "mode":"weekly", "weekdays":[1,2,3,4,5,6,7], "month_days":[],
                    "enabled":False}, session["csrf"])
                schedule_id = schedules["schedules"][0]["id"]
                require(schedules["schedules"][0]["source_ids"] == [source_id], "Scheduler source scope was not saved")
                request("/api/settings", {"workers":2,"auto_resume":True,"timezone":"Asia/Yekaterinburg"}, session["csrf"])
                schedules, _ = request("/api/schedules")
                require(schedules["timezone"] == "Asia/Yekaterinburg", "Scheduler timezone was not updated")
                old_csrf = session["csrf"]
                next_password = secrets.token_urlsafe(24)
                session, _ = request("/api/session/password", {"current_password":password, "new_password":next_password}, old_csrf)
                require(session["authenticated"] is True and session["csrf"] != old_csrf, "Password change must rotate the session")
                request("/api/settings", {"workers":2,"auto_resume":True}, old_csrf, expected=403)
                password = next_password
                request("/api/settings", {"workers":2,"auto_resume":True,"regular_policy":"upload","unexpected_unlimited_action":"keep","remote_reconcile":True}, session["csrf"])
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
                    require("preflight_check" not in restored["settings"], "Restart restored obsolete preflight setting")
                    require(restored["settings"]["regular_policy"] == "upload" and
                            restored["settings"]["unexpected_unlimited_action"] == "keep",
                            "Storage policy settings did not survive a container restart")
                    require("upload_large_files" not in restored["settings"],
                            "Restart must not restore the removed ordinary upload setting")
                    require(restored["settings"]["remote_reconcile"] is True, "Comparison setting was not restored")
                    require(restored["stats"]["pending"] == 1, "Scanned queue did not survive restart")
                    schedules, _ = request("/api/schedules")
                    require(schedules["timezone"] == "Asia/Yekaterinburg" and len(schedules["schedules"]) == 1,
                            "Schedule and its timezone did not survive restart")
                    require(schedules["schedules"][0]["enabled"] is False, "Restart enabled a disabled schedule")
                    body, _ = request(task["preview_url"])
                    require(body == photo_bytes, "Restored queue cannot open the original source")
                preview, _ = request("/api/tasks/delete/preview", {"scope":"all", "delete_cloud":False,
                                                                    "delete_local":False}, session["csrf"])
                require(preview["task_count"] == 1 and preview["total_bytes"] == len(photo_bytes),
                        "Deletion preview did not identify the selected task")
                request("/api/tasks/delete", {"token":preview["token"]}, session["csrf"])
                deadline = time.monotonic() + 30
                while True:
                    cleared, _ = request("/api/state")
                    if cleared["activity"]["phase"] != "deleting":
                        break
                    require(time.monotonic() < deadline, "Queue deletion did not finish")
                    time.sleep(0.1)
                require(cleared["stats"]["total"] == 0 and len(cleared["sources"]) == 1,
                        "Queue clear must retain source registrations")
                request("/api/schedules/remove", {"id":schedule_id}, session["csrf"])
                request("/api/sources/remove", {"id":source_id}, session["csrf"])
                require(photo.read_bytes() == photo_bytes, "Source controls changed host originals")
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
