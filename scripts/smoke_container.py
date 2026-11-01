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
                request("/api/state")
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
                    request("/api/state")
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
