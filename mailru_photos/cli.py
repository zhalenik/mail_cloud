"""Command-line entry point for Windows and Linux. Python standard library only."""

from __future__ import annotations

import argparse
import dataclasses
import getpass
import json
import os
import sys
import tempfile
import threading
import time
from pathlib import Path

from . import __version__
from .api import CloudAPI, ProtocolError
from .auth import AuthClient, AuthError, TokenManager, TokenStore
from .queue import Queue, QueueError
from .runner import ProbeRequired, probe, run
from .transport import Cancelled, TransportError


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".write-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        if os.name != "nt":
            # A durable rename matters for saved logout/pause intent as well
            # as settings: a power loss must not restore an older intention.
            directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def pretty(value) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2))


class Reporter:
    def __init__(self, path: Path, *, quiet: bool = False):
        self.path = path
        self.quiet = quiet
        self._lock = threading.Lock()
        self._uploaded = 0
        self._stream = path.open("a", encoding="utf-8", buffering=1)

    def close(self) -> None:
        self._stream.close()

    def emit(self, event: str, data: dict) -> None:
        with self._lock:
            self._stream.write(json.dumps({"at": time.time(), "event": event, **data}, ensure_ascii=False) + "\n")
            if self.quiet:
                return
            if event == "uploaded":
                self._uploaded += 1
                return
            if event == "progress":
                print(f"Загружено: {data['uploaded']:,}; в очереди: {data['pending']:,}; "
                      f"ошибки: {data['failed']:,}; пропущено: {data['skipped']:,}", flush=True)
            elif event == "probe_wait":
                print(f"Ожидание обновления квоты: {data['seconds']:g} с…", flush=True)
            elif event == "probe_quota":
                print(f"Изменение used.cloud: {data['delta_bytes']:+,} байт", flush=True)
            elif event == "probe_upload":
                print(f"Проверочная загрузка: {data['path']}", flush=True)
            else:
                print(event + ": " + json.dumps(data, ensure_ascii=False), flush=True)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="python -m mailru_photos",
        description="Загрузчик фото и видео в Облако Mail с сохраняемой очередью.",
    )
    root.add_argument("--version", action="version", version=__version__)
    root.add_argument("--state", type=Path, default=Path("state"), help="локальная папка очереди и авторизации (по умолчанию ./state)")
    root.add_argument("--quiet", action="store_true", help="минимальный вывод; журнал остаётся в state/events.jsonl")
    commands = root.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init", help="создать локальную очередь, без сетевых запросов")
    init.add_argument("--destination", required=True, help="новая папка назначения, например /PhotoArchivePC")
    login = commands.add_parser("login", help="вход через cloud-win с паролем внешнего приложения; пароль вводится скрыто")
    login.add_argument("--email", required=True)
    commands.add_parser("doctor", help="проверить авторизацию, признак безлимита и квоту; файлы не загружает")
    folder = commands.add_parser("add-folder", help="добавить исходную папку рекурсивно")
    folder.add_argument("path", type=Path)
    folder.add_argument("--alias", required=True, help="уникальное имя источника")
    folder.add_argument("--destination", help="точная папка в Облаке для этого источника")
    update_folder = commands.add_parser("update-folder", help="изменить облачную папку источника для незавершённых задач")
    update_folder.add_argument("id", type=int, help="идентификатор источника из status")
    update_folder.add_argument("--destination", required=True, help="новая точная папка в Облаке")
    update_folder.add_argument("--alias", help="новое уникальное имя источника")
    remove_folder = commands.add_parser("remove-folder", help="убрать источник и его задачи, сохранив файлы")
    remove_folder.add_argument("id", type=int, help="идентификатор источника из status")
    scan = commands.add_parser("scan", help="обойти все источники и обновить очередь")
    scan.add_argument("--extensions", help="расширения через запятую; по умолчанию фото и видео, например jpg,png,heic,mp4,mov")
    commands.add_parser("status", help="показать состояние очереди без сети")
    trial = commands.add_parser("probe", help="проверить несколько фото или видео до 2 ГБ и измерить квоту")
    trial.add_argument("--count", type=int, default=3)
    trial.add_argument("--settle-seconds", type=float, default=60)
    upload = commands.add_parser("run", help="загрузить очередь после probe; повторный запуск продолжает работу")
    upload.add_argument("--workers", type=int, default=4)
    upload.add_argument("--max-attempts", type=int, default=5)
    retry = commands.add_parser("retry-failed", help="вернуть ошибки в очередь после устранения причины")
    retry.add_argument("--include-permanent", action="store_true")
    errors = commands.add_parser("errors", help="показать записи с ошибками")
    errors.add_argument("--limit", type=int, default=20)
    report = commands.add_parser("report", help="сохранить диагностический отчёт без паролей и токенов")
    report.add_argument("--output", type=Path, default=Path("mailru_diagnostic.json"))
    commands.add_parser("recover", help="восстановить незавершённые задания после падения; сеть не используется")
    return root


def _configuration(directory: Path) -> dict:
    try:
        value = json.loads((directory / "project.json").read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise QueueError("Сначала выполните init --destination /PhotoArchivePC.") from None
    if not isinstance(value, dict) or value.get("version") != 1 or not isinstance(value.get("destination"), str):
        raise QueueError("Некорректный project.json.")
    return value


def _account_login(queue: Queue, directory: Path, email: str) -> None:
    password = getpass.getpass("Нужен пароль внешнего приложения: «Почта, Облако, Календарь — все протоколы».\n"
                               "Двухфакторную защиту можно оставить включённой.\n"
                               "Пароль приложения Mail.ru (ввод скрыт): ")
    try:
        tokens = AuthClient().login(email.strip(), password)
    finally:
        del password

    class MemoryStore:
        def __init__(self, value):
            self.value = value

        def load(self):
            return self.value

        def save(self, value):
            self.value = value

    memory = MemoryStore(tokens)
    api = CloudAPI(TokenManager(memory))
    try:
        try:
            account = api.account_info()
        except (AuthError, ProtocolError, TransportError) as error:
            raise AuthError("OAuth-токен получен, но доступ к Android API Облака (user/info) не подтверждён. "
                            "Новая авторизация не сохранена; фотографии не загружались.",
                            code="account_info_failed", stage="user_info", client_id=memory.value.client_id,
                            status=getattr(error, "status", None),
                            server_error_code=getattr(error, "server_error_code", None)) from None
        if account.email.strip().casefold() != email.strip().casefold():
            raise AuthError("В user/info вернулся другой аккаунт. Новая авторизация не сохранена.",
                            code="account_mismatch", stage="user_info", client_id=memory.value.client_id)
        queue.bind_account(account.email)
        TokenStore(directory / "auth.dat").save(memory.value)
        previous_probe = queue.get_metadata("probe")
        if isinstance(previous_probe, dict) and previous_probe.get("result") == "provisional_pass":
            queue.set_metadata("probe", {**previous_probe, "result": "invalidated", "reason": "new_login"})
        queue.set_metadata("last_auth", {"result": "success", "at": time.time(),
                                         "client_id": memory.value.client_id, "stage": "user_info"})
        pretty({"account": account.email, "unlimited_upload_enabled": account.unlimited_enabled,
                "oauth_client_id": memory.value.client_id,
                "login": "saved_locally", "password_saved": False})
    finally:
        api.close_thread()


def dispatch(args) -> int:
    directory = args.state.expanduser().resolve()
    if args.command == "init":
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        configuration = {"version": 1, "destination": args.destination}
        if (directory / "project.json").exists():
            previous = _configuration(directory)
            if previous["destination"] != args.destination:
                raise QueueError("Эта очередь уже связана с другой папкой. Для новой папки используйте другой --state.")
            configuration = previous
        with Queue(directory / "queue.sqlite3", destination=args.destination) as queue:
            configuration["destination"] = queue.destination
        write_json(directory / "project.json", configuration)
        print(f"Очередь создана. Папка назначения: {configuration['destination']}")
        return 0
    configuration = _configuration(directory)
    with Queue(directory / "queue.sqlite3", destination=configuration["destination"]) as queue:
        if args.command == "add-folder":
            root = queue.add_root(args.path, args.alias, remote_prefix=args.destination)
            pretty(dataclasses.asdict(root))
            return 0
        if args.command == "update-folder":
            root = queue.update_root(args.id, remote_prefix=args.destination, alias=args.alias)
            pretty(dataclasses.asdict(root))
            return 0
        if args.command == "remove-folder":
            pretty({"removed_tasks": queue.remove_root(args.id)})
            return 0
        if args.command == "scan":
            with queue.exclusive_run():
                queue.set_metadata("last_scan", {"complete": False, "reason": "scan_started"})
                if args.extensions is None:
                    extensions = queue.discovery_extensions()
                else:
                    extensions = [item.strip() for item in args.extensions.split(",") if item.strip()]
                extensions = queue.set_discovery_extensions(
                    extensions, custom=True if args.extensions is not None else None,
                )
                print("Обход исходных папок. Фото и видео не декодируются и не копируются…", flush=True)
                result = queue.scan(extensions)
                value = dataclasses.asdict(result)
                queue.set_metadata("last_scan", value)
            pretty(value)
            pretty(queue.stats())
            return 0 if result.complete else 2
        if args.command == "status":
            pretty({"destination": queue.destination, "account": queue.account_id,
                    "sources": [dataclasses.asdict(item) for item in queue.roots()],
                    "stats": queue.stats(), "last_scan": queue.get_metadata("last_scan"),
                    "probe": queue.get_metadata("probe", {}).get("result")})
            return 0
        if args.command == "errors":
            pretty(queue.list_jobs("failed", limit=args.limit))
            return 0
        if args.command == "recover":
            pretty({"recovered": queue.recover_running()})
            return 0
        if args.command == "retry-failed":
            pretty({"queued_again": queue.retry_failed(include_permanent=args.include_permanent)})
            return 0
        if args.command == "report":
            data = {"client_version": __version__, "destination": queue.destination,
                    "stats": queue.stats(), "last_scan": queue.get_metadata("last_scan"),
                    "probe": queue.get_metadata("probe"), "last_run": queue.get_metadata("last_run"),
                    "last_auth": queue.get_metadata("last_auth"),
                    "errors": queue.list_jobs("failed", limit=100)}
            write_json(args.output.expanduser().resolve(), data)
            print(f"Отчёт записан: {args.output}")
            return 0
        if args.command == "login":
            with queue.exclusive_run():
                try:
                    _account_login(queue, directory, args.email)
                except AuthError as error:
                    queue.set_metadata("last_auth", {"result": "failed", "at": time.time(), **error.safe_diagnostics()})
                    raise
            return 0
        if args.command == "doctor":
            # TokenManager loads auth.dat immediately and may rotate its refresh
            # token on either request.  Hold the same process lock as login,
            # probe and run before loading it, not only during the requests.
            with queue.exclusive_run():
                tokens = TokenManager(TokenStore(directory / "auth.dat"))
                api = CloudAPI(tokens)
                try:
                    account = api.account_info()
                    queue.bind_account(account.email)
                    pretty({"account": account.email, "unlimited_upload_enabled": account.unlimited_enabled,
                            "server_upload_limit": account.upload_limit, "quota": api.quota(),
                            "warning": "Живая загрузка проверяется отдельной командой probe."})
                    return 0 if account.unlimited_enabled else 2
                finally:
                    api.close_thread()
        tokens = TokenManager(TokenStore(directory / "auth.dat"))
        api = CloudAPI(tokens)
        reporter = None
        try:
            reporter = Reporter(directory / "events.jsonl", quiet=args.quiet)
            if args.command == "probe":
                result = probe(queue, api, count=args.count, settle_seconds=args.settle_seconds, emit=reporter.emit)
                write_json(directory / "probe-report.json", result)
                pretty(result)
                return 0 if result["result"] == "provisional_pass" else 2
            if args.command == "run":
                result = run(queue, api, workers=args.workers, max_attempts=args.max_attempts, emit=reporter.emit)
                write_json(directory / "last-run.json", result)
                pretty(result)
                stats = result["stats"]
                return 0 if not result["stopped"] and not (stats["pending"] or stats["running"] or stats["failed"] or stats["skipped"]) else 2
        finally:
            api.close_thread()
            if reporter is not None:
                reporter.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        return dispatch(args)
    except AuthError as error:
        print("Ошибка: " + str(error), file=sys.stderr)
        print("Диагностика входа: " + json.dumps(error.safe_diagnostics(), ensure_ascii=False), file=sys.stderr)
        return 2
    except (QueueError, ProtocolError, ProbeRequired, TransportError, Cancelled, ValueError) as error:
        print("Ошибка: " + str(error), file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("Остановлено. Очередь сохранена; используйте recover перед повторным scan или run для продолжения.", file=sys.stderr)
        return 130
    except OSError as error:
        print(f"Ошибка доступа к локальному файлу ({type(error).__name__}). Проверьте пути и права.", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
