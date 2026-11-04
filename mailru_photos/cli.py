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
from .reconcile import reconcile
from .runner import run
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


def _display_data(value):
    """Keep internal observation evidence out of user-facing storage totals."""
    if isinstance(value, dict):
        return {key: _display_data(item) for key, item in value.items()
                if not isinstance(key, str)
                or not key.startswith(("existing_unlimited", "existing_regular", "existing_unknown"))}
    if isinstance(value, (list, tuple)):
        return [_display_data(item) for item in value]
    return value


def pretty(value) -> None:
    print(json.dumps(_display_data(value), ensure_ascii=False, indent=2))


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
    scan = commands.add_parser("scan", help="заново собрать очередь и проверить текущее содержимое Облака")
    scan.add_argument("--extensions", help="расширения через запятую; по умолчанию фото и видео, например jpg,png,heic,mp4,mov")
    scan.add_argument("--local-only", action="store_true", help="только локальный обход: без чтения авторизации и запросов к Облаку")
    commands.add_parser("status", help="показать состояние очереди без сети")
    upload = commands.add_parser("run", help="загрузить очередь; повторный запуск продолжает работу")
    upload.add_argument("--workers", type=int, default=4)
    upload.add_argument("--max-attempts", type=int, default=5)
    upload.add_argument("--regular-policy", choices=("skip", "upload"), default="skip",
                        help="файлы с расходом квоты: пропускать (по умолчанию) или загружать")
    upload.add_argument("--unexpected-unlimited-action", choices=("trash", "keep"), default="trash",
                        help="если сервер не подтвердил ожидаемый безлимит: в корзину (по умолчанию) или оставить")
    upload.add_argument("--job-id", type=int, help="запустить только эту задачу, включая остановленную или ошибочную")
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
        queue.set_metadata("last_auth", {"result": "success", "at": time.time(),
                                         "client_id": memory.value.client_id, "stage": "user_info"})
        pretty({"account": account.email, "unlimited_upload_enabled": account.unlimited_enabled,
                "oauth_client_id": memory.value.client_id,
                "login": "saved_locally", "password_saved": False})
    finally:
        api.close_thread()


def _scan(queue: Queue, directory: Path, args) -> int:
    """Rebuild the current task snapshot; remote history never proves presence."""
    stop = threading.Event()
    value = {"complete": False, "local_complete": False, "reason": "scan_started",
             "root_ids": None, "trigger": "cli", "warning_allowed": False,
             "cloud_checked": False, "cloud_pending": not args.local_only,
             "local_only": args.local_only}
    result_code = 2
    with queue.exclusive_run():
        try:
            queue.recover_running()
            queue.set_metadata("last_scan", value)
            extensions = (queue.discovery_extensions() if args.extensions is None else
                          [item.strip() for item in args.extensions.split(",") if item.strip()])
            extensions = queue.set_discovery_extensions(
                extensions, custom=True if args.extensions is not None else None,
            )
            queue.reset_for_scan()
            # reset_for_scan invalidates its old snapshot too; preserve the
            # CLI-specific intent before a potentially interrupted local walk.
            queue.set_metadata("last_scan", value)
            print("Заново обходим исходные папки. Фото и видео не декодируются и не копируются…", flush=True)
            result = queue.scan(extensions, stop=stop)
            value.update(dataclasses.asdict(result), local_complete=result.complete,
                         complete=False, reason="local_scan_incomplete")
            queue.set_metadata("last_scan", value)
            if result.cancelled or stop.is_set():
                raise Cancelled("Локальное сканирование остановлено.")
            if result.complete:
                if args.local_only or not (directory / "auth.dat").exists():
                    value.update(complete=True, reason="local_only" if args.local_only else "login_required")
                    result_code = 0
                else:
                    value["reason"] = "cloud_scan_started"
                    queue.set_metadata("last_scan", value)
                    queue.set_metadata("last_reconcile", {"at": time.time(), "complete": False,
                                                           "stage": "listing", "reason": "scan_started"})
                    # Loading auth.dat may rotate its refresh token. Keep it
                    # under the same process lock as login/run and the scan.
                    tokens = TokenManager(TokenStore(directory / "auth.dat"))
                    api = CloudAPI(tokens, stop=stop)
                    try:
                        account = api.require_account(queue.account_id)
                        queue.bind_account(account.email)
                        print("Читаем текущий список файлов Облака и сравниваем содержимое…", flush=True)
                        report = reconcile(queue, api, stop=stop)
                        queue.set_metadata("last_reconcile", {"at": time.time(), **report})
                        checked = report.get("complete") is True
                        value.update(complete=checked, cloud_checked=checked, cloud_pending=not checked,
                                     reason="complete" if checked else "cloud_scan_incomplete",
                                     reconcile=report)
                        result_code = 0 if checked else 130 if report.get("cancelled") else 2
                        if result_code:
                            if result_code == 130:
                                value.update(reason="scan_cancelled", cancelled=True)
                                print("Сверка остановлена. Повторите scan для полной проверки Облака.", file=sys.stderr)
                            else:
                                message = (report.get("error_samples") or [{}])[0].get("message")
                                print("Ошибка: содержимое Облака проверено не полностью. Повторите scan. "
                                      + (message or ""), file=sys.stderr)
                    finally:
                        api.close_thread()
            queue.set_metadata("last_scan", value)
        except (Cancelled, KeyboardInterrupt):
            stop.set()
            value.update(complete=False, cloud_checked=False, cloud_pending=not args.local_only,
                         reason="scan_cancelled", cancelled=True)
            queue.set_metadata("last_scan", value)
            print("Сканирование остановлено. Новая очередь сохранена; повторите scan перед загрузкой.", file=sys.stderr)
            result_code = 130
        except Exception:
            value.update(complete=False, cloud_checked=False, cloud_pending=not args.local_only,
                         reason="scan_failed")
            queue.set_metadata("last_scan", value)
            raise
        pretty(value)
        pretty(queue.stats())
    return result_code


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
            return _scan(queue, directory, args)
        if args.command == "status":
            pretty({"destination": queue.destination, "account": queue.account_id,
                    "sources": [dataclasses.asdict(item) for item in queue.roots()],
                    "stats": queue.stats(), "last_scan": queue.get_metadata("last_scan")})
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
                    "last_run": queue.get_metadata("last_run"),
                    "last_auth": queue.get_metadata("last_auth"),
                    "errors": queue.list_jobs("failed", limit=100)}
            write_json(args.output.expanduser().resolve(), _display_data(data))
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
            # and run before loading it, not only during the requests.
            with queue.exclusive_run():
                tokens = TokenManager(TokenStore(directory / "auth.dat"))
                api = CloudAPI(tokens)
                try:
                    account = api.account_info()
                    queue.bind_account(account.email)
                    pretty({"account": account.email, "unlimited_upload_enabled": account.unlimited_enabled,
                            "server_upload_limit": account.upload_limit, "quota": api.quota(),
                            "note": "Диагностика не загружает файлы. Результат хранения проверяется для каждой задачи."})
                    return 0 if account.unlimited_enabled else 2
                finally:
                    api.close_thread()
        if args.command == "run":
            if args.job_id is not None and args.job_id <= 0:
                raise ValueError("--job-id должен быть положительным номером задачи.")
            # Load and rotate credentials under the same process lock as the
            # runner; exclusive_run permits nesting on its owning thread.
            with queue.exclusive_run():
                tokens = TokenManager(TokenStore(directory / "auth.dat"))
                api = CloudAPI(tokens)
                reporter = None
                try:
                    if args.job_id is not None:
                        queue.request_control(args.job_id, "start")
                    reporter = Reporter(directory / "events.jsonl", quiet=args.quiet)
                    result = run(queue, api, workers=args.workers, max_attempts=args.max_attempts,
                                 regular_policy=args.regular_policy,
                                 unexpected_unlimited_action=args.unexpected_unlimited_action,
                                 job_id=args.job_id, emit=reporter.emit)
                    write_json(directory / "last-run.json", result)
                    pretty(result)
                    if args.job_id is not None:
                        job = queue.get_job(args.job_id)
                        completed = job is not None and (job["status"] == "uploaded" or
                                    job["status"] == "skipped" and job.get("skip_reason") == "remote_exists")
                        return 0 if completed and not result["stopped"] else 2
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
    except (QueueError, ProtocolError, TransportError, Cancelled, ValueError) as error:
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
