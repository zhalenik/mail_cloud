"""Bounded, authenticated queue downloads; no file list is materialized in RAM."""
from __future__ import annotations

import csv
from datetime import datetime, timezone
import io
import os

from .content import CONTENT_SECURITY


FIELDS = {"path": "Путь", "name": "Имя файла", "size": "Размер, байт", "status": "Статус"}
FILTERS = {"all", "uploaded_unlimited", "uploaded_regular", "failed", "skipped"}
SORT_FIELDS = {"name", "progress", "status", "size", "created_at", "completed_at"}
DELIMITERS = {"comma": ",", "space": " ", "tab": "\t", "semicolon": ";"}


def options(query):
    allowed = {"status", "fields", "format", "delimiter", "header", "sort_by", "sort", "source_id"}
    if set(query) - allowed or any(len(value) != 1 for value in query.values()):
        raise ValueError("Некорректные параметры выгрузки очереди.")
    values = {key: value[0] for key, value in query.items()}
    result = {"status": values.get("status", "all"), "fields": values.get("fields", "path,name,size,status").split(","),
              "format": values.get("format", "csv"), "delimiter": values.get("delimiter", "semicolon"),
              "header": values.get("header", "1"), "sort_by": values.get("sort_by", "created_at"),
              "sort": values.get("sort", "asc")}
    if (result["status"] not in FILTERS or result["format"] not in {"txt", "csv"}
            or result["delimiter"] not in DELIMITERS or result["header"] not in {"0", "1"}
            or result["sort_by"] not in SORT_FIELDS or result["sort"] not in {"asc", "desc"}
            or not 1 <= len(result["fields"]) <= len(FIELDS)
            or len(set(result["fields"])) != len(result["fields"])
            or any(field not in FIELDS for field in result["fields"])):
        raise ValueError("Выберите фильтр, колонки и формат выгрузки очереди.")
    result["header"] = result["header"] == "1"
    result["delimiter"] = DELIMITERS[result["delimiter"]]
    if "source_id" in values:
        value = values["source_id"]
        if not value.isascii() or not value.isdecimal() or not 0 < int(value) < 2**63:
            raise ValueError("Некорректный источник для выгрузки.")
        result["source_id"] = int(value)
    return result


def _status(row):
    mode = row.get("observed_mode")
    storage = {"unlimited": "безлимит", "regular": "квота"}.get(mode, "режим хранения неизвестен")
    if row.get("skip_reason") == "remote_exists":
        suffix = "; другой путь: " + row["cloud_path"] if not row.get("cloud_path_matches", True) else ""
        return "Уже в Облаке" + suffix
    if row["status"] == "uploaded":
        return "Выполнено; " + storage
    if row.get("paused"):
        return "На паузе"
    value = {"pending": "Ожидает", "running": "Загружается", "failed": "Ошибка", "skipped": "Пропущено"}.get(row["status"], row["status"])
    if row.get("cloud_present"):
        value += "; в Облаке; " + storage
    return value


def _safe_cell(value):
    value = str(value)
    # Quoting protects delimiters/newlines; the prefix also prevents spreadsheet
    # applications from treating an arbitrary filename as an executable formula.
    if value.lstrip(" \t\r\n").startswith(("=", "+", "-", "@")):
        return "'" + value
    return value


class QueueExportResponse:
    """A WSGI iterator owning one SQLite read snapshot until close/disconnect."""
    def __init__(self, queue, settings, *, head=False):
        self.queue = queue
        self.settings = settings
        self.rows = None
        self.status = 200
        self.closed = False
        self._first = True
        self._buffer = io.StringIO(newline="")
        self._writer = csv.writer(self._buffer, delimiter=settings["delimiter"], lineterminator="\r\n")
        filename = "mail-cloud-queue-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%SZ") + "." + settings["format"]
        self.headers = [("Content-Type", ("text/csv" if settings["format"] == "csv" else "text/plain") + "; charset=utf-8"),
                        ("Content-Disposition", 'attachment; filename="' + filename + '"'), *CONTENT_SECURITY]
        if head:
            self.close()
        else:
            try:
                queue._db.execute("BEGIN")
                # Establish the snapshot before headers, not at a later chunk.
                queue._db.execute("SELECT id FROM jobs LIMIT 1").fetchone()
                self.rows = queue.iter_export(None if settings["status"] == "all" else settings["status"],
                                              sort_by=settings["sort_by"], sort=settings["sort"], batch_size=500,
                                              root_ids=settings.get("root_ids"))
            except BaseException:
                self.close()
                raise

    def __iter__(self):
        return self

    def __next__(self):
        if self.closed:
            raise StopIteration
        try:
            self._buffer.seek(0)
            self._buffer.truncate()
            prefix = b""
            if self._first:
                self._first = False
                prefix = b"\xef\xbb\xbf" if self.settings["format"] == "csv" else b""
                if self.settings["header"]:
                    self._writer.writerow([FIELDS[field] for field in self.settings["fields"]])
            exhausted = False
            for _ in range(250):
                try:
                    row = next(self.rows)
                except StopIteration:
                    exhausted = True
                    break
                values = {"path": row["local_path"], "name": os.path.basename(row["local_path"]),
                          "size": row["size"], "status": _status(row)}
                self._writer.writerow([_safe_cell(values[field]) for field in self.settings["fields"]])
            data = prefix + self._buffer.getvalue().encode("utf-8")
            if exhausted:
                self.close()
            if not data:
                raise StopIteration
            return data
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.closed:
            return
        self.closed = True
        try:
            if self.rows is not None:
                self.rows.close()
        finally:
            try:
                self.queue._db.rollback()
            finally:
                self.queue.close()

    def __del__(self):
        if not getattr(self, "closed", True):
            try:
                self.close()
            except Exception:
                pass
