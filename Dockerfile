FROM python:3.12-slim-trixie AS sqlite-build

# Debian's SQLite package can lag the WAL-reset fix. Build a verified upstream
# release separately, so the runtime contains neither a compiler nor sources.
RUN apt-get update \
    && apt-get install --yes --no-install-recommends gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*
COPY scripts/build_sqlite.py /build/build_sqlite.py
RUN python /build/build_sqlite.py /build/libsqlite3.so.0

FROM python:3.12-slim-trixie

ARG VERSION=0.6.0
ARG VCS_REF=unknown
LABEL org.opencontainers.image.title="Mail Cloud Photo Client" \
      org.opencontainers.image.description="Resumable photo and video import with a browser interface" \
      org.opencontainers.image.source="https://github.com/zhalenik/mail_cloud" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY --from=sqlite-build /build/libsqlite3.so.0 /usr/local/lib/libsqlite3.so.0
# Check the library Python actually loaded, not the version in dpkg metadata.
RUN ldconfig \
    && python -c "import sqlite3; assert sqlite3.sqlite_version == '3.54.1', sqlite3.sqlite_version; assert sqlite3.threadsafety == 3"

COPY requirements-web.txt /app/requirements-web.txt
RUN pip install --no-cache-dir -r /app/requirements-web.txt \
    && groupadd --gid 1000 mailru \
    && useradd --uid 1000 --gid 1000 --home-dir /app --no-create-home \
       --shell /usr/sbin/nologin mailru \
    && mkdir /state /photos \
    && chown 1000:1000 /state

COPY mailru_photos /app/mailru_photos

USER 1000:1000
EXPOSE 8787
STOPSIGNAL SIGTERM

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD ["python", "-c", "import http.client; c=http.client.HTTPConnection('127.0.0.1',8787,timeout=3); c.request('GET','/healthz'); r=c.getresponse(); raise SystemExit(0 if r.status==200 else 1)"]

CMD ["python", "-m", "mailru_photos.web", "--host", "0.0.0.0", "--port", "8787", "--state", "/state", "--allow-root", "/photos"]
