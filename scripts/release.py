"""Publish from the tested main commit using only the job's GITHUB_TOKEN.

Version images and Git tags are never replaced. A partially completed release
can resume from its already-pushed image only when its source SHA matches.
Run `version` locally; `publish` is restricted to GitHub Actions on main.
"""
from __future__ import annotations

import argparse
import ast
import base64
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
import tomllib
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from smoke_container import smoke

ROOT = Path(__file__).resolve().parents[1]
VERSION = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\Z")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
SHA = re.compile(r"[0-9a-f]{40}\Z")


def semver(value: str) -> tuple[int, int, int]:
    if not VERSION.fullmatch(value):
        raise ValueError(f"Expected a stable numeric MAJOR.MINOR.PATCH version: {value!r}")
    return tuple(map(int, value.split(".")))


def package_version() -> str:
    with (ROOT / "pyproject.toml").open("rb") as stream:
        version = tomllib.load(stream)["project"]["version"]
    semver(version)
    tree = ast.parse((ROOT / "mailru_photos" / "__init__.py").read_text())
    declared = [ast.literal_eval(node.value) for node in tree.body
                if isinstance(node, ast.Assign)
                and any(isinstance(target, ast.Name) and target.id == "__version__" for target in node.targets)]
    if declared != [version]:
        raise RuntimeError("pyproject.toml and mailru_photos.__version__ must match")
    return version


class APIError(RuntimeError):
    def __init__(self, status: int, url: str):
        self.status = status
        super().__init__(f"HTTP {status} from {url}; refusing to treat an API/authentication failure as a missing image or tag")


def api_json(url, *, headers=None, method="GET", data=None, missing=False):
    body = None if data is None else json.dumps(data).encode()
    supplied = {"Accept": "application/json", "User-Agent": "mail-cloud-release", **(headers or {})}
    if body is not None:
        supplied["Content-Type"] = "application/json"
    try:
        with urlopen(Request(url, data=body, headers=supplied, method=method), timeout=30) as response:
            return json.load(response), response.headers
    except HTTPError as error:
        if error.code == 404 and missing:
            return None, error.headers
        raise APIError(error.code, url) from None


@dataclass(frozen=True)
class Snapshot:
    head: str
    tag_sha: str | None
    releases: list[dict]


def release_skip(version: str, sha: str, snapshot: Snapshot) -> str | None:
    """Return a safe no-op reason, or reject a conflicting immutable tag."""
    wanted = semver(version)
    tag = "v" + version
    for release in snapshot.releases:
        if release["tag_name"] == tag:
            if release.get("draft") or release.get("prerelease"):
                raise RuntimeError(f"{tag} already belongs to a draft/prerelease; choose a new version")
            if snapshot.tag_sha is None:
                raise RuntimeError(f"Published release {tag} has no Git tag; refusing to recreate its identity")
            return f"{tag} is already released from {snapshot.tag_sha}; it is unchanged. Bump the package version for another release."
    if snapshot.tag_sha is not None and snapshot.tag_sha != sha:
        raise RuntimeError(f"Immutable tag {tag} belongs to {snapshot.tag_sha}, not {sha}; bump the version")
    for release in snapshot.releases:
        other = release["tag_name"].removeprefix("v")
        if not release.get("draft") and not release.get("prerelease") and VERSION.fullmatch(other):
            if semver(other) >= wanted:
                return f"Release {release['tag_name']} is at least as new as {tag}; stable is unchanged."
    if snapshot.head != sha:
        return f"Commit {sha} is no longer main HEAD ({snapshot.head}); this superseded run will not publish."
    return None


def stable_skip(version: str, digest: str, current_version: str, current_digest: str) -> str | None:
    current, wanted = semver(current_version), semver(version)
    if current > wanted:
        return f"stable already contains newer version {current_version}; it will not move backward to {version}."
    if current == wanted and current_digest != digest:
        raise RuntimeError(f"stable already contains a different image for version {version}; refusing to replace it")
    return None


class GitHub:
    def __init__(self, repository: str, token: str):
        self.base = "https://api.github.com/repos/" + repository
        self.headers = {"Authorization": "Bearer " + token,
                        "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}

    def call(self, path, **kwargs):
        return api_json(self.base + path, headers=self.headers, **kwargs)[0]

    def tag_sha(self, tag):
        reference = self.call("/git/ref/tags/" + tag, missing=True)
        if reference is None:
            return None
        obj = reference["object"]
        # Support annotated tags created by maintainers, without ever moving one.
        for _ in range(8):
            if obj["type"] == "commit":
                return obj["sha"]
            if obj["type"] != "tag":
                break
            obj = self.call("/git/tags/" + obj["sha"])["object"]
        raise RuntimeError(f"Cannot resolve {tag} to a commit")

    def snapshot(self, tag):
        releases = []
        page = 1
        while True:
            batch = self.call(f"/releases?per_page=100&page={page}")
            releases.extend(batch)
            if len(batch) < 100:
                break
            page += 1
        tag_sha = self.tag_sha(tag)
        head = self.call("/git/ref/heads/main")["object"]["sha"]
        return Snapshot(head, tag_sha, releases)

    def create_tag(self, tag, sha):
        existing = self.tag_sha(tag)
        if existing == sha:
            return
        if existing is not None:
            raise RuntimeError(f"Tag {tag} already belongs to another commit")
        self.call("/git/refs", method="POST", data={"ref": "refs/tags/" + tag, "sha": sha})

    def create_release(self, tag, sha, notes):
        return self.call("/releases", method="POST", data={
            "tag_name": tag, "target_commitish": sha, "name": "Mail Cloud " + tag,
            "body": notes, "draft": False, "prerelease": False, "make_latest": "true"})


class Registry:
    def __init__(self, repository: str, actor: str = "", token: str = ""):
        self.repository = repository.lower()
        self.actor, self.password, self.bearer = actor, token, None
        self.expires = 0.0

    def manifest(self, tag: str) -> str | None:
        if self.bearer is None or time.monotonic() >= self.expires:
            scope = "pull,push" if self.password else "pull"
            query = urlencode({"service": "ghcr.io", "scope": f"repository:{self.repository}:{scope}"})
            headers = {}
            if self.password:
                basic = base64.b64encode(f"{self.actor}:{self.password}".encode()).decode()
                headers["Authorization"] = "Basic " + basic
            result, _ = api_json("https://ghcr.io/token?" + query, headers=headers)
            self.bearer = result["token"]
            self.expires = time.monotonic() + max(1, result.get("expires_in", 60) - 15)
        document, headers = api_json(f"https://ghcr.io/v2/{self.repository}/manifests/{tag}", missing=True,
                                    headers={"Authorization": "Bearer " + self.bearer, "Accept": ", ".join((
                                        "application/vnd.docker.distribution.manifest.v2+json",
                                        "application/vnd.oci.image.manifest.v1+json",
                                        "application/vnd.docker.distribution.manifest.list.v2+json",
                                        "application/vnd.oci.image.index.v1+json"))})
        if document is None:
            return None
        digest = headers.get("Docker-Content-Digest", "")
        if document.get("schemaVersion") != 2 or not DIGEST.fullmatch(digest):
            raise RuntimeError("Registry returned an unsupported manifest or invalid content digest")
        return digest


class Docker:
    def __init__(self, repository: str):
        self.image = "ghcr.io/" + repository.lower()
        self.source = "https://github.com/" + repository

    @staticmethod
    def run(*args, **kwargs):
        return subprocess.run(["docker", *args], check=True, text=True, **kwargs)

    def inspect(self, image):
        return json.loads(self.run("image", "inspect", image, capture_output=True).stdout)[0]

    def remote_labels(self, digest):
        reference = self.image + "@" + digest
        self.run("pull", "--platform", "linux/amd64", reference)
        return self.inspect(reference)["Config"].get("Labels", {})

    def verify_labels(self, image, version, sha):
        labels = self.inspect(image)["Config"].get("Labels", {})
        required = {"org.opencontainers.image.source": self.source,
                    "org.opencontainers.image.version": version, "org.opencontainers.image.revision": sha}
        for key, expected in required.items():
            if labels.get(key) != expected:
                raise RuntimeError(f"{image} has unexpected {key}; refusing to reuse or publish it")

    def build(self, local, version, sha):
        self.run("build", "--pull", "--platform", "linux/amd64",
                 "--build-arg", "VERSION=" + version, "--build-arg", "VCS_REF=" + sha,
                 "--label", "org.opencontainers.image.source=" + self.source,
                 "--label", "org.opencontainers.image.version=" + version,
                 "--label", "org.opencontainers.image.revision=" + sha,
                 "--tag", local, str(ROOT))

    def push(self, local, tag):
        target = self.image + ":" + tag
        self.run("tag", local, target)
        self.run("push", target)


def summary(message: str) -> None:
    print(message, flush=True)
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as stream:
            stream.write(message + "\n\n")


def publish(version, sha, github, registry, docker):
    tag = "v" + version

    def superseded():
        reason = release_skip(version, sha, github.snapshot(tag))
        if reason:
            summary(reason)
        return bool(reason)

    if superseded():
        return
    existing = registry.manifest(tag)
    if existing:
        # Resume after a failed run without rebuilding a released version from a
        # newer base image, or replacing an image made from a different commit.
        docker.remote_labels(existing)
        local = docker.image + "@" + existing
        docker.verify_labels(local, version, sha)
        summary(f"Resuming {tag} from its existing immutable image `{existing}`.")
    else:
        local = "mail-cloud-release:" + sha
        docker.build(local, version, sha)
        docker.verify_labels(local, version, sha)
    smoke(local, version=version)
    if superseded():
        return
    if registry.manifest(tag) != existing:
        raise RuntimeError(f"{tag} changed during validation; refusing to overwrite the version image")
    if not existing:
        docker.push(local, tag)
    digest = registry.manifest(tag)
    if not digest:
        raise RuntimeError("The pushed version image is not available from GHCR")
    if existing is None and docker.image + "@" + digest not in docker.inspect(local).get("RepoDigests", []):
        raise RuntimeError("GHCR version digest does not match the locally tested image")
    # A later push can advance main while Docker is uploading. Leave the already
    # pushed version untouched, and do not let this older run promote stable.
    if superseded():
        summary(f"Version image `{docker.image}:{tag}` exists at `{digest}`; no stable promotion or release was performed.")
        return
    stable_digest = registry.manifest("stable")
    if stable_digest:
        labels = docker.remote_labels(stable_digest)
        if labels.get("org.opencontainers.image.source") != docker.source:
            raise RuntimeError("Existing stable image has an unexpected source repository")
        reason = stable_skip(version, digest, labels.get("org.opencontainers.image.version", ""), stable_digest)
        if reason:
            summary(reason)
            return
    # This is a create-only ref operation, never force-push/update. Creating the
    # tag at main HEAD is supported by GITHUB_TOKEN even when workflows changed.
    if superseded():
        return
    github.create_tag(tag, sha)
    if stable_digest != digest:
        docker.push(local, "stable")
    if registry.manifest("stable") != digest:
        raise RuntimeError("stable does not resolve to the tested version image")
    notes = f"""## Container release

- Version: `{version}`
- Source commit: `{sha}`
- Platform: `linux/amd64` (Intel/AMD TrueNAS SCALE)
- Immutable version image: `{docker.image}:{tag}`
- Stable update channel: `{docker.image}:stable`
- Content digest: `{docker.image}@{digest}`

The installed-package tests and real-container smoke tests passed before publication.
The image was checked with its default non-root user and with mounted state, an
explicit non-root UID/GID, a read-only root filesystem, and a restart.

See the [deployment instructions]({docker.source}/blob/{tag}/README.md).
Changes in this version are listed in the [changelog]({docker.source}/blob/{tag}/CHANGELOG.md).
For an exact, repeatable deployment, use the version tag or content digest above.
GHCR package visibility is configured separately from repository visibility;
the workflow summary reports whether anonymous pulls are available.
"""
    release = github.create_release(tag, sha, notes)
    summary(f"Published [{tag}]({release['html_url']}) from `{sha}`.\n\n"
            f"`{docker.image}:{tag}` and `{docker.image}:stable` both resolve to `{digest}`.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("version", "publish"))
    arguments = parser.parse_args()
    version = package_version()
    if arguments.command == "version":
        print(version)
        return
    if (os.environ.get("GITHUB_ACTIONS") != "true"
            or os.environ.get("GITHUB_REF") != "refs/heads/main"
            or os.environ.get("GITHUB_EVENT_NAME") not in {"push", "workflow_dispatch"}):
        raise RuntimeError("Publication is only allowed in a GitHub Actions main push/manual run")
    repository, sha = os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_SHA"]
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository) or not SHA.fullmatch(sha):
        raise RuntimeError("Invalid repository or commit identity")
    token, actor = os.environ["GH_TOKEN"], os.environ["GITHUB_ACTOR"]
    github, registry, docker = GitHub(repository, token), Registry(repository, actor, token), Docker(repository)
    # Registry credentials live only in this temporary job directory and are
    # delivered on stdin, never on the process command line or in the checkout.
    with tempfile.TemporaryDirectory(prefix="mail-cloud-docker-") as config:
        os.environ["DOCKER_CONFIG"] = config
        docker.run("login", "ghcr.io", "--username", actor, "--password-stdin", input=token + "\n")
        publish(version, sha, github, registry, docker)
    try:
        anonymous = Registry(repository).manifest("v" + version)
        if anonymous:
            summary("Anonymous GHCR pull check passed; no registry login is needed to deploy this package.")
        else:
            summary("No public version image was found for this run.")
    except Exception as error:
        # Visibility is a package setting; neither this token nor the documented
        # Packages REST API provides a visibility-change operation.
        summary("::warning::Anonymous image access is not available or could not be verified. "
                f"Open https://github.com/users/{repository.split('/')[0]}/packages/container/"
                f"{repository.split('/')[1]}/settings and set package visibility to Public "
                "before using an unauthenticated TrueNAS deployment. "
                f"Check result: {type(error).__name__}.")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        print(f"::error::{error}", file=sys.stderr)
        raise SystemExit(1) from None
