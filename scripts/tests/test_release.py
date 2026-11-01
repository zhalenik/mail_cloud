"""Production-release failure cases, with no network or registry mutations."""
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import release

SHA = "a" * 40
OTHER_SHA = "b" * 40
DIGEST = "sha256:" + "1" * 64
NEWER_DIGEST = "sha256:" + "2" * 64


class PolicyTests(unittest.TestCase):
    def test_old_workflow_run_cannot_publish_after_main_advances(self):
        snapshot = release.Snapshot(OTHER_SHA, None, [])
        self.assertIn("no longer main HEAD", release.release_skip("0.4.1", SHA, snapshot))

    def test_published_version_is_a_noop_even_after_code_changes(self):
        snapshot = release.Snapshot(SHA, OTHER_SHA, [{"tag_name": "v0.4.1"}])
        self.assertIn("already released", release.release_skip("0.4.1", SHA, snapshot))

    def test_unreleased_tag_cannot_be_reassigned_to_different_code(self):
        with self.assertRaisesRegex(RuntimeError, "Immutable tag"):
            release.release_skip("0.4.1", SHA, release.Snapshot(SHA, OTHER_SHA, []))

    def test_same_commit_tag_without_release_can_resume(self):
        self.assertIsNone(release.release_skip("0.4.1", SHA, release.Snapshot(SHA, SHA, [])))

    def test_numeric_order_prevents_rollback_across_two_digit_versions(self):
        snapshot = release.Snapshot(SHA, None, [{"tag_name": "v0.4.10"}])
        self.assertIn("at least as new", release.release_skip("0.4.9", SHA, snapshot))
        snapshot = release.Snapshot(SHA, None, [{"tag_name": "v0.4.9"}])
        self.assertIsNone(release.release_skip("0.4.10", SHA, snapshot))

    def test_draft_cannot_silently_be_repurposed(self):
        snapshot = release.Snapshot(SHA, SHA, [{"tag_name": "v0.4.1", "draft": True}])
        with self.assertRaisesRegex(RuntimeError, "draft/prerelease"):
            release.release_skip("0.4.1", SHA, snapshot)

    def test_published_release_with_deleted_tag_is_not_recreated(self):
        snapshot = release.Snapshot(SHA, None, [{"tag_name": "v0.4.1"}])
        with self.assertRaisesRegex(RuntimeError, "no Git tag"):
            release.release_skip("0.4.1", SHA, snapshot)

    def test_newer_stable_is_protected_even_when_its_release_is_missing(self):
        self.assertIn("will not move backward", release.stable_skip("0.4.1", DIGEST, "0.5.0", NEWER_DIGEST))

    def test_same_version_cannot_mean_different_stable_content(self):
        with self.assertRaisesRegex(RuntimeError, "different image"):
            release.stable_skip("0.4.1", DIGEST, "0.4.1", NEWER_DIGEST)

    def test_ambiguous_and_prerelease_versions_are_rejected(self):
        for value in ("01.2.3", "1.02.3", "1.2.03", "1.2", "1.2.3-rc1", "v1.2.3", "1.2.3\n"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                release.semver(value)

    def test_auth_failure_is_never_treated_as_missing_artifact(self):
        with patch.object(release, "urlopen", side_effect=HTTPError("https://ghcr.io/v2/example", 403, "forbidden", {}, None)):
            with self.assertRaises(release.APIError):
                release.api_json("https://ghcr.io/v2/example", missing=True)


class FakeGitHub:
    def __init__(self, events):
        self.events = events
        self.head, self.tag, self.releases = SHA, None, []
        self.fail_release = False

    def snapshot(self, tag):
        return release.Snapshot(self.head, self.tag, self.releases)

    def create_tag(self, tag, sha):
        if self.tag not in (None, sha):
            raise RuntimeError("tag conflict")
        self.tag = sha
        self.events.append("tag")

    def create_release(self, tag, sha, notes):
        self.events.append("release")
        if self.fail_release:
            raise RuntimeError("simulated GitHub release service failure")
        if DIGEST not in notes or sha not in notes:
            raise AssertionError("Release notes must identify the tested digest and source")
        self.releases.append({"tag_name": tag})
        return {"html_url": "https://github.com/example/repo/releases/tag/" + tag}


class FakeRegistry:
    def __init__(self):
        self.tags = {}

    def manifest(self, tag):
        return self.tags.get(tag)


class FakeDocker:
    image = "ghcr.io/example/repo"
    source = "https://github.com/example/repo"

    def __init__(self, events, registry):
        self.events, self.registry = events, registry
        self.labels = {DIGEST: self.make_labels("0.4.1", SHA),
                       NEWER_DIGEST: self.make_labels("0.5.0", OTHER_SHA)}

    def make_labels(self, version, sha):
        return {"org.opencontainers.image.source": self.source,
                "org.opencontainers.image.version": version,
                "org.opencontainers.image.revision": sha}

    def build(self, local, version, sha):
        self.events.append("build")

    def remote_labels(self, digest):
        self.events.append("pull:" + digest)
        return self.labels[digest]

    def verify_labels(self, local, version, sha):
        if "@" in local and self.labels[local.split("@", 1)[1]] != self.make_labels(version, sha):
            raise RuntimeError("different source SHA")

    def inspect(self, local):
        return {"RepoDigests": [self.image + "@" + DIGEST]}

    def push(self, local, tag):
        if "smoke" not in self.events:
            raise AssertionError("An untested image was pushed")
        self.events.append("push:" + tag)
        self.registry.tags[tag] = DIGEST


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.events = []
        self.github, self.registry = FakeGitHub(self.events), FakeRegistry()
        self.docker = FakeDocker(self.events, self.registry)
        self.smoke_patch = patch.object(release, "smoke", side_effect=lambda *_args, **_kwargs: self.events.append("smoke"))
        self.summary_patch = patch.object(release, "summary")
        self.smoke_patch.start()
        self.summary_patch.start()
        self.addCleanup(self.smoke_patch.stop)
        self.addCleanup(self.summary_patch.stop)

    def publish(self):
        release.publish("0.4.1", SHA, self.github, self.registry, self.docker)

    def test_first_release_orders_tests_image_tag_stable_and_release(self):
        self.publish()
        self.assertEqual(self.events, ["build", "smoke", "push:v0.4.1", "tag", "push:stable", "release"])
        self.assertEqual(self.registry.tags, {"v0.4.1": DIGEST, "stable": DIGEST})
        self.assertEqual(self.github.tag, SHA)

    def test_failed_smoke_has_no_remote_side_effects(self):
        with patch.object(release, "smoke", side_effect=RuntimeError("unhealthy image")):
            with self.assertRaisesRegex(RuntimeError, "unhealthy"):
                self.publish()
        self.assertEqual(self.events, ["build"])
        self.assertEqual(self.registry.tags, {})
        self.assertIsNone(self.github.tag)

    def test_partial_release_resumes_exact_image_without_rebuild_or_overwrite(self):
        self.github.fail_release = True
        with self.assertRaisesRegex(RuntimeError, "simulated"):
            self.publish()
        self.events.clear()
        self.github.fail_release = False
        self.publish()
        self.assertNotIn("build", self.events)
        self.assertNotIn("push:v0.4.1", self.events)
        self.assertNotIn("push:stable", self.events)
        self.assertIn("smoke", self.events)
        self.assertEqual(self.events[-1], "release")

    def test_successful_release_rerun_does_nothing(self):
        self.publish()
        self.events.clear()
        self.publish()
        self.assertEqual(self.events, [])

    def test_version_image_from_different_commit_cannot_be_reused(self):
        self.registry.tags["v0.4.1"] = DIGEST
        self.docker.labels[DIGEST] = self.docker.make_labels("0.4.1", OTHER_SHA)
        with self.assertRaisesRegex(RuntimeError, "different source SHA"):
            self.publish()
        self.assertFalse(any(item.startswith("push:") for item in self.events))
        self.assertIsNone(self.github.tag)

    def test_newer_stable_survives_missing_github_release(self):
        self.registry.tags["stable"] = NEWER_DIGEST
        self.publish()
        self.assertEqual(self.registry.tags["stable"], NEWER_DIGEST)
        self.assertNotIn("push:stable", self.events)
        self.assertNotIn("release", self.events)

    def test_main_advancing_during_build_prevents_all_publication(self):
        def build(*_args):
            self.events.append("build")
            self.github.head = OTHER_SHA
        self.docker.build = build
        self.publish()
        self.assertEqual(self.registry.tags, {})
        self.assertIsNone(self.github.tag)

    def test_main_advancing_during_upload_cannot_promote_stable(self):
        original = self.docker.push
        def push(local, tag):
            original(local, tag)
            self.github.head = OTHER_SHA
        self.docker.push = push
        self.publish()
        self.assertEqual(self.registry.tags, {"v0.4.1": DIGEST})
        self.assertIsNone(self.github.tag)
        self.assertNotIn("release", self.events)


if __name__ == "__main__":
    unittest.main()
