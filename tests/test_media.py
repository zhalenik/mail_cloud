"""Routing is conservative; it is never treated as server billing evidence."""
import unittest

from mailru_photos.media import MEDIA_EXTENSIONS, UNLIMITED_MAX_BYTES, classify_media


class MediaPolicyTests(unittest.TestCase):
    def test_documented_formats_accept_unicode_and_case_without_claiming_billing(self):
        for suffix in ("JPG", "JPEG", "PNG", "WEBP", "HEIC", "HEIF", "RAW", "DNG", "TIF", "TIFF",
                       "MP4", "3GP", "MKV", "WEBM", "AVI"):
            with self.subTest(suffix=suffix):
                plan = classify_media("/Фото/Семья/отпуск." + suffix, 512)
                self.assertEqual(plan["mode"], "unlimited")
                self.assertEqual(plan["reason"], "documented_format")
                self.assertIn("сервер", plan["description"])

    def test_mov_is_kept_as_an_account_confirmed_format(self):
        plan = classify_media("video.MOV", 10 * 1024 * 1024)
        self.assertEqual(plan["mode"], "unlimited")
        self.assertEqual(plan["reason"], "account_confirmed_mov")

    def test_small_m4v_and_other_unlisted_formats_remain_discoverable_but_plan_quota(self):
        for suffix in ("m4v", "gif", "3g2", "flv", "hevc", "wmv"):
            with self.subTest(suffix=suffix):
                self.assertIn("." + suffix, MEDIA_EXTENSIONS)
                plan = classify_media("small." + suffix, 5 * 1024 * 1024)
                self.assertEqual(plan["mode"], "regular")
                self.assertEqual(plan["reason"], "outside_documented_formats")
                self.assertIn("политике клиента", plan["description"])

    def test_exact_binary_limit_is_distinct_from_format_selection(self):
        self.assertEqual(classify_media("video.mp4", UNLIMITED_MAX_BYTES)["mode"], "unlimited")
        self.assertEqual(classify_media("video.mp4", UNLIMITED_MAX_BYTES + 1)["reason"], "size_limit")
        self.assertEqual(classify_media("video.m4v", UNLIMITED_MAX_BYTES)["reason"], "outside_documented_formats")
        self.assertEqual(classify_media("video.m4v", UNLIMITED_MAX_BYTES + 1)["reason"], "size_limit")

    def test_non_media_is_not_silently_added_to_default_discovery(self):
        for name in ("archive.zip", "document.pdf", "file", "sound.mp3"):
            self.assertEqual(classify_media(name, 50)["mode"], "regular")
        self.assertNotIn(".zip", MEDIA_EXTENSIONS)
        self.assertNotIn(".pdf", MEDIA_EXTENSIONS)

    def test_invalid_size_is_never_coerced_into_a_routing_decision(self):
        for size in (-1, True, "100", 1.5, None):
            with self.subTest(size=size), self.assertRaises(ValueError):
                classify_media("photo.jpg", size)


if __name__ == "__main__":
    unittest.main()
