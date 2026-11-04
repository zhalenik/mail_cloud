import unittest

from mailru_photos.metadata import (
    MAX_U64,
    MetadataProtocolError,
    encode_add_file,
    encode_mkdir,
    encode_quota,
    encode_varint,
    parse_add_file_response,
    parse_mkdir_response,
    parse_quota_response,
    read_varint,
)


class MetadataWireFormatTests(unittest.TestCase):
    def test_independent_varint_wire_vectors(self):
        vectors = [
            (0, b"\x00"),
            (23, b"\x17"),
            (106, b"\x6a"),
            (127, b"\x7f"),
            (128, b"\x80\x01"),
            (300, b"\xac\x02"),
            (16384, b"\x80\x80\x01"),
            (MAX_U64, b"\xff" * 9 + b"\x01"),
        ]
        for value, wire in vectors:
            with self.subTest(value=value):
                self.assertEqual(encode_varint(value), wire)
                self.assertEqual(read_varint(b"xx" + wire + b"tail", 2), (value, 2 + len(wire)))

    def test_varint_rejects_truncation_and_overflow(self):
        for wire in (b"", b"\x80", b"\x80" * 10, b"\xff" * 9 + b"\x02"):
            with self.subTest(wire=wire):
                with self.assertRaises(MetadataProtocolError):
                    read_varint(wire)
        for value in (-1, MAX_U64 + 1):
            with self.assertRaises(ValueError):
                encode_varint(value)
        for value in (True, 1.5, "1"):
            with self.assertRaises(TypeError):
                encode_varint(value)

    def test_offset_validation_and_noncanonical_wire_compatibility(self):
        for offset in (-1, 2):
            with self.assertRaises(ValueError):
                read_varint(b"\x00", offset)
        with self.assertRaises(TypeError):
            read_varint(b"\x00", True)
        self.assertEqual(read_varint(b"\x80\x00"), (0, 2))

    def test_mkdir_ascii_fixture_has_revision_terminator_and_flags(self):
        # DataEncoder: opcode 106, revision 0, 8 path bytes plus NUL, flags 0.
        self.assertEqual(encode_mkdir("/Photos1"), b"\x6a\x00\x09/Photos1\x00\x00")

    def test_mkdir_length_counts_utf8_bytes_after_nfc_not_characters(self):
        # '/Фото/é': 1 + 8 + 1 + 2 UTF-8 bytes, then one NUL byte.
        expected = b"\x6a\x00\x0d/\xd0\xa4\xd0\xbe\xd1\x82\xd0\xbe/\xc3\xa9\x00\x00"
        self.assertEqual(encode_mkdir("/Фото/e\u0301"), expected)
        self.assertEqual(encode_mkdir("/Фото/é"), expected)

    def test_mkdir_length_crosses_varint_boundary(self):
        # Path has 128 bytes; length includes NUL -> 129 -> 81 01.
        self.assertEqual(encode_mkdir("/" + "a" * 127)[:4], b"\x6a\x00\x81\x01")

    def test_mkdir_invalid_path_and_raw_status(self):
        for path in ("", "relative", "/ok\x00bad"):
            with self.assertRaises(ValueError):
                encode_mkdir(path)
        for status in (0, 1, 4, 9, 11, 17, 18, 254, 255):
            self.assertEqual(parse_mkdir_response(bytes([status]) + b"revision"), status)
        with self.assertRaises(MetadataProtocolError):
            parse_mkdir_response(b"")

    def test_regular_add_independent_wire_fixture_has_create_only_flags(self):
        # APK AddBlobRequest: 103, MPR_NONE, UTF-8 path+NUL, size, epoch
        # seconds, zero, raw hash, flags3. Neither flags1 (replace) nor55
        # (rename/autoupload) may be substituted by this operation.
        digest = bytes(range(20))
        expected = b"\x67\x00\x07/a.mp4\x00\xac\x02\x80\x80\x01\x00" + digest + b"\x03"
        self.assertEqual(encode_add_file("/a.mp4", 300, 16384, digest.hex()), expected)

    def test_regular_add_uses_nfc_bytes_and_64bit_file_size(self):
        digest = "AB" * 20
        wire = encode_add_file("/e\u0301.mov", 1 << 40, 0, digest)
        self.assertTrue(wire.startswith(b"\x67\x00\x08/\xc3\xa9.mov\x00"))
        self.assertEqual(wire[11:], b"\x80\x80\x80\x80\x80\x20\x00\x00" + b"\xab" * 20 + b"\x03")

    def test_regular_add_rejects_invalid_paths_hashes_and_unsigned_fields(self):
        for path in ("", "/", "relative", "/a//b", "/a/../b", "/a/./b", "/ok\x00bad"):
            with self.subTest(path=path), self.assertRaises(ValueError):
                encode_add_file(path, 1, 1, "00" * 20)
        for digest in (None, "", "00" * 19, "00" * 21, "XY" * 20, "00 " * 20):
            with self.subTest(digest=digest), self.assertRaises(ValueError):
                encode_add_file("/ok", 1, 1, digest)
        for size, timestamp in ((-1, 0), (0, -1), (MAX_U64 + 1, 0), (0, MAX_U64 + 1)):
            with self.subTest(size=size, timestamp=timestamp), self.assertRaises(ValueError):
                encode_add_file("/ok", size, timestamp, "00" * 20)

    def test_regular_add_success_reads_status_without_assuming_revision_shape(self):
        first, second = bytes(range(12)), bytes(range(12, 24))
        revisions = (
            b"",
            b"\x00",
            b"\x01" + first + b"\xac\x02",
            b"\x02" + first + b"\x01",
            b"\x03" + first + b"\x00" + second + b"\xff\x01",
            b"\x04" + first + b"\x01" + second + b"\x00",
            b"\x05" + first + b"\xac\x02" + second,
            b"\x06future-server-revision-format",
            b"\x00extension",
        )
        for revision in revisions:
            with self.subTest(revision=revision):
                self.assertEqual(parse_add_file_response(b"\x00" + revision), 0)
        with self.assertRaises(MetadataProtocolError):
            parse_add_file_response(b"")

    def test_regular_add_nonzero_status_is_one_raw_byte_not_varint(self):
        for status in (1, 4, 5, 7, 9, 12, 253, 254, 255):
            with self.subTest(status=status):
                self.assertEqual(parse_add_file_response(bytes([status])), status)

    def test_quota_wire_order_and_account_over_quota(self):
        self.assertEqual(encode_quota(), b"\x17")
        self.assertEqual(parse_quota_response(b"\x00\xac\x02\x80\x80\x01"), (300, 16384))
        self.assertEqual(parse_quota_response(b"\x00\xac\x02\x7f"), (300, 127))
        self.assertEqual(parse_quota_response(b"\x00\x00\x00extension"), (0, 0))

    def test_quota_rejects_failure_and_truncated_fields(self):
        for wire in (b"", b"\x00", b"\x00\x01", b"\x00\x01\x80"):
            with self.subTest(wire=wire):
                with self.assertRaises(MetadataProtocolError):
                    parse_quota_response(wire)
        with self.assertRaises(MetadataProtocolError) as caught:
            parse_quota_response(b"\x0b\x00\x00")
        self.assertEqual(caught.exception.status, 11)


if __name__ == "__main__":
    unittest.main()
