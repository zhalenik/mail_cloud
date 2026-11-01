import unittest

from mailru_photos.metadata import (
    MAX_U64,
    MetadataProtocolError,
    encode_mkdir,
    encode_quota,
    encode_varint,
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
