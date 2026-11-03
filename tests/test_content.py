"""Local content is served only through pinned, verified regular files."""
from __future__ import annotations

import errno
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from mailru_photos.api import FileIdentity
from mailru_photos.content import CHUNK_SIZE, ContentError, open_task_content


@unittest.skipUnless(os.name == 'posix' and hasattr(os, 'O_NOFOLLOW'), 'Secure file serving requires openat')
class ContentTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.base = Path(self.directory.name)
        self.root = self.base / 'photos'
        self.root.mkdir()

    def tearDown(self):
        self.directory.cleanup()

    def source(self, name='photo.jpg', data=b'\xff\xd8\xff' + b'x' * 100):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return path, FileIdentity.from_stat(path.stat())

    def assert_closed(self, descriptor):
        with self.assertRaises(OSError) as caught:
            os.fstat(descriptor)
        self.assertEqual(caught.exception.errno, errno.EBADF)

    def test_original_bytes_are_streamed_in_bounded_chunks_with_safe_headers(self):
        data = b'\xff\xd8\xff' + bytes(range(256)) * 1024
        path, identity = self.source('семья "2025".jpg', data)
        response = open_task_content(self.root, path, identity)
        descriptor = response._fd
        headers = dict(response.headers)
        self.assertEqual(response.status, 200)
        self.assertEqual(headers['Content-Type'], 'image/jpeg')
        self.assertTrue(headers['Content-Disposition'].startswith('inline;'))
        self.assertIn("filename*=UTF-8''", headers['Content-Disposition'])
        self.assertIn('%D1%81', headers['Content-Disposition'])
        self.assertEqual(headers['X-Content-Type-Options'], 'nosniff')
        self.assertEqual(headers['Cache-Control'], 'no-store')
        self.assertIn("default-src 'none'", headers['Content-Security-Policy'])
        pieces = list(response)
        self.assertGreater(len(pieces), 1)
        self.assertTrue(all(0 < len(piece) <= CHUNK_SIZE for piece in pieces))
        self.assertEqual(b''.join(pieces), data)
        self.assert_closed(descriptor)
        response.close()

    def test_single_byte_ranges_and_unknown_units(self):
        path, identity = self.source('file.bin', b'0123456789')
        for value, expected, content_range in [
            ('bytes=2-5', b'2345', 'bytes 2-5/10'),
            ('bytes=7-', b'789', 'bytes 7-9/10'),
            ('bytes=-3', b'789', 'bytes 7-9/10'),
            ('bytes=8-9999', b'89', 'bytes 8-9/10'),
            ('bytes=-1000', b'0123456789', 'bytes 0-9/10'),
        ]:
            with self.subTest(value=value):
                response = open_task_content(self.root, path, identity, range_header=value)
                self.assertEqual(response.status, 206)
                self.assertEqual(dict(response.headers)['Content-Range'], content_range)
                self.assertEqual(dict(response.headers)['Content-Length'], str(len(expected)))
                self.assertEqual(b''.join(response), expected)
        response = open_task_content(self.root, path, identity, range_header='unknown=2-5')
        self.assertEqual(response.status, 200)
        self.assertEqual(b''.join(response), b'0123456789')

    def test_invalid_ranges_close_descriptor_and_include_total_size(self):
        from mailru_photos import content
        path, identity = self.source('file.bin', b'0123456789')
        opened = []
        original = content._open_source
        def record(*args):
            descriptor = original(*args)
            opened.append(descriptor)
            return descriptor
        for value in ('bytes=20-', 'bytes=8-2', 'bytes=-0', 'bytes=0-1,3-4', 'bytes=', 'bytes=1--2', 'bytes=' + '9' * 257):
            with self.subTest(value=value), patch.object(content, '_open_source', side_effect=record):
                with self.assertRaises(ContentError) as caught:
                    open_task_content(self.root, path, identity, range_header=value)
                self.assertEqual(caught.exception.status, 416)
                self.assertEqual(dict(caught.exception.headers)['Content-Range'], 'bytes */10')
                self.assert_closed(opened[-1])

    def test_head_ignores_range_and_closes_before_iteration(self):
        from mailru_photos import content
        path, identity = self.source('file.bin', b'0123456789')
        opened = []
        original = content._open_source
        def record(*args):
            descriptor = original(*args)
            opened.append(descriptor)
            return descriptor
        with patch.object(content, '_open_source', side_effect=record):
            response = open_task_content(self.root, path, identity, head=True, range_header='bytes=2-4')
        self.assertEqual(response.status, 200)
        self.assertEqual(dict(response.headers)['Content-Length'], '10')
        self.assertNotIn('Content-Range', dict(response.headers))
        self.assert_closed(opened[0])
        self.assertEqual(list(response), [])

    def test_inline_requires_allowed_extension_and_matching_image_signature(self):
        cases = [
            ('a.jpg', b'\xff\xd8\xffabc', 'image/jpeg'),
            ('a.png', b'\x89PNG\r\n\x1a\nabc', 'image/png'),
            ('a.gif', b'GIF89aabc', 'image/gif'),
            ('a.webp', b'RIFF0000WEBPabc', 'image/webp'),
            ('a.svg', b'<svg onload="alert(1)"/>', 'application/octet-stream'),
            ('a.html', b'<script>alert(1)</script>', 'application/octet-stream'),
            ('fake.jpg', b'<html>not an image</html>', 'application/octet-stream'),
            ('a.mp4', b'video bytes', 'application/octet-stream'),
        ]
        for name, data, content_type in cases:
            with self.subTest(name=name):
                path, identity = self.source(name, data)
                response = open_task_content(self.root, path, identity)
                headers = dict(response.headers)
                self.assertEqual(headers['Content-Type'], content_type)
                self.assertTrue(headers['Content-Disposition'].startswith(
                    'inline;' if content_type.startswith('image/') else 'attachment;'))
                self.assertEqual(b''.join(response), data)
        path, identity = self.source('force.jpg')
        response = open_task_content(self.root, path, identity, download=True)
        self.assertTrue(dict(response.headers)['Content-Disposition'].startswith('attachment;'))
        response.close()

    def test_symlinks_at_file_directory_and_root_are_rejected(self):
        outside = self.base / 'outside'
        outside.mkdir()
        private = outside / 'private.jpg'
        private.write_bytes(b'outside-private-bytes')
        identity = FileIdentity.from_stat(private.stat())
        (self.root / 'file.jpg').symlink_to(private)
        (self.root / 'directory').symlink_to(outside, target_is_directory=True)
        root_link = self.base / 'root-link'
        root_link.symlink_to(outside, target_is_directory=True)
        for root, path in ((self.root, self.root / 'file.jpg'),
                           (self.root, self.root / 'directory' / 'private.jpg'),
                           (root_link, root_link / 'private.jpg')):
            with self.subTest(path=path), self.assertRaises(ContentError) as caught:
                open_task_content(root, path, identity)
            self.assertEqual(caught.exception.status, 403)

    def test_opened_parent_remains_pinned_when_replaced_with_symlink(self):
        path, identity = self.source('nested/photo.jpg', b'\xff\xd8\xfforiginal')
        outside = self.base / 'outside'
        outside.mkdir()
        (outside / 'photo.jpg').write_bytes(b'\xff\xd8\xffoutside-private')
        real_open = os.open
        replaced = False
        def racing_open(name, flags, *args, **kwargs):
            nonlocal replaced
            descriptor = real_open(name, flags, *args, **kwargs)
            if name == 'nested' and flags & os.O_DIRECTORY and not replaced:
                replaced = True
                (self.root / 'nested').rename(self.root / 'original-parent')
                (self.root / 'nested').symlink_to(outside, target_is_directory=True)
            return descriptor
        with patch('mailru_photos.content.os.open', side_effect=racing_open):
            response = open_task_content(self.root, path, identity)
        self.assertTrue(replaced)
        self.assertEqual(b''.join(response), b'\xff\xd8\xfforiginal')

    def test_missing_changed_outside_and_special_sources_never_stream(self):
        path, identity = self.source()
        replacement = self.root / 'replacement'
        replacement.write_bytes(b'different source')
        replacement.replace(path)
        with self.assertRaises(ContentError) as caught:
            open_task_content(self.root, path, identity)
        self.assertEqual(caught.exception.status, 409)
        path.unlink()
        with self.assertRaises(ContentError) as caught:
            open_task_content(self.root, path, identity)
        self.assertEqual(caught.exception.status, 404)
        with self.assertRaises(ContentError) as caught:
            open_task_content(self.root, self.base / 'outside.jpg', identity)
        self.assertEqual(caught.exception.status, 403)
        fifo = self.root / 'fifo.jpg'
        os.mkfifo(fifo)
        with self.assertRaises(ContentError) as caught:
            open_task_content(self.root, fifo, FileIdentity.from_stat(fifo.stat()))
        self.assertEqual(caught.exception.status, 403)

    def test_client_disconnect_and_midstream_change_close_the_descriptor(self):
        path, identity = self.source(data=b'\xff\xd8\xff' + b'x' * (3 * CHUNK_SIZE))
        response = open_task_content(self.root, path, identity)
        descriptor = response._fd
        self.assertEqual(len(next(response)), CHUNK_SIZE)
        response.close()
        self.assert_closed(descriptor)
        self.assertEqual(list(response), [])
        response = open_task_content(self.root, path, identity)
        descriptor = response._fd
        next(response)
        os.utime(path, ns=(identity.mtime_ns + 1, identity.mtime_ns + 1))
        with self.assertRaises(OSError):
            next(response)
        self.assert_closed(descriptor)


if __name__ == '__main__':
    unittest.main()
