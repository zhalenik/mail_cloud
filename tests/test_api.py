"""Offline behavior checks for the upload transaction, never real account traffic."""

import io
import json
import os
import tempfile
import struct
import threading
import unittest
import uuid
from pathlib import Path
from dataclasses import replace
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit, unquote

from mailru_photos.api import (
    API_BASE, UPLOAD_BASE, MAX_MEDIA_BYTES, AccountInfo, CloudAPI, FileIdentity, ProtocolError,
    RemoteConflict, RemoteError, SourceChanged, UnlimitedNotConfirmed, UnsupportedPhoto, cloud_hash, _retry_after,
    is_auth_failure,
)
from mailru_photos.metadata import read_varint
from mailru_photos.auth import Tokens
from mailru_photos.transport import Cancelled, HTTP, Response, SourceReadError, TransportError, validate_url


# Generated 16x16, one-frame MPEG-4/MP4. ffprobe validated this fixture;
# appending a top-level free box below changes size without changing the video.
# It tests wire routing, not the live service's eligibility decision.
_MP4_ONE_FRAME = bytes.fromhex(
    "0000001c6674797069736f6d0000020069736f6d69736f326d703431000003416d6f6f760000006c6d766864000000000000000000000000"
    "000003e8000003e8000100000100000000000000000000000001000000000000000000000000000000010000000000000000000000000000"
    "40000000000000000000000000000000000000000000000000000000000000020000026b7472616b0000005c746b68640000000300000000"
    "000000000000000100000000000003e800000000000000000000000000000000000100000000000000000000000000000001000000000000"
    "000000000000000040000000001000000010000000000024656474730000001c656c73740000000000000001000003e80000000000010000"
    "000001e36d646961000000206d646864000000000000000000000000000040000000400055c400000000002d68646c720000000000000000"
    "76696465000000000000000000000000566964656f48616e646c6572000000018e6d696e6600000014766d68640000000100000000000000"
    "000000002464696e660000001c6472656600000000000000010000000c75726c20000000010000014e7374626c000000ea73747364000000"
    "0000000001000000da6d70347600000000000000010000000000000000000000000000000000100010004800000048000000000000000113"
    "4c61766336302e33312e313032206d706567340000000000000000000000000018ffff000000606573647300000000038080804f00010004"
    "80808041201100000000030d4000000088058080802f000001b001000001b58913000001000000012000c48d88000d0084021463000001b2"
    "4c61766336302e33312e3130320680808001020000001070617370000000010000000100000014627472740000000000030d400000008800"
    "00001873747473000000000000000100000001000040000000001c7374736300000000000000010000000100000001000000010000001473"
    "74737a000000000000001100000001000000147374636f00000000000000010000036d00000062756474610000005a6d6574610000000000"
    "00002168646c7200000000000000006d6469726170706c0000000000000000000000002d696c737400000025a9746f6f0000001d64617461"
    "00000001000000004c61766636302e31362e3130300000000866726565000000196d646174000001b3001007000001b616051823db7e"
)


def response(status=200, data=None, headers=None):
    raw = data if isinstance(data, bytes) else json.dumps(data if data is not None else {}).encode()
    return Response(status, headers or {}, raw)


class TokensStub:
    def __init__(self):
        self.tokens = Tokens("OLD-TOKEN", "REFRESH-TOKEN", "owner@example.ru")
        self.refresh_calls = 0

    def access_token(self, *, force_refresh=False, rejected_token=None):
        if force_refresh:
            self.refresh_calls += 1
            self.tokens = Tokens("NEW-TOKEN", "NEW-REFRESH", self.tokens.username)
        return self.tokens.access_token


class FakeHTTP:
    def __init__(self, sequence):
        self.sequence = list(sequence)
        self.requests = []

    def request(self, method, url, **kwargs):
        validate_url(url)
        record = {"method": method, "url": url, "headers": kwargs.get("headers", {}),
                  "body": kwargs.get("body", b""), "stream_body": None}
        stream = kwargs.get("stream")
        if stream is not None:
            record["stream_body"] = stream.read(kwargs["length"])
            callback = kwargs.get("progress")
            if callback:
                callback(len(record["stream_body"]))
        self.requests.append(record)
        if not self.sequence:
            raise AssertionError("Unexpected network operation")
        result = self.sequence.pop(0)
        if callable(result):
            result = result(record)
        if isinstance(result, Exception):
            raise result
        return result

    def close(self):
        pass


class UploadBehaviorTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.local = Path(self.folder.name) / "снимок #1.jpg"
        self.data = b"\xff\xd8\xff" + bytes(range(256)) * 16 + b"\xff\xd9"
        self.local.write_bytes(self.data)
        self.identity = FileIdentity.from_stat(self.local.stat())
        self.digest, _ = cloud_hash(self.local)
        self.remote = "/Import/снимок #1.jpg"
        self.saved = []
        self.confirmed = []

    def tearDown(self):
        self.folder.cleanup()

    def test_observation_distinguishes_false_from_missing_before_policy_error(self):
        for extra, mode in (({'is_unlim_upload': False}, 'regular'), ({}, 'unknown'),
                            ({'is_unlim_upload': 'false'}, 'unknown')):
            with self.subTest(mode=mode, extra=extra):
                observations = []
                api, fake, _ = self.api([
                    response(404), response(201, headers={'x-valid-for': '0'}),
                    response(data={'name': self.local.name, 'path': self.remote, 'nodeid': '17', **extra}),
                ])
                with self.assertRaises(UnlimitedNotConfirmed) as caught:
                    api.upload_photo(str(self.local), self.remote, expected=self.identity,
                                     save_intent=self.saved.append, save_confirmation=self.confirmed.append,
                                     save_observation=lambda value: observations.append(dict(value)))
                self.assertEqual(len(observations), 1)
                observed = observations[0]
                self.assertEqual(observed['mode'], mode)
                self.assertEqual(observed['hash'], self.digest)
                self.assertEqual(observed['size'], self.identity.size)
                self.assertEqual(observed['nodeid'], '17')
                self.assertIs(observed['verified'], False)
                self.assertIs(observed['commit_accepted'], True)
                self.assertEqual(caught.exception.observation, observed)
                if mode == 'regular':
                    self.assertIs(observed['is_unlim_upload'], False)
                else:
                    self.assertNotIn('is_unlim_upload', observed)
                self.assertFalse(fake.sequence)
                self.assertEqual(self.confirmed, [])

    def test_observation_is_durable_before_final_stat_and_becomes_verified_after_match(self):
        observations = []
        def verify(_):
            self.assertEqual(len(observations), 1)
            self.assertIs(observations[0]['verified'], False)
            self.assertEqual(self.confirmed, [self.digest])
            return self.stat_response()
        api, _, _ = self.api([
            response(404), response(201, headers={'x-valid-for': '0'}),
            response(data={'name': self.local.name, 'path': self.remote, 'is_unlim_upload': True}), verify,
        ])
        result = api.upload_photo(str(self.local), self.remote, expected=self.identity,
                                  save_intent=self.saved.append, save_confirmation=self.confirmed.append,
                                  save_observation=lambda value: observations.append(dict(value)))
        self.assertEqual([v['verified'] for v in observations], [False, True])
        self.assertEqual(result, observations[1])

    def test_failed_verification_retains_unverified_observation(self):
        observations = []
        api, _, _ = self.api([
            response(404), response(201, headers={'x-valid-for': '0'}),
            response(data={'name': self.local.name, 'is_unlim_upload': True}),
            self.stat_response(digest='0' * 40),
        ])
        with self.assertRaises(ProtocolError):
            api.upload_photo(str(self.local), self.remote, expected=self.identity,
                             save_intent=self.saved.append, save_confirmation=self.confirmed.append,
                             save_observation=lambda value: observations.append(dict(value)))
        self.assertEqual(len(observations), 1)
        self.assertEqual(observations[0]['mode'], 'unlimited')
        self.assertIs(observations[0]['verified'], False)

    def test_wrong_commit_identity_is_not_saved_as_a_known_cloud_object(self):
        for changed in ({'name': 'someone-else.jpg'}, {'path': '/Other/file.jpg'}):
            observations = []
            api, _, _ = self.api([
                response(404), response(201, headers={'x-valid-for': '0'}),
                response(data={'name': self.local.name, 'path': self.remote, 'is_unlim_upload': False, **changed}),
            ])
            with self.assertRaises(ProtocolError):
                api.upload_photo(str(self.local), self.remote, expected=self.identity,
                                 save_intent=self.saved.append, save_observation=observations.append)
            self.assertEqual(observations, [])

    def test_recovery_keeps_saved_false_even_with_contradictory_unlimited_confirmation(self):
        saved = {'mode': 'regular', 'hash': self.digest, 'size': self.identity.size,
                 'path': self.remote, 'is_unlim_upload': False}
        prior = {'cloud_hash': self.digest, 'remote_path': self.remote, 'storage_mode': 'unlimited',
                 'unlimited_confirmed': True, 'storage_observation': saved}
        api, fake, _ = self.api([self.stat_response()])
        observations = []
        with self.assertRaises(UnlimitedNotConfirmed) as caught:
            api.upload_photo(str(self.local), self.remote, expected=self.identity, prior_intent=prior,
                             save_intent=self.saved.append, save_observation=observations.append)
        self.assertEqual(caught.exception.observation['mode'], 'regular')
        self.assertIs(caught.exception.observation['is_unlim_upload'], False)
        self.assertIs(caught.exception.observation['verified'], True)
        self.assertEqual(len(fake.requests), 1)

    def test_regular_observation_precedes_verification_for_small_m4v(self):
        self.video_fixture('.m4v')
        observations = []
        def verify(_):
            self.assertEqual(len(observations), 1)
            self.assertEqual(observations[0]['mode'], 'regular')
            self.assertEqual(observations[0]['regular_evidence'], 'metadata_commit_response')
            self.assertIs(observations[0]['verified'], False)
            return self.stat_response()
        api, fake, _ = self.regular_api([
            response(404), self.quota_response(), response(201, headers={'x-valid-for': '0'}),
            response(data=b'\x00\x00'), verify,
        ])
        result = self.regular_upload(api, save_observation=lambda value: observations.append(dict(value)))
        self.assertLess(self.identity.size, MAX_MEDIA_BYTES)
        self.assertEqual(result['storage_mode'], 'regular')
        self.assertEqual([v['verified'] for v in observations], [False, True])
        self.assertFalse(any('/private/add' in r['url'] for r in fake.requests))

    def test_stat_normalizes_nfd_and_encodes_unicode_as_uri_bytes(self):
        api, fake, _ = self.api([self.stat_response()])
        api.stat('/Семья/е\u0308ж & #?%.jpg')
        target = urlsplit(fake.requests[0]['url']).path
        self.assertTrue(target.isascii())
        self.assertEqual(unquote(target), '/api/v1/private/stat/Семья/ёж & #?%.jpg')

    def trash_response(self, **changes):
        return response(data={'status': 200, 'body': {'kind': 'file', 'home': self.remote,
                                                     'hash': self.digest, 'size': self.identity.size, **changes}})

    def test_trash_uses_file_remove_only_after_exact_identity_and_checks_404(self):
        api, fake, _ = self.api([self.trash_response(), response(data={'status': 200}), response(404)])
        result = api.trash_file(self.remote, expected_hash=self.digest, expected_size=self.identity.size)
        self.assertTrue(result['trashed'])
        self.assertTrue(result['absent'])
        self.assertEqual([r['method'] for r in fake.requests], ['GET', 'POST', 'GET'])
        post = fake.requests[1]
        self.assertEqual(urlsplit(post['url']).path, '/api/m1/file/remove')
        self.assertEqual(parse_qs(post['body'].decode('ascii')), {'home': [self.remote]})
        self.assertEqual(parse_qs(urlsplit(post['url']).query), {'access_token': ['OLD-TOKEN']})
        self.assertNotIn('Authorization', post['headers'])
        self.assertFalse(any('trashbin/empty' in r['url'] for r in fake.requests))

    def test_trash_refuses_folder_unknown_kind_and_changed_identity(self):
        for changes in ({'kind': 'folder'}, {'kind': 'camera-upload'}, {'kind': None},
                        {'hash': '0' * 40}, {'size': self.identity.size + 1}, {'home': '/Other/file.jpg'}):
            with self.subTest(changes=changes):
                api, fake, _ = self.api([self.trash_response(**changes)])
                with self.assertRaises(ProtocolError):
                    api.trash_file(self.remote, expected_hash=self.digest, expected_size=self.identity.size)
                self.assertEqual([r['method'] for r in fake.requests], ['GET'])

    def test_trash_optional_node_id_is_strict_and_cannot_delete_a_replacement(self):
        for mobile in (self.stat_response(), response(data={'hash': self.digest, 'size': self.identity.size,
                                                           'nodeid': 'different'})):
            api, fake, _ = self.api([mobile])
            with self.assertRaises(ProtocolError):
                api.trash_file(self.remote, expected_hash=self.digest, expected_size=self.identity.size,
                               expected_node_id='saved-node')
            self.assertEqual([r['method'] for r in fake.requests], ['GET'])
        api, fake, _ = self.api([response(404), self.trash_response()])
        with self.assertRaises(ProtocolError):
            api.trash_file(self.remote, expected_hash=self.digest, expected_size=self.identity.size,
                           expected_node_id='saved-node')
        self.assertFalse(any(r['method'] == 'POST' for r in fake.requests))

    def test_trash_404_is_idempotent_but_405_is_not_missing(self):
        api, fake, _ = self.api([response(404)])
        result = api.trash_file(self.remote, expected_hash=self.digest, expected_size=self.identity.size)
        self.assertTrue(result['already_missing'])
        self.assertFalse(result['trashed'])
        self.assertEqual([r['method'] for r in fake.requests], ['GET'])
        api, fake, _ = self.api([response(405)])
        with self.assertRaises(RemoteError):
            api.trash_file(self.remote, expected_hash=self.digest, expected_size=self.identity.size)
        self.assertFalse(any(r['method'] == 'POST' for r in fake.requests))

    def test_trash_does_not_retry_an_ambiguous_mutation(self):
        api, fake, _ = self.api([self.trash_response(), TransportError('Lost trash response')])
        with self.assertRaises(TransportError):
            api.trash_file(self.remote, expected_hash=self.digest, expected_size=self.identity.size)
        self.assertEqual([r['method'] for r in fake.requests], ['GET', 'POST'])

    def test_trash_success_requires_disappearance_and_server_success_status(self):
        api, fake, _ = self.api([self.trash_response(), response(data={'status': 400})])
        with self.assertRaises(RemoteError):
            api.trash_file(self.remote, expected_hash=self.digest, expected_size=self.identity.size)
        api, fake, _ = self.api([self.trash_response(), response(data={'status': 200})]
                                 + [self.trash_response()] * 4)
        with patch.object(api, '_wait'), self.assertRaises(ProtocolError):
            api.trash_file(self.remote, expected_hash=self.digest, expected_size=self.identity.size)
        self.assertEqual(sum(r['method'] == 'POST' for r in fake.requests), 1)

    def api(self, sequence, *, parent_known=True):
        fake = FakeHTTP(sequence)
        tokens = TokensStub()
        api = CloudAPI(tokens, transport_factory=lambda: fake)
        if parent_known:
            api._directories.add("/Import")
        return api, fake, tokens

    def stat_response(self, digest=None, size=None):
        return response(data={"hash": self.digest if digest is None else digest,
                              "size": self.identity.size if size is None else size})

    def upload(self, api, *, prior_intent=None, is_retried=False):
        return api.upload_photo(str(self.local), self.remote, expected=self.identity,
                                prior_intent=prior_intent, save_intent=self.saved.append,
                                save_confirmation=self.confirmed.append,
                                is_retried=is_retried)

    def video_fixture(self, suffix='.mp4'):
        self.local = Path(self.folder.name) / ('видео #1' + suffix)
        # Representative container bytes for an offline wire-level test, not a
        # claim that a decoder or the live Mail service accepted this fixture.
        self.data = b'\x00\x00\x00\x18ftypisom\x00\x00\x02\x00isommp42' + bytes(range(256)) * 4
        self.local.write_bytes(self.data)
        self.identity = FileIdentity.from_stat(self.local.stat())
        self.digest, _ = cloud_hash(self.local)
        self.remote = '/Import/' + self.local.name
        self.saved.clear()
        self.confirmed.clear()


    def test_video_upload_preserves_original_bytes_and_strict_confirmation(self):
        for suffix in ('.MP4', '.MOV', '.m4v', '.mkv', '.webm', '.avi', '.3gp'):
            with self.subTest(suffix=suffix):
                self.video_fixture(suffix)
                api, fake, _ = self.api([
                    response(404), response(404, headers={'x-upload-limit': str(MAX_MEDIA_BYTES)}),
                    response(201, headers={'x-valid-for': '0'}),
                    response(data={'name': self.local.name, 'path': self.remote, 'is_unlim_upload': True}),
                    self.stat_response(),
                ])
                result = self.upload(api)
                self.assertIs(result['is_unlim_upload'], True)
                self.assertIs(result['stat_matched'], True)
                self.assertEqual(self.saved, [self.digest])
                self.assertEqual(self.confirmed, [self.digest])
                body = next(item for item in fake.requests if item['stream_body'] is not None)
                self.assertEqual(body['stream_body'], self.data)
                commit = next(item for item in fake.requests if item['method'] == 'POST')
                payload = json.loads(commit['body'])
                self.assertIs(payload['unlimited_upload'], True)
                self.assertEqual(payload['upload_type'], 'AUTO')
                self.assertEqual(payload['path'], self.remote)
                self.assertEqual(payload['size'], len(self.data))
                self.assertEqual(payload['hash'], self.digest)
                self.assertFalse(fake.sequence)


    def test_video_resume_sends_only_remaining_original_bytes(self):
        self.video_fixture('.mov')
        offset = 256
        api, fake, _ = self.api([
            response(404),
            response(200, headers={'x-valid-for': '0', 'x-received': str(offset),
                                  'x-location': UPLOAD_BASE + '/resumable/' + self.digest}),
            response(201, headers={'x-valid-for': '0'}),
            response(data={'name': self.local.name, 'path': self.remote, 'is_unlim_upload': True}),
            self.stat_response(),
        ])
        self.assertIs(self.upload(api, is_retried=True)['stat_matched'], True)
        body = next(item for item in fake.requests if item['stream_body'] is not None)
        self.assertEqual(body['stream_body'], self.data[offset:])
        self.assertEqual(body['headers']['Content-Range'], f'bytes {offset}-{len(self.data)-1}/{len(self.data)}')


    def test_video_without_unlimited_confirmation_does_not_fall_back(self):
        self.video_fixture()
        api, fake, _ = self.api([
            response(404), response(201, headers={'x-valid-for': '0'}),
            response(data={'name': self.local.name, 'path': self.remote, 'is_unlim_upload': False}),
        ])
        with self.assertRaises(UnlimitedNotConfirmed):
            self.upload(api)
        self.assertEqual(self.confirmed, [])
        commits = [item for item in fake.requests if item['method'] == 'POST']
        self.assertEqual(len(commits), 1)
        self.assertIs(json.loads(commits[0]['body'])['unlimited_upload'], True)


    def test_oversize_video_is_rejected_before_hashing_or_network(self):
        self.video_fixture()
        self.identity = replace(self.identity, size=MAX_MEDIA_BYTES + 1)
        api, fake, _ = self.api([])
        with patch('mailru_photos.api.cloud_hash', side_effect=AssertionError('Must not hash oversized video')):
            with self.assertRaisesRegex(UnsupportedPhoto, '2 ГБ'):
                self.upload(api)
        self.assertEqual(fake.requests, [])
        self.assertEqual(self.saved, [])


    def test_five_and_ten_mib_video_use_the_same_auto_wire_flow_as_photo(self):
        def transfer():
            api, fake, _ = self.api([
                response(404), response(404, headers={'x-upload-limit': str(MAX_MEDIA_BYTES)}),
                response(201, headers={'x-valid-for': '0'}),
                response(data={'name': self.local.name, 'path': self.remote, 'is_unlim_upload': True}),
                self.stat_response(),
            ])
            result = self.upload(api)
            commit = next(request for request in fake.requests if request['method'] == 'POST')
            body = next(request for request in fake.requests if request['stream_body'] is not None)
            self.assertEqual(body['stream_body'], self.data)
            for request in fake.requests:
                if request['method'] == 'PUT':
                    self.assertEqual(parse_qs(urlsplit(request['url']).query)['upload_type'], ['AUTO'])
            self.assertEqual(commit['url'], API_BASE + '/api/v1/private/add')
            payload = json.loads(commit['body'])
            self.assertIs(result['is_unlim_upload'], True)
            return {key: value for key, value in payload.items()
                    if key not in {'hash', 'last_modified', 'path', 'size'}}

        photo_parameters = transfer()
        for suffix, size in (('.m4v', 5 * 1024 * 1024), ('.mp4', 10 * 1024 * 1024)):
            with self.subTest(suffix=suffix, size=size):
                self.local = Path(self.folder.name) / ('small-video' + suffix)
                padding = size - len(_MP4_ONE_FRAME)
                self.data = _MP4_ONE_FRAME + struct.pack('>I', padding) + b'free' + b'\0' * (padding - 8)
                self.local.write_bytes(self.data)
                self.identity = FileIdentity.from_stat(self.local.stat())
                self.digest, _ = cloud_hash(self.local)
                self.remote = '/Import/' + self.local.name
                self.saved.clear()
                self.confirmed.clear()
                self.assertEqual(transfer(), photo_parameters)
                self.assertEqual(self.confirmed, [self.digest])

    def regular_api(self, sequence, *, parent_known=True):
        api, fake, tokens = self.api(sequence, parent_known=parent_known)
        api._metadata_url = 'https://cloclo1.datacloudmail.ru'
        api._metadata_until = float('inf')
        return api, fake, tokens

    def regular_upload(self, api, **kwargs):
        kwargs.setdefault('account', AccountInfo('owner@example.ru', True, 10_000_000))
        kwargs.setdefault('expected', self.identity)
        kwargs.setdefault('save_intent', self.saved.append)
        kwargs.setdefault('save_confirmation', self.confirmed.append)
        return api.upload_regular(str(self.local), self.remote, **kwargs)

    @staticmethod
    def quota_response(*, total=100_000_000, cloud=0, mail=0):
        return response(data={'quota': total, 'used': {'cloud': cloud, 'mail': mail, 'total': cloud + mail}})

    def test_regular_native_commit_is_create_only_and_saves_proof_before_stat(self):
        self.video_fixture()
        progress = []

        def commit(request):
            self.assertEqual(self.saved, [self.digest])
            self.assertEqual(self.confirmed, [])
            self.assertEqual(request['headers']['Content-Type'], 'application/octet-stream')
            self.assertEqual(urlsplit(request['url']).hostname, 'cloclo1.datacloudmail.ru')
            query = parse_qs(urlsplit(request['url']).query)
            self.assertIn('token', query)
            self.assertIn('client_id', query)
            raw = request['body']
            self.assertEqual(raw[:2], b'\x67\x00')
            length, offset = read_varint(raw, 2)
            self.assertEqual(raw[offset:offset + length], self.remote.encode('utf-8') + b'\x00')
            size, offset = read_varint(raw, offset + length)
            timestamp, offset = read_varint(raw, offset)
            self.assertEqual(size, len(self.data))
            self.assertEqual(timestamp, self.identity.mtime_ns // 1_000_000_000)
            self.assertEqual(raw[offset:], b'\x00' + bytes.fromhex(self.digest) + b'\x03')
            return response(data=b'\x00\x01' + bytes(range(12)) + b'\xac\x02')

        def verification(_):
            self.assertEqual(self.confirmed, [self.digest])
            return self.stat_response()

        api, fake, _ = self.regular_api([
            response(404), self.quota_response(),
            response(404, headers={'x-upload-limit': '10000000', 'x-max-upload-limit': '10000000'}),
            response(201, headers={'x-valid-for': '0'}), commit, verification,
        ])
        result = self.regular_upload(api, progress=progress.append)
        self.assertEqual(result['storage_mode'], 'regular')
        self.assertEqual(result['regular_evidence'], 'metadata_commit_response')
        self.assertEqual(result['quota_status'], 'uses_cloud_quota')
        self.assertTrue(result['commit_accepted'])
        self.assertTrue(result['stat_matched'])
        self.assertFalse(result['recovered_commit'])
        self.assertNotIn('is_unlim_upload', result)
        self.assertNotIn('unlimited_evidence', result)
        self.assertEqual(progress, [len(self.data)])
        self.assertEqual(next(r['stream_body'] for r in fake.requests if r['stream_body'] is not None), self.data)
        for request in fake.requests:
            self.assertNotIn('/private/add', request['url'])
            if request['method'] == 'PUT':
                self.assertEqual(parse_qs(urlsplit(request['url']).query)['upload_type'], ['MANUAL'])
        self.assertFalse(fake.sequence)

    def test_regular_resume_sends_only_remaining_original_bytes(self):
        self.video_fixture('.mov')
        offset = 400
        target = UPLOAD_BASE + '/resumable/' + self.digest
        api, fake, _ = self.regular_api([
            response(404), self.quota_response(),
            response(200, headers={'x-valid-for': '0', 'x-received': str(offset), 'x-location': target,
                                  'x-upload-limit': '10000000', 'x-max-upload-limit': '10000000'}),
            response(201, headers={'x-valid-for': '0'}), response(data=b'\x00\x00'), self.stat_response(),
        ])
        self.assertTrue(self.regular_upload(api, is_retried=True)['stat_matched'])
        body = next(r for r in fake.requests if r['stream_body'] is not None)
        self.assertEqual(body['stream_body'], self.data[offset:])
        self.assertEqual(body['headers']['Content-Range'], f'bytes {offset}-{len(self.data) - 1}/{len(self.data)}')
        query = parse_qs(urlsplit(body['url']).query)
        self.assertEqual(query['upload_type'], ['MANUAL'])
        self.assertEqual(query['is_retried'], ['true'])

    def test_regular_account_limit_is_explicit_and_checked_before_hash_or_body(self):
        for limit in (None, 0, True, self.identity.size - 1):
            with self.subTest(limit=limit):
                api, fake, _ = self.regular_api([response(404)])
                info = AccountInfo('owner@example.ru', True, limit)
                with patch('mailru_photos.api.cloud_hash', side_effect=AssertionError('Must reject before hashing')):
                    with self.assertRaises(ProtocolError):
                        self.regular_upload(api, account=info)
                self.assertEqual([r['method'] for r in fake.requests], ['GET'])
                self.assertEqual(self.saved, [])

    def test_regular_reads_account_limit_when_caller_does_not_supply_it(self):
        api, fake, _ = self.regular_api([
            response(404), response(data={'email': 'owner@example.ru', 'unlimited_upload_enabled': True,
                                         'upload_limit': 10_000_000}), self.quota_response(),
            response(201, headers={'x-valid-for': '0'}), response(data=b'\x00\x00'), self.stat_response(),
        ])
        self.assertTrue(self.regular_upload(api, account=None)['stat_matched'])
        self.assertTrue(fake.requests[1]['url'].endswith('/api/v1/user/info'))

    def test_account_limit_uses_smallest_reported_cap_without_a_universal_default(self):
        for values, expected in (({}, None), ({'upload_limit': 12345}, 12345),
                                 ({'max_file_size': '56789'}, 56789),
                                 ({'upload_limit': 12345, 'max_file_size': 10000}, 10000)):
            with self.subTest(values=values):
                api, _, _ = self.api([response(data={
                    'email': 'owner@example.ru', 'unlimited_upload_enabled': True, **values,
                })])
                self.assertEqual(api.account_info().upload_limit, expected)

    def test_regular_free_space_includes_shared_mail_usage_before_hash_or_body(self):
        api, fake, _ = self.regular_api([response(404), self.quota_response(total=10000, cloud=0, mail=9999)])
        with patch('mailru_photos.api.cloud_hash', side_effect=AssertionError('Must reject before hashing')):
            with self.assertRaises(RemoteError) as caught:
                self.regular_upload(api)
        self.assertEqual(caught.exception.code, 'CONDITION/OVERQUOTA')
        self.assertTrue(caught.exception.fatal)
        self.assertEqual([r['method'] for r in fake.requests], ['GET', 'GET'])

    def test_regular_current_server_caps_apply_before_new_resumed_or_cached_blob_commit(self):
        for status in (200, 201, 404):
            for maximum, limit, error in (
                (self.identity.size - 1, 10000000, UnsupportedPhoto),
                (self.identity.size - 1, self.identity.size - 1, UnsupportedPhoto),
                (10000000, self.identity.size - 1, RemoteError),
                (None, self.identity.size - 1, RemoteError),
            ):
                with self.subTest(status=status, maximum=maximum, limit=limit):
                    headers = {'x-valid-for': '0', 'x-received': '10',
                               'x-location': UPLOAD_BASE + '/resumable/' + self.digest,
                               'x-upload-limit': str(limit)}
                    if maximum is not None:
                        headers['x-max-upload-limit'] = str(maximum)
                    api, fake, _ = self.regular_api([response(404), self.quota_response(), response(status, headers=headers)])
                    with self.assertRaises(error):
                        self.regular_upload(api)
                    self.assertFalse(any(r['stream_body'] is not None or r['method'] == 'POST' for r in fake.requests))
                    self.assertEqual(self.saved, [])

    def test_regular_no_missing_or_malformed_native_ack_becomes_success(self):
        for raw in (b'', b'\x00', b'\x00\x06', b'\x00\x01short', b'\x00\x00extra',
                    b'\x01', b'\x04', b'\x05', b'\x09', b'\x0c', b'\xfe'):
            with self.subTest(raw=raw):
                self.saved.clear()
                self.confirmed.clear()
                api, fake, _ = self.regular_api([
                    response(404), self.quota_response(), response(201, headers={'x-valid-for': '0'}), response(data=raw),
                ])
                with self.assertRaises(ProtocolError):
                    self.regular_upload(api)
                self.assertEqual(self.saved, [self.digest])
                self.assertEqual(self.confirmed, [])
                self.assertEqual(fake.requests[-1]['method'], 'POST')

    def test_regular_native_no_space_is_fatal_and_bad_blob_is_retryable(self):
        for status, code, retryable, fatal in ((7, 'CONDITION/OVERQUOTA', False, True),
                                             (253, 'CONDITION/BADBLOB', True, False)):
            with self.subTest(status=status):
                api, _, _ = self.regular_api([
                    response(404), self.quota_response(), response(201, headers={'x-valid-for': '0'}),
                    response(data=bytes([status])),
                ])
                with self.assertRaises(RemoteError) as caught:
                    self.regular_upload(api)
                self.assertEqual(caught.exception.code, code)
                self.assertIs(caught.exception.retryable, retryable)
                self.assertIs(caught.exception.fatal, fatal)
                self.assertEqual(self.confirmed, [])

    def test_regular_lost_commit_response_cannot_adopt_matching_existing_file(self):
        api, _, _ = self.regular_api([
            response(404), self.quota_response(), response(201, headers={'x-valid-for': '0'}),
            TransportError('Simulated lost ordinary commit response'),
        ])
        with self.assertRaises(TransportError):
            self.regular_upload(api)
        self.assertEqual(self.saved, [self.digest])
        self.assertEqual(self.confirmed, [])
        intent = {'cloud_hash': self.digest, 'remote_path': self.remote, 'storage_mode': 'regular'}
        second, fake, _ = self.regular_api([self.stat_response()])
        with self.assertRaises(RemoteConflict):
            self.regular_upload(second, prior_intent=intent)
        self.assertEqual([r['method'] for r in fake.requests], ['GET'])

    def test_regular_saved_confirmation_recovers_without_requiring_space_again(self):
        def fail_stat(_):
            self.assertEqual(self.confirmed, [self.digest])
            return TransportError('Simulated disconnect after positive ordinary commit')

        api, _, _ = self.regular_api([
            response(404), self.quota_response(), response(201, headers={'x-valid-for': '0'}),
            response(data=b'\x00\x00'), fail_stat, fail_stat, fail_stat,
        ])
        with patch.object(api, '_wait'), self.assertRaises(TransportError):
            self.regular_upload(api)
        intent = {'cloud_hash': self.digest, 'remote_path': self.remote,
                  'storage_mode': 'regular', 'regular_confirmed': True}
        second, fake, _ = self.regular_api([self.stat_response()])
        result = self.regular_upload(second, prior_intent=intent, account=None)
        self.assertEqual(result['regular_evidence'], 'saved_commit_response')
        self.assertEqual(result['storage_mode'], 'regular')
        self.assertTrue(result['recovered_commit'])
        self.assertFalse(result['commit_accepted'])
        self.assertNotIn('is_unlim_upload', result)
        self.assertEqual([r['method'] for r in fake.requests], ['GET'])

    def test_regular_existing_path_needs_its_own_positive_mode_specific_proof(self):
        base = {'cloud_hash': self.digest, 'remote_path': self.remote,
                'regular_confirmed': True, 'storage_mode': 'regular'}
        for intent in (None, {**base, 'storage_mode': 'unlimited'},
                       {**base, 'storage_mode': None}, {**base, 'regular_confirmed': 1},
                       {**base, 'regular_confirmed': False}, {**base, 'cloud_hash': '0' * 40},
                       {'cloud_hash': self.digest, 'remote_path': self.remote, 'unlimited_confirmed': True}):
            with self.subTest(intent=intent):
                api, fake, _ = self.regular_api([self.stat_response()])
                with self.assertRaises(RemoteConflict):
                    self.regular_upload(api, prior_intent=intent)
                self.assertEqual([r['method'] for r in fake.requests], ['GET'])
                self.assertEqual(self.saved, [])

    def test_regular_wrong_final_hash_does_not_finish_but_keeps_positive_commit_proof(self):
        api, _, _ = self.regular_api([
            response(404), self.quota_response(), response(201, headers={'x-valid-for': '0'}),
            response(data=b'\x00\x00'), self.stat_response(digest='0' * 40),
        ])
        with self.assertRaises(ProtocolError):
            self.regular_upload(api)
        self.assertEqual(self.confirmed, [self.digest])

    def test_regular_changed_source_after_body_is_not_committed(self):
        def change_source(_):
            with self.local.open('ab') as stream:
                stream.write(b'changed')
            return response(201, headers={'x-valid-for': '0'})

        api, fake, _ = self.regular_api([
            response(404), self.quota_response(), response(404, headers={'x-upload-limit': '10000000'}), change_source,
        ])
        with self.assertRaises(SourceChanged):
            self.regular_upload(api)
        self.assertEqual(self.saved, [])
        self.assertFalse(any(r['method'] == 'POST' for r in fake.requests))

    def test_binary_two_gib_limit_keeps_eligible_files_in_unlimited_mode(self):
        self.assertEqual(MAX_MEDIA_BYTES, 2 ** 31)
        for size in (2_000_000_001, MAX_MEDIA_BYTES):
            with self.subTest(size=size):
                expected = replace(self.identity, size=size)
                intent = {'cloud_hash': self.digest, 'remote_path': self.remote, 'unlimited_confirmed': True}
                api, _, _ = self.api([self.stat_response(size=size)])
                with patch('mailru_photos.api.cloud_hash', return_value=(self.digest, expected)):
                    result = api.upload_photo(str(self.local), self.remote, expected=expected,
                                              prior_intent=intent, save_intent=self.saved.append)
                self.assertEqual(result['storage_mode'], 'unlimited')
                self.assertTrue(result['recovered_commit'])


    def test_unlimited_recovery_rejects_regular_mode_even_with_unlimited_flag(self):
        intent = {'cloud_hash': self.digest, 'remote_path': self.remote, 'storage_mode': 'regular',
                  'unlimited_confirmed': True, 'regular_confirmed': True}
        api, fake, _ = self.api([self.stat_response()])
        with self.assertRaises(RemoteConflict):
            self.upload(api, prior_intent=intent)
        self.assertEqual([r['method'] for r in fake.requests], ['GET'])


    def test_quota_uses_cloud_component_from_canonical_host(self):
        api, fake, _ = self.api([response(data={"quota": 1000,
                                               "used": {"cloud": 123, "mail": 400, "total": 523}})])
        self.assertEqual(api.quota(), {"used_bytes": 123, "total_bytes": 1000,
                                      "mail_bytes": 400, "combined_used_bytes": 523})
        request = fake.requests[0]
        self.assertEqual(request["method"], "GET")
        self.assertEqual(request["url"], "https://cloud.mail.ru/api/v4/user/quota")
        self.assertEqual(request["headers"]["Authorization"], "Bearer OLD-TOKEN")

    def test_quota_requires_explicit_cloud_component(self):
        api, fake, _ = self.api([response(data={"quota": 1000, "used": {"mail": 400, "total": 523}})])
        with self.assertRaises(ProtocolError):
            api.quota()
        self.assertEqual(len(fake.requests), 1)

    def test_new_upload_creates_parent_then_commits_verified_original(self):
        def accepted_commit(request):
            self.assertEqual(self.saved, [self.digest], "Durable intent must precede finalization")
            payload = json.loads(request["body"])
            self.assertTrue(payload["unlimited_upload"])
            self.assertEqual(payload["hash"], self.digest)
            self.assertEqual(payload["size"], len(self.data))
            self.assertEqual(payload["path"], self.remote)
            self.assertEqual(payload["platform"], "cloud-android")
            self.assertEqual(payload["upload_type"], "AUTO")
            self.assertFalse(payload["overwrite"])
            self.assertFalse(payload["options"]["overwrite"])
            return response(data={"name": self.local.name, "path": self.remote, "nodeid": "123", "is_unlim_upload": True})

        api, fake, _ = self.api([
            response(404),
            response(data=b"https://cloclo1.datacloudmail.ru 192.0.2.1 500\n"),
            response(data=b"\x00\x01"),
            response(404, headers={"x-upload-limit": str(10_000_000)}),
            response(201, headers={"x-valid-for": "3600"}),
            accepted_commit,
            self.stat_response(),
        ], parent_known=False)
        result = self.upload(api)
        self.assertTrue(result["commit_accepted"])
        self.assertTrue(result["stat_matched"])
        self.assertFalse(fake.sequence)
        self.assertEqual([item["method"] for item in fake.requests], ["GET", "GET", "POST", "PUT", "PUT", "POST", "GET"])
        body_request = next(item for item in fake.requests if item["stream_body"] is not None)
        self.assertEqual(body_request["stream_body"], self.data)
        self.assertNotIn("token=", body_request["url"])
        self.assertEqual(body_request["headers"]["Authorization"], "Bearer OLD-TOKEN")
        upload_requests = [r for r in fake.requests if r["method"] == "PUT"]
        queries = [parse_qs(urlsplit(r["url"]).query, keep_blank_values=True) for r in upload_requests]
        self.assertEqual(queries[0], queries[1])
        self.assertEqual(queries[0]["platform"], ["cloud-android"])
        self.assertEqual(queries[0]["gaid"], [""])
        self.assertEqual(queries[0]["is_background"], ["true"])
        self.assertEqual(queries[0]["upload_type"], ["AUTO"])
        self.assertEqual(queries[0]["network"], ["wi-fi"])
        self.assertEqual(queries[0]["is_retried"], ["false"])
        self.assertEqual(str(uuid.UUID(queries[0]["session_id"][0])), queries[0]["session_id"][0])

    def test_resume_transmits_only_unreceived_suffix(self):
        offset = 37
        target = "https://cloclo8.datacloudmail.ru/resumable/" + self.digest + "?upload_id=abc%2B123"
        api, fake, _ = self.api([
            response(404),
            response(200, headers={"x-received": str(offset), "x-location": target,
                                  "x-valid-for": "3600", "x-upload-limit": "10000000"}),
            response(201, headers={"x-valid-for": "3600"}),
            response(data={"name": self.local.name, "path": self.remote, "is_unlim_upload": True}),
            self.stat_response(),
        ])
        self.upload(api, is_retried=True)
        upload = next(item for item in fake.requests if item["stream_body"] is not None)
        self.assertTrue(upload["url"].startswith(target + "&"))
        query = parse_qs(urlsplit(upload["url"]).query, keep_blank_values=True)
        self.assertEqual(query["upload_id"], ["abc+123"])
        self.assertEqual(query["is_retried"], ["true"])
        self.assertEqual(upload["stream_body"], self.data[offset:])
        self.assertEqual(upload["headers"]["Content-Range"], f"bytes {offset}-{len(self.data)-1}/{len(self.data)}")

    def test_resumed_blob_progress_includes_offset_without_double_counting_completion(self):
        offset = 37
        api, fake, _ = self.api([
            response(404),
            response(200, headers={'x-valid-for': '0', 'x-received': str(offset),
                                  'x-location': UPLOAD_BASE + '/resumable/' + self.digest}),
            response(202, headers={'x-wait-for': '1'}),
            response(201, headers={'x-valid-for': '0'}),
            response(data={'name': self.local.name, 'path': self.remote, 'is_unlim_upload': True}),
            self.stat_response(),
        ])
        api._wait = lambda seconds: None
        deltas = []
        result = api.upload_photo(str(self.local), self.remote, expected=self.identity,
                                  save_intent=self.saved.append, save_confirmation=self.confirmed.append,
                                  progress=deltas.append)
        self.assertTrue(result['stat_matched'])
        self.assertEqual(deltas, [offset, self.identity.size - offset])
        self.assertEqual(sum(deltas), self.identity.size)
        self.assertEqual(next(item for item in fake.requests if item['stream_body'] is not None)['stream_body'],
                         self.data[offset:])

    def test_cached_complete_blob_reports_full_size_without_sending_body(self):
        api, fake, _ = self.api([
            response(404), response(201, headers={'x-valid-for': '0'}),
            response(data={'name': self.local.name, 'path': self.remote, 'is_unlim_upload': True}),
            self.stat_response(),
        ])
        deltas = []
        api.upload_photo(str(self.local), self.remote, expected=self.identity,
                         save_intent=self.saved.append, save_confirmation=self.confirmed.append,
                         progress=deltas.append)
        self.assertEqual(deltas, [self.identity.size])
        self.assertFalse(any(item['stream_body'] is not None for item in fake.requests))

    def test_lost_commit_response_cannot_infer_unlimited_from_matching_hash(self):
        api, fake, _ = self.api([
            response(404), response(201, headers={"x-valid-for": "3600"}),
            TransportError("Simulated lost finalization response"),
        ])
        with self.assertRaises(TransportError):
            self.upload(api)
        self.assertEqual(self.saved, [self.digest])
        intent = {"cloud_hash": self.digest, "remote_path": self.remote}
        api2, second, _ = self.api([self.stat_response()])
        with self.assertRaises(UnlimitedNotConfirmed):
            self.upload(api2, prior_intent=intent)
        self.assertEqual(self.confirmed, [])
        self.assertEqual(len(second.requests), 1)
        self.assertEqual(second.requests[0]["method"], "GET")

    def test_saved_positive_confirmation_recovers_after_stat_disconnect(self):
        api, _, _ = self.api([
            response(404), response(201, headers={"x-valid-for": "0"}),
            response(data={"name": self.local.name, "path": self.remote, "is_unlim_upload": True}),
            TransportError("Simulated stat disconnect"),
            TransportError("Simulated stat disconnect"),
            TransportError("Simulated stat disconnect"),
        ])
        with self.assertRaises(TransportError):
            self.upload(api)
        self.assertEqual(self.confirmed, [self.digest])
        intent = {"cloud_hash": self.digest, "remote_path": self.remote, "unlimited_confirmed": True}
        api2, second, _ = self.api([self.stat_response()])
        result = self.upload(api2, prior_intent=intent)
        self.assertTrue(result["recovered_commit"])
        self.assertFalse(result["commit_accepted"])
        self.assertIs(result["is_unlim_upload"], True)
        self.assertEqual(result["unlimited_evidence"], "saved_commit_response")
        self.assertEqual(len(second.requests), 1)
        self.assertEqual(second.requests[0]["method"], "GET")

    def test_confirmation_is_persisted_before_verification_stat(self):
        def checked_stat(_):
            self.assertEqual(self.confirmed, [self.digest])
            return self.stat_response()
        api, fake, _ = self.api([
            response(404), response(201, headers={"x-valid-for": "0"}),
            response(data={"name": self.local.name, "path": self.remote, "is_unlim_upload": True}), checked_stat,
        ])
        result = self.upload(api)
        self.assertIs(result["is_unlim_upload"], True)
        self.assertEqual(result["quota_status"], "server_confirmed_unlimited")
        self.assertFalse(fake.sequence)

    def test_task_scope_resets_cached_transport_after_cancelled_task(self):
        api, fake, _ = self.api([response(data={'ok': True})])
        cached = api.transport
        self.assertIs(cached.stop, api.stop)
        paused = threading.Event()
        paused.set()
        with self.assertRaises(Cancelled):
            with api.task_scope(paused):
                self.assertIs(api.transport, cached)
                self.assertIs(cached.stop, paused)
                api._request('GET', API_BASE + '/api/v1/user/info')
        self.assertIs(cached.stop, api.stop)
        self.assertFalse(api.stop.is_set())
        self.assertEqual(fake.requests, [])
        with api.task_scope(threading.Event()):
            self.assertEqual(api._request('GET', API_BASE + '/api/v1/user/info').status, 200)
        self.assertIs(cached.stop, api.stop)
        self.assertEqual(len(fake.requests), 1)

    def test_accepted_commit_is_saved_when_pause_arrives_with_response(self):
        paused = threading.Event()
        def accepted_after_pause(_):
            paused.set()
            return response(data={'name': self.local.name, 'path': self.remote, 'is_unlim_upload': True})
        api, fake, _ = self.api([
            response(404), response(201, headers={'x-valid-for': '0'}),
            accepted_after_pause, self.stat_response(),
        ])
        with self.assertRaises(Cancelled):
            with api.task_scope(paused):
                self.upload(api)
        self.assertEqual(self.saved, [self.digest])
        self.assertEqual(self.confirmed, [self.digest])
        self.assertFalse(api.stop.is_set())
        self.assertEqual(len(fake.requests), 3, 'Do not start stat after pause, but keep the accepted response')
        with api.task_scope(threading.Event()):
            result = self.upload(api, prior_intent={'cloud_hash': self.digest, 'remote_path': self.remote,
                                                   'unlimited_confirmed': True})
        self.assertTrue(result['recovered_commit'])
        self.assertEqual(sum(request['method'] == 'POST' for request in fake.requests), 1)
        self.assertFalse(fake.sequence)

    def test_source_read_failure_has_safe_local_error(self):
        with patch('builtins.open', side_effect=PermissionError('private-detail-do-not-copy')):
            with self.assertRaises(SourceReadError) as caught:
                cloud_hash(self.local, expected=self.identity)
        self.assertNotIn('private-detail', str(caught.exception))

    def test_missing_or_nontrue_unlimited_flag_never_finishes_or_falls_back(self):
        missing = object()
        for flag in (missing, False, 0, 1, "true", None):
            with self.subTest(flag="missing" if flag is missing else flag):
                self.saved.clear()
                self.confirmed.clear()
                added = {"name": self.local.name, "path": self.remote}
                if flag is not missing:
                    added["is_unlim_upload"] = flag
                api, fake, _ = self.api([
                    response(404), response(201, headers={"x-valid-for": "0"}), response(data=added),
                ])
                with self.assertRaises(UnlimitedNotConfirmed):
                    self.upload(api)
                self.assertEqual(self.confirmed, [])
                self.assertEqual([r["method"] for r in fake.requests], ["GET", "PUT", "POST"])
                self.assertTrue(json.loads(fake.requests[-1]["body"])["unlimited_upload"])

    def test_matching_existing_file_without_our_intent_is_not_overwritten(self):
        api, fake, _ = self.api([self.stat_response()])
        with self.assertRaises(RemoteConflict):
            self.upload(api)
        self.assertEqual(len(fake.requests), 1)
        self.assertEqual(self.saved, [])

    def test_unlimited_rejection_does_not_fall_back_to_ordinary_add(self):
        api, fake, _ = self.api([
            response(404), response(201, headers={"x-valid-for": "3600"}),
            response(400, data={"error": "CONDITION/NOT_ACCEPTABLE/UNLIM_UPL_TOTAL"}),
        ])
        with self.assertRaises(RemoteError) as caught:
            self.upload(api)
        self.assertTrue(caught.exception.fatal)
        commits = [r for r in fake.requests if r["method"] == "POST"]
        self.assertEqual(len(commits), 1)
        self.assertTrue(json.loads(commits[0]["body"])["unlimited_upload"])
        self.assertEqual(urlsplit(commits[0]["url"]).path, "/api/v1/private/add")

    def test_disabled_entitlement_stops_before_writes(self):
        api, fake, _ = self.api([response(data={"email": "owner@example.ru", "unlimited_upload_enabled": False})])
        with self.assertRaises(ProtocolError):
            api.require_account("owner@example.ru")
        self.assertEqual([r["method"] for r in fake.requests], ["GET"])

    def test_missing_entitlement_or_wrong_account_fails_closed(self):
        for data in ({"email": "owner@example.ru"}, {"email": "different@example.ru", "unlimited_upload_enabled": True}):
            with self.subTest(data=data):
                api, _, _ = self.api([response(data=data)])
                with self.assertRaises(ProtocolError):
                    api.require_account("owner@example.ru")

    def test_rejected_token_refreshed_once_then_retried(self):
        api, fake, tokens = self.api([response(403), response(data={"email": "owner@example.ru", "unlimited_upload_enabled": True})])
        info = api.require_account("owner@example.ru")
        self.assertTrue(info.unlimited_enabled)
        self.assertEqual(tokens.refresh_calls, 1)
        self.assertEqual([r["headers"]["Authorization"] for r in fake.requests], ["Bearer OLD-TOKEN", "Bearer NEW-TOKEN"])

    def test_repeated_unauthorized_does_not_loop(self):
        api, fake, tokens = self.api([response(403), response(403)])
        with self.assertRaises(RemoteError) as caught:
            api.require_account()
        self.assertTrue(is_auth_failure(caught.exception))
        self.assertEqual(tokens.refresh_calls, 1)
        self.assertEqual(len(fake.requests), 2)

    def test_public_dispatcher_rejection_does_not_invalidate_account_credentials(self):
        for status in (401, 403):
            with self.subTest(status=status):
                api, fake, tokens = self.api([response(status)])
                with self.assertRaises(RemoteError) as caught:
                    api._metadata_endpoint()
                self.assertFalse(is_auth_failure(caught.exception))
                self.assertTrue(caught.exception.fatal, 'Discovery failure still stops the upload safely')
                self.assertEqual(tokens.refresh_calls, 0)
                self.assertEqual(len(fake.requests), 1)
                self.assertNotIn('Authorization', fake.requests[0]['headers'])

    def test_redirect_is_not_followed_with_credentials(self):
        api, fake, _ = self.api([response(307, headers={"location": "https://other.example/collect"})])
        with self.assertRaises(RemoteError):
            self.upload(api)
        self.assertEqual(len(fake.requests), 1)

    def test_untrusted_resume_location_gets_no_bytes(self):
        api, fake, _ = self.api([
            response(404), response(200, headers={"x-received": "10", "x-location": "https://cloud.mail.ru.evil.example/collect",
                                                "x-valid-for": "3600"}),
        ])
        with self.assertRaises(TransportError):
            self.upload(api)
        self.assertFalse(any(r["stream_body"] is not None for r in fake.requests))
        self.assertEqual(self.saved, [])

    def test_modified_source_during_streaming_is_never_committed(self):
        def mutate_after_body(request):
            with self.local.open("ab") as changed:
                changed.write(b"changed")
            return response(201, headers={"x-valid-for": "3600"})

        api, fake, _ = self.api([response(404), response(404, headers={"x-upload-limit": "10000000"}), mutate_after_body])
        with self.assertRaises(SourceChanged):
            self.upload(api)
        self.assertFalse(any(r["method"] == "POST" for r in fake.requests))
        self.assertEqual(self.saved, [])

    def test_wrong_hash_after_commit_is_not_reported_as_success(self):
        api, fake, _ = self.api([
            response(404), response(201, headers={"x-valid-for": "3600"}),
            response(data={"name": self.local.name, "is_unlim_upload": True}), self.stat_response(digest="0" * 40),
        ])
        with self.assertRaises(ProtocolError):
            self.upload(api)
        self.assertFalse(fake.sequence)

    def test_missing_blob_confirmation_stops_before_commit(self):
        api, fake, _ = self.api([response(404), response(201)])
        with self.assertRaises(ProtocolError):
            self.upload(api)
        self.assertFalse(any(r["method"] == "POST" for r in fake.requests))

    def test_malformed_json_or_size_does_not_succeed(self):
        malformed = [b"<html>unexpected</html>", {"size": len(self.data), "hash": "no hash"}]
        malformed.extend({"size": size, "hash": self.digest} for size in (
            True, -1, "-1", float("inf"), float("nan"), "inf", "nan", None, 1 << 65, "9" * 5000,
        ))
        for data in malformed:
            with self.subTest(data=data):
                api, fake, _ = self.api([response(data=data)])
                with self.assertRaises(ProtocolError):
                    self.upload(api)
                self.assertEqual(len(fake.requests), 1)

    def test_invalid_blob_lifetime_never_reaches_commit(self):
        for lifetime in (None, "", "-1", "nan", "inf", "1.5", "9223372036854775808"):
            for resume_status in (200, 201):
                with self.subTest(lifetime=lifetime, resume_status=resume_status):
                    headers = {"x-received": "10", "x-location": UPLOAD_BASE + "/resumable/" + self.digest}
                    if lifetime is not None:
                        headers["x-valid-for"] = lifetime
                    api, fake, _ = self.api([response(404), response(resume_status, headers=headers)])
                    with self.assertRaises(ProtocolError):
                        self.upload(api)
                    self.assertFalse(any(r["method"] == "POST" or r["stream_body"] is not None for r in fake.requests))

    def test_invalid_lifetime_after_sending_bytes_never_commits(self):
        for lifetime in (None, "", "-1", "nan", "inf", "1.5", "9223372036854775808"):
            with self.subTest(lifetime=lifetime):
                headers = {} if lifetime is None else {"x-valid-for": lifetime}
                api, fake, _ = self.api([
                    response(404), response(404, headers={"x-upload-limit": "10000000"}),
                    response(201, headers=headers),
                ])
                with self.assertRaises(ProtocolError):
                    self.upload(api)
                self.assertFalse(any(r["method"] == "POST" for r in fake.requests))
                self.assertEqual(self.saved, [])

    def test_zero_lifetime_available_blob_can_finalize_verified_file(self):
        api, fake, _ = self.api([
            response(404), response(201, headers={"x-valid-for": "0"}),
            response(data={"name": self.local.name, "path": self.remote, "is_unlim_upload": True}),
            self.stat_response(),
        ])
        result = self.upload(api)
        self.assertTrue(result["commit_accepted"])
        self.assertTrue(result["stat_matched"])
        self.assertEqual(self.saved, [self.digest])
        self.assertFalse(any(r["stream_body"] is not None for r in fake.requests))
        self.assertFalse(fake.sequence)

    def test_zero_lifetime_partial_blob_resumes_remaining_bytes_then_finalizes(self):
        offset = 37
        api, fake, _ = self.api([
            response(404),
            response(200, headers={"x-valid-for": "0", "x-received": str(offset),
                                  "x-location": UPLOAD_BASE + "/resumable/" + self.digest}),
            response(201, headers={"x-valid-for": "3600"}),
            response(data={"name": self.local.name, "path": self.remote, "is_unlim_upload": True}),
            self.stat_response(),
        ])
        result = self.upload(api)
        self.assertTrue(result["commit_accepted"])
        self.assertTrue(result["stat_matched"])
        upload = next(r for r in fake.requests if r["stream_body"] is not None)
        self.assertEqual(upload["stream_body"], self.data[offset:])
        self.assertEqual(upload["headers"]["Content-Range"], f"bytes {offset}-{len(self.data)-1}/{len(self.data)}")
        self.assertFalse(fake.sequence)

    def test_zero_lifetime_after_body_can_finalize_verified_file(self):
        api, fake, _ = self.api([
            response(404), response(404, headers={"x-upload-limit": "10000000"}),
            response(201, headers={"x-valid-for": "0"}),
            response(data={"name": self.local.name, "path": self.remote, "is_unlim_upload": True}),
            self.stat_response(),
        ])
        result = self.upload(api)
        self.assertTrue(result["commit_accepted"])
        self.assertTrue(result["stat_matched"])
        self.assertEqual(self.saved, [self.digest])
        self.assertEqual(next(r["stream_body"] for r in fake.requests if r["stream_body"] is not None), self.data)
        self.assertFalse(fake.sequence)


class TransportBoundaryTests(unittest.TestCase):
    def test_only_known_tls_service_locations_are_accepted(self):
        for address in (
            "https://android-api.cloud.mail.ru/api/v1/user/info",
            "https://uploader.cloud.mail.ru/resumable/ABC",
            "https://cloclo10.datacloudmail.ru/upload",
            "https://x.cldmail.ru/upload",
        ):
            validate_url(address)
        for address in (
            "http://uploader.cloud.mail.ru/upload",
            "https://cloud.mail.ru.evil.example/upload",
            "https://evilcloud.mail.ru/upload",
            "https://cloud.mail.ru:444/upload",
            "https://name:password@cloud.mail.ru/upload",
            "https://cloud.mail.ru/upload#fragment",
            "https://cloud.mail.ru/upload\r\nFake: header",
            " https://cloud.mail.ru/upload",
            "https://cloud.mail.ru/файл",
            "https://127.0.0.1/upload",
        ):
            with self.subTest(address=address):
                with self.assertRaises(TransportError):
                    validate_url(address)

    def test_request_timeout_must_be_finite(self):
        for value in (-1, 0, float("nan"), float("inf")):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    HTTP(timeout=value)

    def test_retry_after_cannot_create_nonfinite_or_overflowing_wait(self):
        for value in ("NaN", "nan", "inf", "Infinity", "-inf", "1e300", "not a date"):
            with self.subTest(value=value):
                self.assertIsNone(_retry_after(response(429, headers={"retry-after": value})))
        self.assertEqual(_retry_after(response(429, headers={"retry-after": "30"})), 30)
        self.assertEqual(_retry_after(response(429, headers={"retry-after": "0"})), 0)


if __name__ == "__main__":
    unittest.main()
