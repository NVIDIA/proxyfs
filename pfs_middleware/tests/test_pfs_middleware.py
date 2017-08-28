# Copyright (c) 2016 SwiftStack, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
import collections
import errno
import eventlet
import hashlib
import json
import mock
import os
import socket
import unittest
from StringIO import StringIO
from swift.common import swob
from xml.etree import ElementTree

import pfs_middleware.middleware as mware


class FakeProxy(object):
    """
    Vaguely Swift-proxy-server-ish WSGI application used in testing the
    ProxyFS middleware.
    """
    def __init__(self):
        self._calls = []

        # key is a 2-tuple (request-method, url-path)
        #
        # value is how many WSGI iterables were created but not destroyed;
        # if all is well-behaved, the value should be 0 upon completion of
        # the user's request.
        self._unclosed_req_paths = collections.defaultdict(int)

        # key is a 2-tuple (request-method, url-path)
        #
        # value is a 3-tuple (response status, headers, body)
        self._responses = {}

    @property
    def calls(self):
        return tuple(self._calls)

    def register(self, method, path, response_status, headers, body=''):
        self._responses[(method, path)] = (response_status, headers, body)

    def __call__(self, env, start_response):
        method = env['REQUEST_METHOD']
        path = env['PATH_INFO']

        req = swob.Request(env)
        self._calls.append((method, path, swob.HeaderKeyDict(req.headers)))

        if (env.get('swift.authorize') and not env.get(
                'swift.authorize_override')):
            denial_response = env['swift.authorize'](req)
            if denial_response:
                return denial_response

        try:
            status_int, headers, body = self._responses[(method, path)]
        except KeyError:
            print("Didn't find \"%s %s\" in registered responses" % (
                method, path))
            raise

        if method == 'PUT':
            bytes_read = 0
            # consume the whole request body, just like a PUT would
            for chunk in iter(env['wsgi.input'].read, ''):
                bytes_read += len(chunk)
            cl = req.headers.get('Content-Length')

            if cl is not None and cl != bytes_read:
                error_resp = swob.HTTPClientDisconnect(
                    request=req,
                    body=("Content-Length didn't match"
                          " body length (says FakeProxy)"))
                return error_resp(env, start_response)

            if cl is None \
               and "chunked" not in req.headers.get("Transfer-Encoding", ""):
                error_resp = swob.HTTPLengthRequired(
                    request=req,
                    body="No Content-Length (says FakeProxy)")
                return error_resp(env, start_response)

        resp = swob.Response(
            body=body, status=status_int, headers=headers,
            # We cheat a little here and use swob's handling of the Range
            # header instead of doing it ourselves.
            conditional_response=True)

        return resp(env, start_response)


class FakeJsonRpc(object):
    """
    Fake out JSON-RPC calls.

    This object is used to replace the JsonRpcClient helper object.
    """
    def __init__(self):
        self._calls = []
        self._rpc_handlers = {}

    @property
    def calls(self):
        return tuple(self._calls)

    def register_handler(self, method, handler):
        """
        :param method: name of JSON-RPC method, e.g. Server.RpcGetObject

        :param handler: callable to handle that method. Callable must take
            one JSON-RPC object (dict) as argument and return a single
            JSON-RPC object (dict).
        """
        self._rpc_handlers[method] = handler

    def call(self, rpc_request, _timeout):
        # Note: rpc_request here is a JSON-RPC request object. In Python
        # terms, it's a dictionary with a particular format.

        call_id = rpc_request.get('id')

        # let any exceptions out so callers can see them and fix their
        # request generators
        assert rpc_request['jsonrpc'] == '2.0'
        method = rpc_request['method']

        # rpc.* are reserved by JSON-RPC 2.0
        assert not method.startswith("rpc.")

        # let KeyError out so callers can see it and fix their mocks
        handler = self._rpc_handlers[method]

        # params may be omitted, a list (positional), or a dict (by name)
        if 'params' not in rpc_request:
            rpc_response = handler()
            self._calls.append((method, ()))
        elif isinstance(rpc_request['params'], (list, tuple)):
            rpc_response = handler(*(rpc_request['params']))
            self._calls.append((method, tuple(rpc_request['params'])))
        elif isinstance(rpc_request['params'], (dict,)):
            raise NotImplementedError("haven't needed this yet")
        else:
            raise ValueError(
                "FakeJsonRpc can't handle params of type %s (%r)" %
                (type(rpc_request['params']), rpc_request['params']))

        if call_id is None:
            # a JSON-RPC request without an "id" parameter is a
            # "notification", i.e. a special request that receives no
            # response.
            return None
        elif 'id' in rpc_response and rpc_response['id'] != call_id:
            # We don't enforce that the handler pay any attention to 'id',
            # but if it does, it has to get it right.
            raise ValueError("handler for %s set 'id' attr to bogus value" %
                             (method,))
        else:
            rpc_response['id'] = call_id
        return rpc_response


class FakeJsonRpcWithErrors(FakeJsonRpc):
    def __init__(self, *a, **kw):
        super(FakeJsonRpcWithErrors, self).__init__(*a, **kw)
        self._errors = []

    def add_call_error(self, ex):
        self._errors.append(ex)

    def call(self, *a, **kw):
        # Call super() so we get call tracking.
        retval = super(FakeJsonRpcWithErrors, self).call(*a, **kw)
        if self._errors:
            raise self._errors.pop(0)
        return retval


class FakeLogger(object):
    def critical(fmt, *args):
        pass

    def exception(fmt, *args):
        pass

    def error(fmt, *args):
        pass

    def warn(fmt, *args):
        pass

    def info(fmt, *args):
        pass

    def debug(fmt, *args):
        pass


class TestHelperFunctions(unittest.TestCase):
    def test_extract_errno(self):
        self.assertEqual(2, mware.extract_errno("errno: 2"))
        self.assertEqual(17, mware.extract_errno("errno: 17"))
        self.assertEqual(None, mware.extract_errno("it broke"))

    def test_parse_path(self):
        self.assertEqual(
            mware.parse_path("/v1/a/c/o"),
            ["v1", "a", "c", "o"])
        self.assertEqual(
            mware.parse_path("/v1/a/c/obj/with/slashes"),
            ["v1", "a", "c", "obj/with/slashes"])
        self.assertEqual(
            mware.parse_path("/v1/a/c/obj/trailing/slashes///"),
            ["v1", "a", "c", "obj/trailing/slashes///"])
        self.assertEqual(
            mware.parse_path("/v1/a/c/"),
            ["v1", "a", "c", None])
        self.assertEqual(
            mware.parse_path("/v1/a/c"),
            ["v1", "a", "c", None])
        self.assertEqual(
            mware.parse_path("/v1/a/"),
            ["v1", "a", None, None])
        self.assertEqual(
            mware.parse_path("/v1/a"),
            ["v1", "a", None, None])
        self.assertEqual(
            mware.parse_path("/info"),
            ["info", None, None, None])
        self.assertEqual(
            mware.parse_path("/"),
            [None, None, None, None])


class BaseMiddlewareTest(unittest.TestCase):
    # no test cases in here, just common setup and utility functions
    def setUp(self):
        super(BaseMiddlewareTest, self).setUp()
        self.app = FakeProxy()
        self.pfs = mware.PfsMiddleware(self.app, {
            'bimodal_recheck_interval': 'inf',  # avoid timing dependencies
        }, FakeLogger())
        self.bimodal_accounts = {"AUTH_test"}

        self.app.register('HEAD', '/v1/AUTH_test', 204, {}, '')

        self.app.register(
            'GET', '/info',
            200, {'Content-Type': 'application/json'},
            json.dumps({
                # some stuff omitted
                "slo": {
                    "max_manifest_segments": 1000,
                    "max_manifest_size": 2097152,
                    "min_segment_size": 1},
                "swift": {
                    "account_autocreate": True,
                    "account_listing_limit": 9876,  # note: not default
                    "allow_account_management": True,
                    "container_listing_limit": 6543,  # note: not default
                    "extra_header_count": 0,
                    "max_account_name_length": 256,
                    "max_container_name_length": 256,
                    "max_file_size": 5368709122,
                    "max_header_size": 8192,
                    "max_meta_count": 90,
                    "max_meta_name_length": 128,
                    "max_meta_overall_size": 4096,
                    "max_meta_value_length": 256,
                    "max_object_name_length": 1024,
                    "policies": [{
                        "aliases": "default",
                        "default": True,
                        "name": "default"}],
                    "strict_cors_mode": True,
                    "version": "2.9.1.dev47"
                },
                "tempauth": {"account_acls": True}}))

        self.fake_rpc = FakeJsonRpc()
        patcher = mock.patch('pfs_middleware.middleware.JsonRpcClient',
                             lambda *_: self.fake_rpc)
        patcher.start()
        self.addCleanup(patcher.stop)

        def fake_RpcIsAccountBimodal(request):
            return {
                "error": None,
                "result": {
                    "IsBimodal":
                    request["AccountName"] in self.bimodal_accounts,
                    "ActivePeerPrivateIPAddr": "127.0.0.1",
                }}

        self.fake_rpc.register_handler("Server.RpcIsAccountBimodal",
                                       fake_RpcIsAccountBimodal)

    def call_app(self, req, app=None, expect_exception=False):
        if app is None:
            app = self.app

        status = [None]
        headers = [None]

        def start_response(s, h, ei=None):
            status[0] = s
            headers[0] = swob.HeaderKeyDict(h)

        body_iter = app(req.environ, start_response)
        body = ''
        caught_exc = None
        try:
            try:
                for chunk in body_iter:
                    body += chunk
            finally:
                # WSGI says we have to do this. Plus, if we don't, then
                # our leak detector gets false positives.
                if callable(getattr(body_iter, 'close', None)):
                    body_iter.close()
        except Exception as exc:
            if expect_exception:
                caught_exc = exc
            else:
                raise

        if expect_exception:
            return status[0], headers[0], body, caught_exc
        else:
            return status[0], headers[0], body

    def call_pfs(self, req, **kwargs):
        return self.call_app(req, app=self.pfs, **kwargs)


class TestAccountGet(BaseMiddlewareTest):
    def setUp(self):
        super(TestAccountGet, self).setUp()

        self.app.register(
            'HEAD', '/v1/AUTH_test',
            204,
            {'X-Account-Meta-Flavor': 'cherry',
             'X-Account-Sysmeta-Shipping-Class': 'ultraslow'},
            '')

        def mock_RpcGetAccount(_):
            return {
                "error": None,
                "result": {
                    "AccountEntries": [
                        {"Basename": "chickens"},
                        {"Basename": "cows"},
                        {"Basename": "goats"},
                        {"Basename": "pigs"}]}}

        self.fake_rpc.register_handler(
            "Server.RpcGetAccount", mock_RpcGetAccount)

    def test_text(self):
        req = swob.Request.blank("/v1/AUTH_test")
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(body, "chickens\ncows\ngoats\npigs\n")

    def test_json(self):
        req = swob.Request.blank("/v1/AUTH_test?format=json")
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(json.loads(body), [
            {"count": 0, "bytes": 0, "name": "chickens"},
            {"count": 0, "bytes": 0, "name": "cows"},
            {"count": 0, "bytes": 0, "name": "goats"},
            {"count": 0, "bytes": 0, "name": "pigs"}])

    def test_xml(self):
        req = swob.Request.blank("/v1/AUTH_test?format=xml")
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(headers["Content-Type"],
                         "application/xml; charset=utf-8")
        self.assertTrue(body.startswith(
            """<?xml version='1.0' encoding='utf-8'?>"""))

        root_node = ElementTree.fromstring(body)
        self.assertEqual(root_node.tag, 'account')
        self.assertEqual(root_node.attrib["name"], 'AUTH_test')

        containers = root_node.getchildren()
        self.assertEqual(containers[0].tag, 'container')

        # The XML account listing doesn't use XML attributes for data, but
        # rather a sequence of tags like <name>X</name> <bytes>Y</bytes> ...
        con_attr_tags = containers[0].getchildren()
        self.assertEqual(len(con_attr_tags), 3)

        name_node = con_attr_tags[0]
        self.assertEqual(name_node.tag, 'name')
        self.assertEqual(name_node.text, 'chickens')
        self.assertEqual(name_node.attrib, {})  # nothing extra in there

        count_node = con_attr_tags[1]
        self.assertEqual(count_node.tag, 'count')
        self.assertEqual(count_node.text, '0')
        self.assertEqual(count_node.attrib, {})

        bytes_node = con_attr_tags[2]
        self.assertEqual(bytes_node.tag, 'bytes')
        self.assertEqual(bytes_node.text, '0')
        self.assertEqual(bytes_node.attrib, {})

    def test_metadata(self):
        req = swob.Request.blank("/v1/AUTH_test")
        status, headers, body = self.call_pfs(req)
        self.assertEqual(headers.get("X-Account-Meta-Flavor"), "cherry")
        self.assertEqual(headers.get("X-Account-Sysmeta-Shipping-Class"),
                         "ultraslow")

    def test_marker(self):
        req = swob.Request.blank("/v1/AUTH_test?marker=mk")
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(2, len(self.fake_rpc.calls))
        # rpc_calls[0] is a call to RpcIsAccountBimodal, which is not
        # relevant to what we're testing here
        self.assertEqual(self.fake_rpc.calls[1][1][0]['Marker'], 'mk')

    def test_limit(self):
        req = swob.Request.blank("/v1/AUTH_test?limit=101")
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(2, len(self.fake_rpc.calls))
        self.assertEqual(self.fake_rpc.calls[1][1][0]['MaxEntries'], 101)

    def test_default_limit(self):
        req = swob.Request.blank("/v1/AUTH_test")
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(2, len(self.fake_rpc.calls))
        # value from GET /info
        self.assertEqual(self.fake_rpc.calls[1][1][0]['MaxEntries'], 9876)

    def test_not_found(self):
        self.app.register('HEAD', '/v1/AUTH_missing', 404, {}, '')
        self.app.register('GET', '/v1/AUTH_missing', 404, {}, '')
        req = swob.Request.blank("/v1/AUTH_missing")
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '404 Not Found')

    def test_rpc_error(self):
        def broken_RpcGetAccount(_):
            return {
                "error": "errno: 123",  # meaningless errno
                "result": None}

        self.fake_rpc.register_handler(
            "Server.RpcGetAccount", broken_RpcGetAccount)
        req = swob.Request.blank("/v1/AUTH_test")
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '500 Internal Error')

    def test_empty_last_page(self):
        def last_page_RpcGetAccount(_):
            return {
                "error": None,
                # None, not [], for mysterious reasons
                "result": {"AccountEntries": None}}

        self.fake_rpc.register_handler(
            "Server.RpcGetAccount", last_page_RpcGetAccount)
        req = swob.Request.blank("/v1/AUTH_test?marker=zzz")
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(body, '')

    def test_spaces(self):
        self.bimodal_accounts.add('AUTH_test with spaces')
        self.app.register('HEAD', '/v1/AUTH_test with spaces', 204, {}, '')
        req = swob.Request.blank("/v1/AUTH_test with spaces")
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(self.fake_rpc.calls[1][1][0]['VirtPath'],
                         '/v1/AUTH_test with spaces')


class TestAccountHead(BaseMiddlewareTest):
    def setUp(self):
        super(TestAccountHead, self).setUp()

        self.app.register(
            'HEAD', '/v1/AUTH_test',
            204,
            {'X-Account-Meta-Beans': 'lots of'},
            '')

    def test_indicator_header(self):
        req = swob.Request.blank("/v1/AUTH_test",
                                 environ={"REQUEST_METHOD": "HEAD"})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '204 No Content')
        self.assertEqual(headers.get("ProxyFS-Enabled"), "yes")
        self.assertEqual(body, '')

    def test_in_transit(self):

        def fake_RpcIsAccountBimodal(request):
            return {
                "error": None,
                "result": {
                    "IsBimodal": True,
                    "ActivePeerPrivateIPAddr": ""}}
        self.fake_rpc.register_handler("Server.RpcIsAccountBimodal",
                                       fake_RpcIsAccountBimodal)

        req = swob.Request.blank("/v1/AUTH_test",
                                 environ={"REQUEST_METHOD": "HEAD"})
        status, _, _ = self.call_pfs(req)
        self.assertEqual(status, '503 Service Unavailable')


class TestObjectGet(BaseMiddlewareTest):
    def setUp(self):
        super(TestObjectGet, self).setUp()

        def dummy_rpc(request):
            return {"error": None, "result": {}}

        self.fake_rpc.register_handler("Server.RpcRenewLease", dummy_rpc)
        self.fake_rpc.register_handler("Server.RpcReleaseLease", dummy_rpc)

    def test_info_passthrough(self):
        self.app.register(
            'GET', '/info', 200, {}, '{"stuff": "yes"}')

        req = swob.Request.blank('/info')
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(body, '{"stuff": "yes"}')

    def test_non_bimodal_account(self):
        self.app.register(
            'HEAD', '/v1/AUTH_unimodal', 204, {}, '')
        self.app.register(
            'GET', '/v1/AUTH_unimodal/c/o', 200, {}, 'squirrel')

        req = swob.Request.blank('/v1/AUTH_unimodal/c/o')
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(body, 'squirrel')

    def test_GET_basic(self):
        # ProxyFS log segments look a lot like actual file contents followed
        # by a bunch of fairly opaque binary data plus some JSON-looking
        # bits. The actual bytes in the file here are the 8 that spell
        # "burritos"; the rest of the bytes are there so we can omit them in
        # the response to the user.
        self.app.register(
            'GET', '/v1/AUTH_test/InternalContainerName/0000000000c11fbd',
            200, {},
            ("burritos\x62\x6f\x6f\x74{\"Stuff\": \"probably\"}\x00\x00\x00"))

        def mock_RpcGetObject(get_object_req):
            self.assertEqual(get_object_req['VirtPath'],
                             "/v1/AUTH_test/notes/lunch")
            self.assertEqual(get_object_req['ReadEntsIn'], [])

            return {
                "error": None,
                "result": {
                    "FileSize": 8,
                    "Metadata": "",
                    "InodeNumber": 1245,
                    "NumWrites": 2424,
                    "ModificationTime": 1481152134331862558,
                    "LeaseId": "prominority-sarcocyst",
                    "ReadEntsOut": [{
                        "ObjectPath": ("/v1/AUTH_test/InternalContainer"
                                       "Name/0000000000c11fbd"),
                        "Offset": 0,
                        "Length": 8}]}}

        req = swob.Request.blank('/v1/AUTH_test/notes/lunch')

        self.fake_rpc.register_handler(
            "Server.RpcGetObject", mock_RpcGetObject)
        status, headers, body = self.call_pfs(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers["Last-Modified"],
                         "Wed, 07 Dec 2016 23:08:55 GMT")
        self.assertEqual(headers["ETag"],
                         mware.construct_etag("AUTH_test", 1245, 2424))
        self.assertEqual(body, 'burritos')

    def test_GET_authed(self):
        self.app.register(
            'GET', '/v1/AUTH_test/InternalContainerName/0000000001178995',
            200, {},
            ("burr\x62\x6f\x6f\x74{\"Stuff\": \"probably\"}\x00\x00\x00"))
        self.app.register(
            'GET', '/v1/AUTH_test/InternalContainerName/0000000004181255',
            200, {},
            ("itos\x62\x6f\x6f\x74{\"more\": \"junk\"}\x00\x00\x00"))

        def mock_RpcHead(head_req):
            return {
                "error": None,
                "result": {
                    "Metadata": None,
                    "ModificationTime": 1488414932080993000,
                    "FileSize": 0,
                    "IsDir": True,
                    "InodeNumber": 6283109,
                    "NumWrites": 0,
                }}

        def mock_RpcGetObject(get_object_req):
            self.assertEqual(get_object_req['VirtPath'],
                             "/v1/AUTH_test/notes/lunch")
            self.assertEqual(get_object_req['ReadEntsIn'], [])

            return {
                "error": None,
                "result": {
                    "FileSize": 8,
                    "Metadata": "",
                    "InodeNumber": 1245,
                    "NumWrites": 2424,
                    "ModificationTime": 1481152134331862558,
                    "LeaseId": "a65b5591b90fe6035e669f1f216502d2",
                    "ReadEntsOut": [{
                        "ObjectPath": ("/v1/AUTH_test/InternalContainer"
                                       "Name/0000000001178995"),
                        "Offset": 0,
                        "Length": 4},
                        {"ObjectPath": ("/v1/AUTH_test/InternalContainer"
                                        "Name/0000000004181255"),
                         "Offset": 0,
                         "Length": 4}]}}

        def auth_callback(req):
            if "InternalContainerName" in req.path:
                return swob.HTTPForbidden(body="lol nope", request=req)

        req = swob.Request.blank('/v1/AUTH_test/notes/lunch')
        req.environ["swift.authorize"] = auth_callback

        self.fake_rpc.register_handler(
            "Server.RpcGetObject", mock_RpcGetObject)
        self.fake_rpc.register_handler(
            "Server.RpcHead", mock_RpcHead)
        status, headers, body = self.call_pfs(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers["Last-Modified"],
                         "Wed, 07 Dec 2016 23:08:55 GMT")
        self.assertEqual(headers["ETag"],
                         mware.construct_etag("AUTH_test", 1245, 2424))
        self.assertEqual(body, 'burritos')

    def test_GET_sparse(self):
        # This log segment, except for the obvious fake metadata at the end,
        # was obtained by really doing this:
        #
        # with open(filename, 'w') as fh:
        #     fh.write('sparse')
        #     fh.seek(10006)
        #     fh.write('file')
        #
        # Despite its appearance, this is really what the underlying log
        # segment for a sparse file looks like, at least sometimes.
        self.app.register(
            'GET', '/v1/AUTH_test/InternalContainerName/00000000000000D0',
            200, {},
            ("sparsefile\x57\x18\xa0\xf3-junkety-junk"))

        def mock_RpcGetObject(get_object_req):
            self.assertEqual(get_object_req['VirtPath'],
                             "/v1/AUTH_test/c/sparse-file")
            self.assertEqual(get_object_req['ReadEntsIn'], [])

            return {
                "error": None,
                "result": {
                    "FileSize": 10010,
                    "Metadata": "",
                    "InodeNumber": 1245,
                    "NumWrites": 2424,
                    "ModificationTime": 1481152134331862558,
                    "LeaseId": "6840595b3370f109dc8ed388b41800a4",
                    "ReadEntsOut": [{
                        "ObjectPath": ("/v1/AUTH_test/InternalContainer"
                                       "Name/00000000000000D0"),
                        "Offset": 0,
                        "Length": 6
                    }, {
                        "ObjectPath": "",  # empty path means zero-fill
                        "Offset": 0,
                        "Length": 10000,
                    }, {
                        "ObjectPath": ("/v1/AUTH_test/InternalContainer"
                                       "Name/00000000000000D0"),
                        "Offset": 6,
                        "Length": 4}]}}

        req = swob.Request.blank('/v1/AUTH_test/c/sparse-file')

        self.fake_rpc.register_handler(
            "Server.RpcGetObject", mock_RpcGetObject)
        status, headers, body = self.call_pfs(req)

        self.assertEqual(status, "200 OK")
        self.assertEqual(body, "sparse" + ("\x00" * 10000) + "file")

    def test_GET_multiple_segments(self):
        # Typically, a GET request will include data from multiple log
        # segments. Small files written all at once might fit in a single
        # log segment, but that's not always true.
        self.app.register(
            'GET', '/v1/AUTH_test/InternalContainerName/0000000000000001',
            200, {},
            ("There once was an X from place B,\n\xff\xea\x00junk"))
        self.app.register(
            'GET', '/v1/AUTH_test/InternalContainerName/00000000000000a3',
            200, {},
            ("That satisfied predicate P.\n\xff\xea\x00junk"))
        self.app.register(
            'GET', '/v1/AUTH_test/InternalContainerName/00000000000000a4',
            200, {},
            ("He or she did thing A,\n\xff\xea\x00junk"))
        self.app.register(
            'GET', '/v1/AUTH_test/InternalContainerName/00000000000000e0',
            200, {},
            ("In an adjective way,\n\xff\xea\x00junk"))
        self.app.register(
            'GET', '/v1/AUTH_test/InternalContainerName/0000000000000108',
            200, {},
            ("Resulting in circumstance C.\xff\xea\x00junk"))

        def mock_RpcGetObject(get_object_req):
            self.assertEqual(get_object_req['VirtPath'],
                             "/v1/AUTH_test/limericks/generic")
            self.assertEqual(get_object_req['ReadEntsIn'], [])

            return {
                "error": None,
                "result": {
                    "FileSize": 134,
                    "Metadata": "",
                    "InodeNumber": 1245,
                    "NumWrites": 2424,
                    "ModificationTime": 1481152134331862558,
                    "LeaseId": "who cares",
                    "ReadEntsOut": [{
                        "ObjectPath": ("/v1/AUTH_test/InternalContainer"
                                       "Name/0000000000000001"),
                        "Offset": 0,
                        "Length": 34,
                    }, {
                        "ObjectPath": ("/v1/AUTH_test/InternalContainer"
                                       "Name/00000000000000a3"),
                        "Offset": 0,
                        "Length": 28,
                    }, {
                        "ObjectPath": ("/v1/AUTH_test/InternalContainer"
                                       "Name/00000000000000a4"),
                        "Offset": 0,
                        "Length": 23,
                    }, {
                        "ObjectPath": ("/v1/AUTH_test/InternalContainer"
                                       "Name/00000000000000e0"),
                        "Offset": 0,
                        "Length": 21,
                    }, {
                        "ObjectPath": ("/v1/AUTH_test/InternalContainer"
                                       "Name/0000000000000108"),
                        "Offset": 0,
                        "Length": 28,
                    }]}}

        req = swob.Request.blank('/v1/AUTH_test/limericks/generic')

        self.fake_rpc.register_handler(
            "Server.RpcGetObject", mock_RpcGetObject)
        status, headers, body = self.call_pfs(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(body, (
            "There once was an X from place B,\n"
            "That satisfied predicate P.\n"
            "He or she did thing A,\n"
            "In an adjective way,\n"
            "Resulting in circumstance C."))

    def test_GET_range(self):
        # Typically, a GET response will include data from multiple log
        # segments. Small files written all at once might fit in a single
        # log segment, but most files won't.
        self.app.register(
            'GET', '/v1/AUTH_test/InternalContainerName/0000000000000001',
            200, {},
            ("Caerphilly, Cheddar, Cheshire, \xff\xea\x00junk"))
        self.app.register(
            'GET', '/v1/AUTH_test/InternalContainerName/0000000000000002',
            200, {},
            ("Duddleswell, Dunlop, Coquetdale, \xff\xea\x00junk"))
        self.app.register(
            'GET', '/v1/AUTH_test/InternalContainerName/0000000000000003',
            200, {},
            ("Derby, Gloucester, Wensleydale\xff\xea\x00junk"))

        def mock_RpcGetObject(get_object_req):
            self.assertEqual(get_object_req['VirtPath'],
                             "/v1/AUTH_test/cheeses/UK")
            self.assertEqual(get_object_req['ReadEntsIn'],
                             [{"Offset": 21, "Len": 49}])

            return {
                "error": None,
                "result": {
                    "FileSize": 94,
                    "Metadata": "",
                    "InodeNumber": 1245,
                    "NumWrites": 2424,
                    "ModificationTime": 1481152134331862558,
                    "LeaseId": "982938",
                    "ReadEntsOut": [{
                        "ObjectPath": ("/v1/AUTH_test/InternalContainer"
                                       "Name/0000000000000001"),
                        "Offset": 21,
                        "Length": 10,
                    }, {
                        "ObjectPath": ("/v1/AUTH_test/InternalContainer"
                                       "Name/0000000000000002"),
                        "Offset": 0,
                        "Length": 33,
                    }, {
                        "ObjectPath": ("/v1/AUTH_test/InternalContainer"
                                       "Name/0000000000000003"),
                        "Offset": 0,
                        "Length": 5}]}}

        req = swob.Request.blank('/v1/AUTH_test/cheeses/UK',
                                 headers={"Range": "bytes=21-69"})

        self.fake_rpc.register_handler(
            "Server.RpcGetObject", mock_RpcGetObject)
        status, headers, body = self.call_pfs(req)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers.get('Content-Range'), "bytes 21-69/94")
        self.assertEqual(
            body, 'Cheshire, Duddleswell, Dunlop, Coquetdale, Derby')

    def test_GET_range_suffix(self):
        self.app.register(
            'GET', '/v1/AUTH_test/InternalContainerName/0000000000000001',
            200, {},
            ("hydrogen, helium, \xff\xea\x00junk"))
        self.app.register(
            'GET', '/v1/AUTH_test/InternalContainerName/0000000000000002',
            200, {},
            ("lithium, beryllium, boron, carbon, nitrogen, \xff\xea\x00junk"))

        def mock_RpcGetObject(get_object_req):
            self.assertEqual(get_object_req['VirtPath'],
                             "/v1/AUTH_test/c/elements")
            self.assertEqual(get_object_req['ReadEntsIn'],
                             [{"Offset": None, "Len": 10}])

            return {
                "error": None,
                "result": {
                    "FileSize": 94,
                    "Metadata": "",
                    "InodeNumber": 1245,
                    "NumWrites": 2424,
                    "ModificationTime": 1481152134331862558,
                    "LeaseId": "fc00:752b:5cca:a544:2d41:3177:2c71:85ae",
                    "ReadEntsOut": [{
                        "ObjectPath": ("/v1/AUTH_test/InternalContainer"
                                       "Name/0000000000000002"),
                        "Offset": 35,
                        "Length": 10}]}}

        req = swob.Request.blank('/v1/AUTH_test/c/elements',
                                 headers={"Range": "bytes=-10"})

        self.fake_rpc.register_handler(
            "Server.RpcGetObject", mock_RpcGetObject)
        status, headers, body = self.call_pfs(req)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers.get('Content-Range'), "bytes 84-93/94")
        self.assertEqual(body, "nitrogen, ")

    def test_GET_range_prefix(self):
        self.app.register(
            'GET', '/v1/AUTH_test/InternalContainerName/0000000000000001',
            200, {},
            ("hydrogen, helium, \xff\xea\x00junk"))
        self.app.register(
            'GET', '/v1/AUTH_test/InternalContainerName/0000000000000002',
            200, {},
            ("lithium, beryllium, boron, carbon, nitrogen, \xff\xea\x00junk"))

        def mock_RpcGetObject(get_object_req):
            self.assertEqual(get_object_req['VirtPath'],
                             "/v1/AUTH_test/c/elements")
            self.assertEqual(get_object_req['ReadEntsIn'],
                             [{"Offset": 40, "Len": None}])

            return {
                "error": None,
                "result": {
                    "FileSize": 62,
                    "Metadata": "",
                    "InodeNumber": 1245,
                    "NumWrites": 2424,
                    "ModificationTime": 1481152134331862558,
                    "LeaseId": "",
                    "ReadEntsOut": [{
                        "ObjectPath": ("/v1/AUTH_test/InternalContainer"
                                       "Name/0000000000000002"),
                        "Offset": 22,
                        "Length": 23}]}}

        req = swob.Request.blank('/v1/AUTH_test/c/elements',
                                 headers={"Range": "bytes=40-"})

        self.fake_rpc.register_handler(
            "Server.RpcGetObject", mock_RpcGetObject)
        status, headers, body = self.call_pfs(req)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers.get('Content-Range'), "bytes 40-61/62")
        self.assertEqual(body, "ron, carbon, nitrogen, ")

    def test_GET_range_unsatisfiable(self):
        self.app.register(
            'GET', '/v1/AUTH_test/InternalContainerName/0000000000000001',
            200, {},
            ("hydrogen, helium, \xff\xea\x00junk"))
        self.app.register(
            'GET', '/v1/AUTH_test/InternalContainerName/0000000000000002',
            200, {},
            ("lithium, beryllium, boron, carbon, nitrogen, \xff\xea\x00junk"))

        def mock_RpcGetObject(get_object_req):
            self.assertEqual(get_object_req['VirtPath'],
                             "/v1/AUTH_test/c/elements")
            self.assertEqual(get_object_req['ReadEntsIn'],
                             [{"Offset": 4000, "Len": None}])

            return {
                "error": None,
                "result": {
                    "FileSize": 62,
                    "Metadata": "",
                    "InodeNumber": 1245,
                    "NumWrites": 2424,
                    "ModificationTime": 1481152134331862558,
                    "LeaseId": "borkbork",
                    "ReadEntsOut": None}}

        req = swob.Request.blank('/v1/AUTH_test/c/elements',
                                 headers={"Range": "bytes=4000-"})

        self.fake_rpc.register_handler(
            "Server.RpcGetObject", mock_RpcGetObject)
        status, headers, body = self.call_pfs(req)

        self.assertEqual(status, '416 Requested Range Not Satisfiable')

    def test_GET_multiple_ranges(self):
        # Until Swift supports multiple ranges for large objects, we don't
        # either; this is one of the perils of code re-use. OTOH, using
        # SegmentedIterable saved us a whole bunch of work.
        #
        # This change has been proposed upstream; see
        # I24716e3271cf3370642e3755447e717fd7d9957c at
        # https://review.openstack.org for details.
        self.app.register(
            'GET', '/v1/AUTH_test/InternalContainerName/0000000000000001',
            200, {}, "abcd1234efgh5678")

        def mock_RpcGetObject(get_object_req):
            self.assertEqual(get_object_req['VirtPath'],
                             "/v1/AUTH_test/c/crud")
            self.assertEqual(get_object_req['ReadEntsIn'], [])

            return {
                "error": None,
                "result": {
                    "FileSize": 16,
                    "Metadata": "",
                    "InodeNumber": 1245,
                    "NumWrites": 2424,
                    "ModificationTime": 1481152134331862558,
                    "LeaseId": "e1885b511fa445d18b1d447a5606a06d",
                    "ReadEntsOut": [{
                        "ObjectPath": ("/v1/AUTH_test/InternalContainer"
                                       "Name/0000000000000001"),
                        "Offset": 0,
                        "Length": 16}]}}

        req = swob.Request.blank('/v1/AUTH_test/c/crud',
                                 headers={"Range": "bytes=2-4,6-8,10-12"})

        self.fake_rpc.register_handler(
            "Server.RpcGetObject", mock_RpcGetObject)
        status, headers, body = self.call_pfs(req)

        self.assertEqual(status, '200 OK')
        self.assertNotIn('Content-Range', headers)
        self.assertEqual(body, 'abcd1234efgh5678')

    def test_GET_metadata(self):
        self.app.register(
            'GET', '/v1/AUTH_test/InternalContainerName/0000000000000001',
            200, {}, "abcd1234efgh5678")

        def mock_RpcGetObject(get_object_req):
            self.assertEqual(get_object_req['VirtPath'],
                             "/v1/AUTH_test/c/crud")

            return {
                "error": None,
                "result": {
                    "FileSize": 16,
                    "Metadata": base64.b64encode(
                        json.dumps({"X-Object-Meta-Cow": "moo"})),
                    "InodeNumber": 1245,
                    "NumWrites": 2424,
                    "ModificationTime": 1481152134331862558,
                    "LeaseId": "fe57a6ed-758a-23fb-f7d4-9683aee07c0e",
                    "ReadEntsOut": [{
                        "ObjectPath": ("/v1/AUTH_test/InternalContainer"
                                       "Name/0000000000000001"),
                        "Offset": 0,
                        "Length": 16}]}}

        req = swob.Request.blank('/v1/AUTH_test/c/crud')

        self.fake_rpc.register_handler(
            "Server.RpcGetObject", mock_RpcGetObject)
        status, headers, body = self.call_pfs(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers.get("X-Object-Meta-Cow"), "moo")
        self.assertEqual(body, 'abcd1234efgh5678')

    def test_GET_not_found(self):
        def mock_RpcGetObject(get_object_req):
            self.assertEqual(get_object_req['VirtPath'],
                             "/v1/AUTH_test/c/missing")

            return {
                "error": "errno: 2",
                "result": None}

        req = swob.Request.blank('/v1/AUTH_test/c/missing')
        self.fake_rpc.register_handler(
            "Server.RpcGetObject", mock_RpcGetObject)
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '404 Not Found')

    def test_GET_weird_error(self):
        def mock_RpcGetObject(get_object_req):
            self.assertEqual(get_object_req['VirtPath'],
                             "/v1/AUTH_test/c/superborked")

            return {
                "error": "errno: 23",  # ENFILE (too many open files in system)
                "result": None}

        req = swob.Request.blank('/v1/AUTH_test/c/superborked')
        self.fake_rpc.register_handler(
            "Server.RpcGetObject", mock_RpcGetObject)
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '500 Internal Error')

    def test_GET_zero_byte(self):
        def mock_RpcGetObject(get_object_req):
            self.assertEqual(get_object_req['VirtPath'],
                             "/v1/AUTH_test/c/empty")
            return {
                "error": None,
                "result": {"FileSize": 0, "ReadEntsOut": None, "Metadata": "",
                           "LeaseId": "3d73d2bcf39224df00d5ccd912d92c82",
                           "InodeNumber": 1245, "NumWrites": 2424,
                           "ModificationTime": 1481152134331862558}}

        self.fake_rpc.register_handler(
            "Server.RpcGetObject", mock_RpcGetObject)

        req = swob.Request.blank('/v1/AUTH_test/c/empty')
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(headers["Content-Length"], "0")

    def test_GET_dir(self):
        def mock_RpcGetObject(get_object_req):
            self.assertEqual(get_object_req['VirtPath'],
                             "/v1/AUTH_test/c/a-dir")
            return {
                "error": "errno: 21",  # EISDIR
                "result": None}

        self.fake_rpc.register_handler(
            "Server.RpcGetObject", mock_RpcGetObject)

        req = swob.Request.blank('/v1/AUTH_test/c/a-dir')
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(headers["Content-Length"], "0")
        self.assertEqual(headers["Content-Type"], "application/directory")

    def test_GET_special_chars(self):
        self.app.register(
            'GET', '/v1/AUTH_test/InternalContainerName/0000000000000123',
            200, {}, "abcd1234efgh5678")

        def mock_RpcGetObject(get_object_req):
            return {
                "error": None,
                "result": {
                    "FileSize": 8,
                    "Metadata": "",
                    "InodeNumber": 1245,
                    "NumWrites": 2424,
                    "ModificationTime": 1481152134331862558,
                    "LeaseId": "a7ec296d3f3c39ef95407789c436f5f8",
                    "ReadEntsOut": [{
                        "ObjectPath": ("/v1/AUTH_test/InternalContainer"
                                       "Name/0000000000000123"),
                        "Offset": 0,
                        "Length": 16}]}}

        req = swob.Request.blank('/v1/AUTH_test/c o n/o b j')

        self.fake_rpc.register_handler(
            "Server.RpcGetObject", mock_RpcGetObject)
        status, headers, body = self.call_pfs(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(body, 'abcd1234efgh5678')
        self.assertEqual(self.fake_rpc.calls[1][1][0]['VirtPath'],
                         '/v1/AUTH_test/c o n/o b j')

    def test_lease_maintenance(self):
        self.app.register(
            'GET', '/v1/AUTH_test/InternalContainerName/0000000000000456',
            200, {}, "some contents")

        expected_lease_id = "f1abb6c7fc4b4463b04f8313d71986b7"

        def mock_RpcGetObject(get_object_req):
            return {
                "error": None,
                "result": {
                    "FileSize": 13,
                    "Metadata": "",
                    "InodeNumber": 7677424,
                    "NumWrites": 2325461,
                    "ModificationTime": 1488841810471415000,
                    "LeaseId": expected_lease_id,
                    "ReadEntsOut": [{
                        "ObjectPath": ("/v1/AUTH_test/InternalContainer"
                                       "Name/0000000000000456"),
                        "Offset": 0,
                        "Length": 13}]}}

        req = swob.Request.blank('/v1/AUTH_test/some/thing')

        self.fake_rpc.register_handler(
            "Server.RpcGetObject", mock_RpcGetObject)
        status, headers, body = self.call_pfs(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(body, 'some contents')

        called_rpcs = [c[0] for c in self.fake_rpc.calls]

        # There's at least one RpcRenewLease, but perhaps there's more: it's
        # time-based, and we aren't mocking out time in this test.
        self.assertEqual(called_rpcs[0], 'Server.RpcIsAccountBimodal')
        self.assertEqual(called_rpcs[1], 'Server.RpcGetObject')
        self.assertEqual(called_rpcs[-2], 'Server.RpcRenewLease')
        self.assertEqual(called_rpcs[-1], 'Server.RpcReleaseLease')

        # make sure we got the right lease ID
        self.assertEqual(self.fake_rpc.calls[-2][1][0]['LeaseId'],
                         expected_lease_id)
        self.assertEqual(self.fake_rpc.calls[-1][1][0]['LeaseId'],
                         expected_lease_id)


class TestContainerHead(BaseMiddlewareTest):
    def setUp(self):
        super(TestContainerHead, self).setUp()

        self.serialized_container_metadata = ""

        # All these tests run against the same container.
        def mock_RpcHead(head_container_req):
            return {
                "error": None,
                "result": {
                    "Metadata": base64.b64encode(
                        self.serialized_container_metadata),
                    "ModificationTime": 1479240397189581131,
                    "FileSize": 0,
                    "IsDir": True,
                    "InodeNumber": 2718,
                    "NumWrites": 0,
                }}

        self.fake_rpc.register_handler(
            "Server.RpcHead", mock_RpcHead)

    def test_special_chars(self):
        req = swob.Request.blank("/v1/AUTH_test/a container",
                                 environ={"REQUEST_METHOD": "HEAD"})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '204 No Content')
        self.assertEqual(self.fake_rpc.calls[1][1][0]['VirtPath'],
                         '/v1/AUTH_test/a container')

    def test_no_meta(self):
        self.serialized_container_metadata = ""

        req = swob.Request.blank("/v1/AUTH_test/a-container",
                                 environ={"REQUEST_METHOD": "HEAD"})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '204 No Content')
        self.assertEqual(self.fake_rpc.calls[1][1][0]['VirtPath'],
                         '/v1/AUTH_test/a-container')

    def test_bogus_meta(self):
        self.serialized_container_metadata = "{[{[{[{[{[[(((!"

        req = swob.Request.blank("/v1/AUTH_test/a-container",
                                 environ={"REQUEST_METHOD": "HEAD"})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '204 No Content')

    def test_meta(self):
        self.serialized_container_metadata = json.dumps({
            "X-Container-Sysmeta-Fish": "cod",
            "X-Container-Meta-Fish": "trout"})

        req = swob.Request.blank("/v1/AUTH_test/a-container",
                                 environ={"REQUEST_METHOD": "HEAD"})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '204 No Content')
        self.assertEqual(headers["X-Container-Sysmeta-Fish"], "cod")
        self.assertEqual(headers["X-Container-Meta-Fish"], "trout")

    def test_not_found(self):
        def mock_RpcHead(head_container_req):
            self.assertEqual(head_container_req['VirtPath'],
                             '/v1/AUTH_test/a-container')
            return {"error": "errno: 2", "result": None}

        self.fake_rpc.register_handler(
            "Server.RpcHead", mock_RpcHead)

        req = swob.Request.blank("/v1/AUTH_test/a-container",
                                 environ={"REQUEST_METHOD": "HEAD"})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '404 Not Found')

    def test_other_error(self):
        def mock_RpcHead(head_container_req):
            self.assertEqual(head_container_req['VirtPath'],
                             '/v1/AUTH_test/a-container')
            return {"error": "errno: 7581", "result": None}

        self.fake_rpc.register_handler(
            "Server.RpcHead", mock_RpcHead)

        req = swob.Request.blank("/v1/AUTH_test/a-container",
                                 environ={"REQUEST_METHOD": "HEAD"})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '500 Internal Error')


class TestContainerGet(BaseMiddlewareTest):
    def setUp(self):
        super(TestContainerGet, self).setUp()
        self.serialized_container_metadata = ""

        # All these tests run against the same container.
        def mock_RpcGetContainer(get_container_req):
            return {
                "error": None,
                "result": {
                    "Metadata": base64.b64encode(
                        self.serialized_container_metadata),
                    "ContainerEntries": [{
                        "Basename": "images",
                        "FileSize": 0,
                        "ModificationTime": 1471915816359209849,
                        "IsDir": True,
                        "InodeNumber": 2489682,
                        "NumWrites": 0,
                    }, {
                        "Basename": "images/avocado.png",
                        "FileSize": 3503770,
                        "ModificationTime": 1471915816859209471,
                        "IsDir": False,
                        "InodeNumber": 9213768,
                        "NumWrites": 2,
                    }, {
                        "Basename": "images/banana.png",
                        "FileSize": 2189865,
                        # has fractional seconds = 0 to cover edge cases
                        "ModificationTime": 1471915873000000000,
                        "IsDir": False,
                        "InodeNumber": 8878410,
                        "NumWrites": 2,
                    }, {
                        "Basename": "images/cherimoya.png",
                        "FileSize": 1636662,
                        "ModificationTime": 1471915917767421311,
                        "IsDir": False,
                        "InodeNumber": 8879064,
                        "NumWrites": 2,
                    }, {
                        "Basename": "images/durian.png",
                        "FileSize": 8414281,
                        "ModificationTime": 1471985233074909930,
                        "IsDir": False,
                        "InodeNumber": 5807979,
                        "NumWrites": 3,
                    }, {
                        "Basename": "images/elderberry.png",
                        "FileSize": 3178293,
                        "ModificationTime": 1471985240833932653,
                        "IsDir": False,
                        "InodeNumber": 4974021,
                        "NumWrites": 1,
                    }]}}

        self.fake_rpc.register_handler(
            "Server.RpcGetContainer", mock_RpcGetContainer)

    def test_text(self):
        req = swob.Request.blank('/v1/AUTH_test/a-container')
        status, headers, body = self.call_pfs(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers["Content-Type"], "text/plain; charset=utf-8")
        self.assertEqual(body, ("images\n"
                                "images/avocado.png\n"
                                "images/banana.png\n"
                                "images/cherimoya.png\n"
                                "images/durian.png\n"
                                "images/elderberry.png\n"))
        self.assertEqual(self.fake_rpc.calls[1][1][0]['VirtPath'],
                         '/v1/AUTH_test/a-container')

    def test_metadata(self):
        self.serialized_container_metadata = json.dumps({
            "X-Container-Sysmeta-Fish": "tilefish",
            "X-Container-Meta-Fish": "haddock"})
        req = swob.Request.blank('/v1/AUTH_test/a-container')
        status, headers, body = self.call_pfs(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers["X-Container-Sysmeta-Fish"], "tilefish")
        self.assertEqual(headers["X-Container-Meta-Fish"], "haddock")

    def test_bogus_metadata(self):
        self.serialized_container_metadata = "{<xml?"
        req = swob.Request.blank('/v1/AUTH_test/a-container')
        status, headers, body = self.call_pfs(req)

        self.assertEqual(status, '200 OK')

    def test_json(self):
        req = swob.Request.blank('/v1/AUTH_test/a-container',
                                 headers={"Accept": "application/json"})
        status, headers, body = self.call_pfs(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers["Content-Type"],
                         "application/json; charset=utf-8")
        resp_data = json.loads(body)
        self.assertIsInstance(resp_data, list)
        self.assertEqual(len(resp_data), 6)
        self.assertEqual(resp_data[0], {
            "name": "images",
            "bytes": 0,
            "content_type": "application/directory",
            "hash": mware.construct_etag(
                "AUTH_test", 2489682, 0),
            "last_modified": "2016-08-23T01:30:16.359210"})
        self.assertEqual(resp_data[1], {
            "name": "images/avocado.png",
            "bytes": 3503770,
            "content_type": "image/png",
            "hash": mware.construct_etag(
                "AUTH_test", 9213768, 2),
            "last_modified": "2016-08-23T01:30:16.859210"})
        self.assertEqual(resp_data[2], {
            "name": "images/banana.png",
            "bytes": 2189865,
            "content_type": "image/png",
            "hash": mware.construct_etag(
                "AUTH_test", 8878410, 2),
            "last_modified": "2016-08-23T01:31:13.000000"})
        self.assertEqual(resp_data[3], {
            "name": "images/cherimoya.png",
            "bytes": 1636662,
            "content_type": "image/png",
            "hash": mware.construct_etag(
                "AUTH_test", 8879064, 2),
            "last_modified": "2016-08-23T01:31:57.767421"})
        self.assertEqual(resp_data[4], {
            "name": "images/durian.png",
            "bytes": 8414281,
            "content_type": "image/png",
            "hash": mware.construct_etag(
                "AUTH_test", 5807979, 3),
            "last_modified": "2016-08-23T20:47:13.074910"})
        self.assertEqual(resp_data[5], {
            "name": "images/elderberry.png",
            "bytes": 3178293,
            "content_type": "image/png",
            "hash": mware.construct_etag(
                "AUTH_test", 4974021, 1),
            "last_modified": "2016-08-23T20:47:20.833933"})

    def test_json_query_param(self):
        req = swob.Request.blank('/v1/AUTH_test/a-container?format=json')
        status, headers, body = self.call_pfs(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers["Content-Type"],
                         "application/json; charset=utf-8")
        json.loads(body)  # doesn't crash

    def test_xml(self):
        req = swob.Request.blank('/v1/AUTH_test/a-container',
                                 headers={"Accept": "text/xml"})
        status, headers, body = self.call_pfs(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers["Content-Type"],
                         "text/xml; charset=utf-8")
        self.assertTrue(body.startswith(
            """<?xml version='1.0' encoding='utf-8'?>"""))

        root_node = ElementTree.fromstring(body)
        self.assertEqual(root_node.tag, 'container')
        self.assertEqual(root_node.attrib["name"], 'a-container')

        objects = root_node.getchildren()
        self.assertEqual(6, len(objects))
        self.assertEqual(objects[0].tag, 'object')

        # The XML container listing doesn't use XML attributes for data, but
        # rather a sequence of tags like <name>X</name> <hash>Y</hash> ...
        #
        # We do an exhaustive check of one object's attributes, then
        # spot-check the rest of the listing for brevity's sake.
        obj_attr_tags = objects[1].getchildren()
        self.assertEqual(len(obj_attr_tags), 5)

        name_node = obj_attr_tags[0]
        self.assertEqual(name_node.tag, 'name')
        self.assertEqual(name_node.text, 'images/avocado.png')
        self.assertEqual(name_node.attrib, {})  # nothing extra in there

        hash_node = obj_attr_tags[1]
        self.assertEqual(hash_node.tag, 'hash')
        self.assertEqual(hash_node.text, mware.construct_etag(
            "AUTH_test", 9213768, 2))
        self.assertEqual(hash_node.attrib, {})

        bytes_node = obj_attr_tags[2]
        self.assertEqual(bytes_node.tag, 'bytes')
        self.assertEqual(bytes_node.text, '3503770')
        self.assertEqual(bytes_node.attrib, {})

        content_type_node = obj_attr_tags[3]
        self.assertEqual(content_type_node.tag, 'content_type')
        self.assertEqual(content_type_node.text, 'image/png')
        self.assertEqual(content_type_node.attrib, {})

        last_modified_node = obj_attr_tags[4]
        self.assertEqual(last_modified_node.tag, 'last_modified')
        self.assertEqual(last_modified_node.text, '2016-08-23T01:30:16.859210')
        self.assertEqual(last_modified_node.attrib, {})

        # Make sure the directory has the right type
        obj_attr_tags = objects[0].getchildren()
        self.assertEqual(len(obj_attr_tags), 5)

        name_node = obj_attr_tags[0]
        self.assertEqual(name_node.tag, 'name')
        self.assertEqual(name_node.text, 'images')

        content_type_node = obj_attr_tags[3]
        self.assertEqual(content_type_node.tag, 'content_type')
        self.assertEqual(content_type_node.text, 'application/directory')

        # Check the names are correct
        all_names = [tag.getchildren()[0].text for tag in objects]
        self.assertEqual(
            ["images", "images/avocado.png", "images/banana.png",
             "images/cherimoya.png", "images/durian.png",
             "images/elderberry.png"],
            all_names)

    def test_xml_alternate_mime_type(self):
        req = swob.Request.blank('/v1/AUTH_test/a-container',
                                 headers={"Accept": "application/xml"})
        status, headers, body = self.call_pfs(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers["Content-Type"],
                         "application/xml; charset=utf-8")
        self.assertTrue(body.startswith(
            """<?xml version='1.0' encoding='utf-8'?>"""))

    def test_xml_query_param(self):
        req = swob.Request.blank('/v1/AUTH_test/a-container?format=xml')
        status, headers, body = self.call_pfs(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers["Content-Type"],
                         "application/xml; charset=utf-8")
        self.assertTrue(body.startswith(
            """<?xml version='1.0' encoding='utf-8'?>"""))

    def test_xml_special_chars(self):
        req = swob.Request.blank('/v1/AUTH_test/c o n',
                                 headers={"Accept": "text/xml"})
        status, headers, body = self.call_pfs(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers["Content-Type"],
                         "text/xml; charset=utf-8")
        self.assertTrue(body.startswith(
            """<?xml version='1.0' encoding='utf-8'?>"""))

        root_node = ElementTree.fromstring(body)
        self.assertEqual(root_node.tag, 'container')
        self.assertEqual(root_node.attrib["name"], 'c o n')
        self.assertEqual(self.fake_rpc.calls[1][1][0]['VirtPath'],
                         '/v1/AUTH_test/c o n')

    def test_marker(self):
        req = swob.Request.blank('/v1/AUTH_test/a-container?marker=sharpie')
        status, _, _ = self.call_pfs(req)
        self.assertEqual(status, '200 OK')

        rpc_calls = self.fake_rpc.calls
        self.assertEqual(len(rpc_calls), 2)
        # rpc_calls[0] is a call to RpcIsAccountBimodal, which is not
        # relevant to what we're testing here
        rpc_method, rpc_args = rpc_calls[1]
        # sanity check
        self.assertEqual(rpc_method, "Server.RpcGetContainer")
        self.assertEqual(rpc_args[0]["Marker"], "sharpie")

    def test_prefix(self):
        req = swob.Request.blank('/v1/AUTH_test/a-container?prefix=cow')
        status, _, _ = self.call_pfs(req)
        self.assertEqual(status, '200 OK')

        rpc_calls = self.fake_rpc.calls
        self.assertEqual(len(rpc_calls), 2)
        # rpc_calls[0] is a call to RpcIsAccountBimodal, which is not
        # relevant to what we're testing here
        rpc_method, rpc_args = rpc_calls[1]
        # sanity check
        self.assertEqual(rpc_method, "Server.RpcGetContainer")
        self.assertEqual(rpc_args[0]["Prefix"], "cow")

    def test_default_limit(self):
        req = swob.Request.blank('/v1/AUTH_test/a-container')
        status, _, _ = self.call_pfs(req)
        self.assertEqual(status, '200 OK')

        rpc_calls = self.fake_rpc.calls
        self.assertEqual(len(rpc_calls), 2)
        # rpc_calls[0] is a call to RpcIsAccountBimodal, which is not
        # relevant to what we're testing here
        rpc_method, rpc_args = rpc_calls[1]
        self.assertEqual(rpc_args[0]["MaxEntries"], 6543)

    def test_valid_user_supplied_limit(self):
        req = swob.Request.blank('/v1/AUTH_test/a-container?limit=150')
        status, _, _ = self.call_pfs(req)
        self.assertEqual(status, '200 OK')

        rpc_calls = self.fake_rpc.calls
        self.assertEqual(len(rpc_calls), 2)
        # rpc_calls[0] is a call to RpcIsAccountBimodal, which is not
        # relevant to what we're testing here
        rpc_method, rpc_args = rpc_calls[1]
        self.assertEqual(rpc_args[0]["MaxEntries"], 150)

    def test_zero_supplied_limit(self):
        req = swob.Request.blank('/v1/AUTH_test/a-container?limit=0')
        status, _, _ = self.call_pfs(req)
        self.assertEqual(status, '200 OK')

        rpc_calls = self.fake_rpc.calls
        self.assertEqual(len(rpc_calls), 2)

        # rpc_calls[0] is a call to RpcIsAccountBimodal, which is not
        # relevant to what we're testing here
        rpc_method, rpc_args = rpc_calls[1]
        self.assertEqual(rpc_args[0]["MaxEntries"], 0)

    def test_negative_supplied_limit(self):
        req = swob.Request.blank('/v1/AUTH_test/a-container?limit=-1')
        status, _, _ = self.call_pfs(req)
        self.assertEqual(status, '200 OK')

        rpc_calls = self.fake_rpc.calls
        self.assertEqual(len(rpc_calls), 2)

        # rpc_calls[0] is a call to RpcIsAccountBimodal, which is not
        # relevant to what we're testing here
        rpc_method, rpc_args = rpc_calls[1]
        self.assertEqual(rpc_args[0]["MaxEntries"], 6543)  # default value

    def test_overlarge_supplied_limits(self):
        req = swob.Request.blank('/v1/AUTH_test/a-container?limit=6544')
        status, _, _ = self.call_pfs(req)
        self.assertEqual(status, '412 Precondition Failed')

        rpc_calls = self.fake_rpc.calls
        # rpc_calls[0] is a call to RpcIsAccountBimodal, which is not
        # relevant to what we're testing here
        self.assertEqual(len(rpc_calls), 1)

    def test_bogus_user_supplied_limit(self):
        req = swob.Request.blank('/v1/AUTH_test/a-container?limit=chihuahua')
        status, _, _ = self.call_pfs(req)
        self.assertEqual(status, '200 OK')

        rpc_calls = self.fake_rpc.calls
        self.assertEqual(len(rpc_calls), 2)
        # rpc_calls[0] is a call to RpcIsAccountBimodal, which is not
        # relevant to what we're testing here
        rpc_method, rpc_args = rpc_calls[1]
        self.assertEqual(rpc_args[0]["MaxEntries"], 6543)  # default value

    def test_default_limit_matches_proxy_server(self):
        req = swob.Request.blank('/v1/AUTH_test/a-container')
        status, _, _ = self.call_pfs(req)
        self.assertEqual(status, '200 OK')

        rpc_calls = self.fake_rpc.calls
        self.assertEqual(len(rpc_calls), 2)
        # rpc_calls[0] is a call to RpcIsAccountBimodal, which is not
        # relevant to what we're testing here
        rpc_method, rpc_args = rpc_calls[1]
        self.assertEqual(rpc_method, "Server.RpcGetContainer")
        self.assertEqual(rpc_args[0]["MaxEntries"], 6543)  # default value

    def test_not_found(self):
        def mock_RpcGetContainer_error(get_container_req):
            self.assertEqual(get_container_req['VirtPath'],
                             '/v1/AUTH_test/a-container')

            return {"error": "errno: 2", "result": None}

        self.fake_rpc.register_handler(
            "Server.RpcGetContainer", mock_RpcGetContainer_error)

        req = swob.Request.blank('/v1/AUTH_test/a-container')
        status, _, _ = self.call_pfs(req)
        self.assertEqual(status, '404 Not Found')

    def test_other_error(self):
        def mock_RpcGetContainer_error(get_container_req):
            self.assertEqual(get_container_req['VirtPath'],
                             '/v1/AUTH_test/a-container')

            return {"error": "errno: 1661", "result": None}

        self.fake_rpc.register_handler(
            "Server.RpcGetContainer", mock_RpcGetContainer_error)

        req = swob.Request.blank('/v1/AUTH_test/a-container')
        status, _, _ = self.call_pfs(req)
        self.assertEqual(status, '500 Internal Error')


class TestContainerPost(BaseMiddlewareTest):
    def test_missing_container(self):
        def mock_RpcHead(_):
            return {"error": "errno: 2", "result": None}

        self.fake_rpc.register_handler(
            "Server.RpcHead", mock_RpcHead)

        req = swob.Request.blank(
            "/v1/AUTH_test/new-con",
            environ={"REQUEST_METHOD": "POST"},
            headers={"X-Container-Meta-One-Fish": "two fish"})
        status, _, _ = self.call_pfs(req)
        self.assertEqual("404 Not Found", status)

        rpc_calls = self.fake_rpc.calls
        self.assertEqual(2, len(rpc_calls))

        method, args = rpc_calls[0]
        self.assertEqual(method, "Server.RpcIsAccountBimodal")
        self.assertEqual(args[0]["AccountName"], "AUTH_test")

        method, args = rpc_calls[1]
        self.assertEqual(method, "Server.RpcHead")
        self.assertEqual(args[0]["VirtPath"], "/v1/AUTH_test/new-con")

    def test_existing_container(self):
        old_meta = json.dumps({
            "X-Container-Read": "xcr",
            "X-Container-Meta-One-Fish": "no fish"})

        def mock_RpcHead(_):
            return {
                "error": None,
                "result": {
                    "Metadata": base64.b64encode(old_meta),
                    "ModificationTime": 1482280565956671142,
                    "FileSize": 0,
                    "IsDir": True,
                    "InodeNumber": 6515443,
                    "NumWrites": 0}}

        self.fake_rpc.register_handler(
            "Server.RpcHead", mock_RpcHead)

        self.fake_rpc.register_handler(
            "Server.RpcPost", lambda *a: {"error": None, "result": {}})

        req = swob.Request.blank(
            "/v1/AUTH_test/new-con",
            environ={"REQUEST_METHOD": "POST"},
            headers={"X-Container-Meta-One-Fish": "two fish"})
        status, _, _ = self.call_pfs(req)
        self.assertEqual("204 No Content", status)

        rpc_calls = self.fake_rpc.calls
        self.assertEqual(3, len(rpc_calls))

        method, args = rpc_calls[0]
        self.assertEqual(method, "Server.RpcIsAccountBimodal")
        self.assertEqual(args[0]["AccountName"], "AUTH_test")

        method, args = rpc_calls[1]
        self.assertEqual(method, "Server.RpcHead")
        self.assertEqual(args[0]["VirtPath"], "/v1/AUTH_test/new-con")

        method, args = rpc_calls[2]
        self.assertEqual(method, "Server.RpcPost")
        self.assertEqual(args[0]["VirtPath"], "/v1/AUTH_test/new-con")
        self.assertEqual(base64.b64decode(args[0]["OldMetaData"]), old_meta)
        new_meta = json.loads(base64.b64decode(args[0]["NewMetaData"]))
        self.assertEqual(new_meta["X-Container-Meta-One-Fish"], "two fish")
        self.assertEqual(new_meta["X-Container-Read"], "xcr")


class TestContainerPut(BaseMiddlewareTest):
    def setUp(self):
        super(TestContainerPut, self).setUp()

        # This only returns success/failure, not any interesting data
        def mock_RpcPutContainer(_):
            return {"error": None, "result": {}}

        self.fake_rpc.register_handler(
            "Server.RpcPutContainer", mock_RpcPutContainer)

    def test_new_container(self):
        def mock_RpcHead(_):
            return {"error": "errno: 2", "result": None}

        self.fake_rpc.register_handler(
            "Server.RpcHead", mock_RpcHead)

        req = swob.Request.blank(
            "/v1/AUTH_test/new-con",
            environ={"REQUEST_METHOD": "PUT"},
            headers={"X-Container-Meta-Red-Fish": "blue fish"})
        status, _, _ = self.call_pfs(req)
        self.assertEqual("201 Created", status)

        rpc_calls = self.fake_rpc.calls
        self.assertEqual(3, len(rpc_calls))

        method, args = rpc_calls[0]
        self.assertEqual(method, "Server.RpcIsAccountBimodal")
        self.assertEqual(args[0]["AccountName"], "AUTH_test")

        method, args = rpc_calls[1]
        self.assertEqual(method, "Server.RpcHead")
        self.assertEqual(args[0]["VirtPath"], "/v1/AUTH_test/new-con")

        method, args = rpc_calls[2]
        self.assertEqual(method, "Server.RpcPutContainer")
        self.assertEqual(args[0]["VirtPath"], "/v1/AUTH_test/new-con")
        self.assertEqual(args[0]["OldMetadata"], "")
        self.assertEqual(
            base64.b64decode(args[0]["NewMetadata"]),
            json.dumps({"X-Container-Meta-Red-Fish": "blue fish"}))

    def test_existing_container(self):
        old_meta = json.dumps({
            "X-Container-Read": "xcr",
            "X-Container-Write": "xcw",
            "X-Container-Meta-Red-Fish": "dead fish"})

        def mock_RpcHead(_):
            return {
                "error": None,
                "result": {
                    "Metadata": base64.b64encode(old_meta),
                    "ModificationTime": 1482270529646747881,
                    "FileSize": 0,
                    "IsDir": True,
                    "InodeNumber": 8054914,
                    "NumWrites": 0}}

        self.fake_rpc.register_handler(
            "Server.RpcHead", mock_RpcHead)

        req = swob.Request.blank(
            "/v1/AUTH_test/new-con",
            environ={"REQUEST_METHOD": "PUT"},
            headers={"X-Container-Meta-Red-Fish": "blue fish"})
        status, _, _ = self.call_pfs(req)
        self.assertEqual("202 Accepted", status)

        rpc_calls = self.fake_rpc.calls
        self.assertEqual(3, len(rpc_calls))

        method, args = rpc_calls[0]
        self.assertEqual(method, "Server.RpcIsAccountBimodal")
        self.assertEqual(args[0]["AccountName"], "AUTH_test")

        method, args = rpc_calls[1]
        self.assertEqual(method, "Server.RpcHead")
        self.assertEqual(args[0]["VirtPath"], "/v1/AUTH_test/new-con")

        method, args = rpc_calls[2]
        self.assertEqual(method, "Server.RpcPutContainer")
        self.assertEqual(args[0]["VirtPath"], "/v1/AUTH_test/new-con")
        self.assertEqual(base64.b64decode(args[0]["OldMetadata"]), old_meta)
        new_meta = json.loads(base64.b64decode(args[0]["NewMetadata"]))
        self.assertEqual(new_meta["X-Container-Meta-Red-Fish"], "blue fish")
        self.assertEqual(new_meta["X-Container-Read"], "xcr")
        self.assertEqual(new_meta["X-Container-Write"], "xcw")


class TestContainerDelete(BaseMiddlewareTest):
    def test_success(self):
        def mock_RpcDelete(_):
            return {"error": None, "result": {}}

        self.fake_rpc.register_handler(
            "Server.RpcDelete", mock_RpcDelete)

        req = swob.Request.blank("/v1/AUTH_test/empty-con",
                                 environ={"REQUEST_METHOD": "DELETE"})
        status, _, _ = self.call_pfs(req)
        self.assertEqual("204 No Content", status)

        self.assertEqual(2, len(self.fake_rpc.calls))
        self.assertEqual("/v1/AUTH_test/empty-con",
                         self.fake_rpc.calls[1][1][0]["VirtPath"])

    def test_special_chars(self):
        def mock_RpcDelete(_):
            return {"error": None, "result": {}}

        self.fake_rpc.register_handler(
            "Server.RpcDelete", mock_RpcDelete)

        req = swob.Request.blank("/v1/AUTH_test/e m p t y",
                                 environ={"REQUEST_METHOD": "DELETE"})
        status, _, _ = self.call_pfs(req)
        self.assertEqual("204 No Content", status)
        self.assertEqual("/v1/AUTH_test/e m p t y",
                         self.fake_rpc.calls[1][1][0]["VirtPath"])

    def test_not_found(self):
        def mock_RpcDelete(_):
            return {"error": "errno: 2", "result": None}

        self.fake_rpc.register_handler(
            "Server.RpcDelete", mock_RpcDelete)

        req = swob.Request.blank("/v1/AUTH_test/empty-con",
                                 environ={"REQUEST_METHOD": "DELETE"})
        status, _, _ = self.call_pfs(req)
        self.assertEqual("404 Not Found", status)

    def test_not_empty(self):
        def mock_RpcDelete(_):
            return {"error": "errno: 39", "result": None}

        self.fake_rpc.register_handler(
            "Server.RpcDelete", mock_RpcDelete)

        req = swob.Request.blank("/v1/AUTH_test/empty-con",
                                 environ={"REQUEST_METHOD": "DELETE"})
        status, _, _ = self.call_pfs(req)
        self.assertEqual("409 Conflict", status)

    def test_other_error(self):
        def mock_RpcDelete(_):
            return {"error": "errno: 987654321", "result": None}

        self.fake_rpc.register_handler(
            "Server.RpcDelete", mock_RpcDelete)

        req = swob.Request.blank("/v1/AUTH_test/empty-con",
                                 environ={"REQUEST_METHOD": "DELETE"})
        status, _, _ = self.call_pfs(req)
        self.assertEqual("500 Internal Error", status)


class TestObjectPut(BaseMiddlewareTest):
    def setUp(self):
        super(TestObjectPut, self).setUp()

        # These mocks act as though everything was successful. Failure tests
        # can override the relevant mocks in the individual test cases.
        def mock_RpcHead(head_container_req):
            # Empty container, but it exists. That's enough for testing
            # object PUT.
            return {
                "error": None,
                "result": {
                    "Metadata": "",
                    "ModificationTime": 14792389930244718933,
                    "FileSize": 0,
                    "IsDir": True,
                    "InodeNumber": 1828,
                    "NumWrites": 893,
                }}

        put_loc_count = collections.defaultdict(int)

        def mock_RpcPutLocation(put_location_req):
            # Give a different sequence of physical paths for each object
            # name
            virt_path = put_location_req["VirtPath"]
            obj_name = hashlib.sha1(virt_path).hexdigest().upper()
            phys_path = "/v1/AUTH_test/PhysContainer_1/" + obj_name
            if put_loc_count[virt_path] > 0:
                phys_path += "-%02x" % put_loc_count[virt_path]
            put_loc_count[virt_path] += 1

            # Someone's probably about to PUT an object there, so let's set
            # up the mock to allow it. Doing it here ensures that the
            # location comes out of this RPC and nowhere else.

            self.app.register('PUT', phys_path, 201, {}, "")

            return {
                "error": None,
                "result": {"PhysPath": phys_path}}

        def mock_RpcPutComplete(put_complete_req):
            return {"error": None, "result": {
                "ModificationTime": 12345,
                "InodeNumber": 678,
                "NumWrites": 9}}

        self.fake_rpc.register_handler(
            "Server.RpcHead", mock_RpcHead)
        self.fake_rpc.register_handler(
            "Server.RpcPutLocation", mock_RpcPutLocation)
        self.fake_rpc.register_handler(
            "Server.RpcPutComplete", mock_RpcPutComplete)

    def test_basic(self):
        wsgi_input = StringIO("sparkleberry-displeasurably")

        req = swob.Request.blank("/v1/AUTH_test/a-container/an-object",
                                 environ={"REQUEST_METHOD": "PUT",
                                          "wsgi.input": wsgi_input})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '201 Created')
        self.assertEqual(headers["ETag"],
                         mware.construct_etag("AUTH_test", 678, 9))

        rpc_calls = self.fake_rpc.calls
        self.assertEqual(len(rpc_calls), 4)

        method, args = rpc_calls[0]
        self.assertEqual(method, "Server.RpcIsAccountBimodal")
        self.assertEqual(args[0]["AccountName"], "AUTH_test")

        method, args = rpc_calls[1]
        self.assertEqual(method, "Server.RpcHead")
        self.assertEqual(args[0]["VirtPath"], "/v1/AUTH_test/a-container")

        method, args = rpc_calls[2]
        self.assertEqual(method, "Server.RpcPutLocation")
        self.assertEqual(args[0]["VirtPath"],
                         "/v1/AUTH_test/a-container/an-object")

        method, args = rpc_calls[3]
        expected_phys_path = ("/v1/AUTH_test/PhysContainer_1/"
                              "80D184B041B9BF0C2EE8D55D8DC9797BF7129E13")
        self.assertEqual(method, "Server.RpcPutComplete")
        self.assertEqual(args[0]["VirtPath"],
                         "/v1/AUTH_test/a-container/an-object")
        self.assertEqual(args[0]["PhysPaths"], [expected_phys_path])
        self.assertEqual(args[0]["PhysLengths"], [len(wsgi_input.getvalue())])

    def test_modification_time(self):
        def mock_RpcPutComplete(put_complete_req):
            return {"error": None, "result": {
                "ModificationTime": 1481311245635845000,
                "InodeNumber": 4116394,
                "NumWrites": 1}}

        self.fake_rpc.register_handler(
            "Server.RpcPutComplete", mock_RpcPutComplete)

        wsgi_input = StringIO("Rhodothece-cholesterinuria")

        req = swob.Request.blank("/v1/AUTH_test/a-container/an-object",
                                 environ={"REQUEST_METHOD": "PUT",
                                          "wsgi.input": wsgi_input})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, "201 Created")
        self.assertEqual(headers["Last-Modified"],
                         "Fri, 09 Dec 2016 19:20:46 GMT")

    def test_special_chars(self):
        wsgi_input = StringIO("pancreas-mystagogically")

        req = swob.Request.blank("/v1/AUTH_test/c o n/o b j",
                                 environ={"REQUEST_METHOD": "PUT",
                                          "wsgi.input": wsgi_input})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '201 Created')

        rpc_calls = self.fake_rpc.calls
        self.assertEqual(len(rpc_calls), 4)

        method, args = rpc_calls[0]
        self.assertEqual(method, "Server.RpcIsAccountBimodal")
        self.assertEqual(args[0]["AccountName"], "AUTH_test")

        method, args = rpc_calls[1]
        self.assertEqual(method, "Server.RpcHead")
        self.assertEqual(args[0]["VirtPath"], "/v1/AUTH_test/c o n")

        method, args = rpc_calls[2]
        self.assertEqual(method, "Server.RpcPutLocation")
        self.assertEqual(args[0]["VirtPath"],
                         "/v1/AUTH_test/c o n/o b j")

        method, args = rpc_calls[3]
        self.assertEqual(method, "Server.RpcPutComplete")
        self.assertEqual(args[0]["VirtPath"],
                         "/v1/AUTH_test/c o n/o b j")

    def test_big(self):
        wsgi_input = StringIO('A' * 100 + 'B' * 100 + 'C' * 75)
        self.pfs.max_log_segment_size = 100

        req = swob.Request.blank("/v1/AUTH_test/con/obj",
                                 environ={"REQUEST_METHOD": "PUT",
                                          "wsgi.input": wsgi_input},
                                 headers={"X-Trans-Id": "big-txid",
                                          "Content-Length": "275"})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '201 Created')
        self.assertEqual(headers["Content-Type"], "application/octet-stream")

        rpc_calls = self.fake_rpc.calls
        self.assertEqual(len(rpc_calls), 6)

        method, args = rpc_calls[0]
        self.assertEqual(method, "Server.RpcIsAccountBimodal")
        self.assertEqual(args[0]["AccountName"], "AUTH_test")

        method, args = rpc_calls[1]
        self.assertEqual(method, "Server.RpcHead")
        self.assertEqual(args[0]["VirtPath"], "/v1/AUTH_test/con")

        # 3 calls to RpcPutLocation since this was spread across 3 log
        # segments
        method, args = rpc_calls[2]
        self.assertEqual(method, "Server.RpcPutLocation")
        self.assertEqual(args[0]["VirtPath"],
                         "/v1/AUTH_test/con/obj")

        method, args = rpc_calls[3]
        self.assertEqual(method, "Server.RpcPutLocation")
        self.assertEqual(args[0]["VirtPath"],
                         "/v1/AUTH_test/con/obj")

        method, args = rpc_calls[4]
        self.assertEqual(method, "Server.RpcPutLocation")
        self.assertEqual(args[0]["VirtPath"],
                         "/v1/AUTH_test/con/obj")

        method, args = rpc_calls[5]
        self.assertEqual(method, "Server.RpcPutComplete")
        self.assertEqual(args[0]["VirtPath"],
                         "/v1/AUTH_test/con/obj")
        pre = "/v1/AUTH_test/PhysContainer_1/"
        self.assertEqual(
            args[0]["PhysPaths"],
            [pre + "1550057D8B0039185EB6184C599C940E51953403",
             pre + "1550057D8B0039185EB6184C599C940E51953403-01",
             pre + "1550057D8B0039185EB6184C599C940E51953403-02"])
        self.assertEqual(args[0]["PhysLengths"], [100, 100, 75])

        # check the txids as well
        self.assertEqual(
            "big-txid-000", self.app.calls[1][2]["X-Trans-Id"])  # 1st PUT
        self.assertEqual(
            "big-txid-001", self.app.calls[2][2]["X-Trans-Id"])  # 2nd PUT
        self.assertEqual(
            "big-txid-002", self.app.calls[3][2]["X-Trans-Id"])  # 3rd PUT

        # If we sent the original Content-Length, the first PUT would fail.
        # At some point, we should send the correct Content-Length value
        # when we can compute it, but for now, we just send nothing.
        self.assertNotIn("Content-Length", self.app.calls[1][2])  # 1st PUT
        self.assertNotIn("Content-Length", self.app.calls[2][2])  # 2nd PUT
        self.assertNotIn("Content-Length", self.app.calls[3][2])  # 3rd PUT

    def test_big_exact_multiple(self):
        wsgi_input = StringIO('A' * 100 + 'B' * 100)
        self.pfs.max_log_segment_size = 100

        req = swob.Request.blank("/v1/AUTH_test/con/obj",
                                 environ={"REQUEST_METHOD": "PUT",
                                          "wsgi.input": wsgi_input},
                                 headers={"X-Trans-Id": "big-txid"})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '201 Created')
        self.assertEqual(headers["Content-Type"], "application/octet-stream")

        rpc_calls = self.fake_rpc.calls
        self.assertEqual(len(rpc_calls), 5)

        method, args = rpc_calls[0]
        self.assertEqual(method, "Server.RpcIsAccountBimodal")
        self.assertEqual(args[0]["AccountName"], "AUTH_test")

        method, args = rpc_calls[1]
        self.assertEqual(method, "Server.RpcHead")
        self.assertEqual(args[0]["VirtPath"], "/v1/AUTH_test/con")

        # 2 calls to RpcPutLocation since this was spread across 2 log
        # segments. We didn't make a 0-length log segment and try to splice
        # that in.
        method, args = rpc_calls[2]
        self.assertEqual(method, "Server.RpcPutLocation")
        self.assertEqual(args[0]["VirtPath"],
                         "/v1/AUTH_test/con/obj")

        method, args = rpc_calls[3]
        self.assertEqual(method, "Server.RpcPutLocation")
        self.assertEqual(args[0]["VirtPath"],
                         "/v1/AUTH_test/con/obj")

        method, args = rpc_calls[4]
        self.assertEqual(method, "Server.RpcPutComplete")
        self.assertEqual(args[0]["VirtPath"],
                         "/v1/AUTH_test/con/obj")
        pre = "/v1/AUTH_test/PhysContainer_1/"
        self.assertEqual(
            args[0]["PhysPaths"],
            [pre + "1550057D8B0039185EB6184C599C940E51953403",
             pre + "1550057D8B0039185EB6184C599C940E51953403-01"])
        self.assertEqual(args[0]["PhysLengths"], [100, 100])

    def test_missing_container(self):
        def mock_RpcHead_not_found(head_container_req):
            # This is what you get for no-such-container.
            return {
                "error": "errno: 2",
                "result": None}

        self.fake_rpc.register_handler(
            "Server.RpcHead", mock_RpcHead_not_found)

        wsgi_input = StringIO("toxicum-brickcroft")

        req = swob.Request.blank("/v1/AUTH_test/a-container/an-object",
                                 environ={"REQUEST_METHOD": "PUT",
                                          "wsgi.input": wsgi_input})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, "404 Not Found")

    def test_metadata(self):
        wsgi_input = StringIO("extranean-paleophysiology")

        headers_in = {
            "X-Object-Meta-Color": "puce",
            "X-Object-Sysmeta-Flavor": "bbq",

            "Content-Disposition": "recycle when no longer needed",
            "Content-Encoding": "quadruple rot13",
            "Content-Type": "application/eggplant",
            # NB: we'll never actually see these two together, but it's fine
            # for this test since we're only looking at which headers get
            # saved and which don't.
            "X-Object-Manifest": "pre/fix",
            "X-Static-Large-Object": "yes",

            # These are not included
            "X-Timestamp": "1473968446.11364",
            "X-Object-Qmeta-Shape": "trapezoidal",
        }

        req = swob.Request.blank("/v1/AUTH_test/a-container/an-object",
                                 headers=headers_in,
                                 environ={"REQUEST_METHOD": "PUT",
                                          "wsgi.input": wsgi_input})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '201 Created')

        serialized_metadata = self.fake_rpc.calls[3][1][0]["Metadata"]
        metadata = json.loads(base64.b64decode(serialized_metadata))
        self.assertEqual(metadata.get("X-Object-Meta-Color"), "puce")
        self.assertEqual(metadata.get("X-Object-Sysmeta-Flavor"), "bbq")
        self.assertEqual(metadata.get("X-Object-Manifest"), "pre/fix")
        self.assertEqual(metadata.get("X-Static-Large-Object"), "yes")
        self.assertEqual(metadata.get("Content-Disposition"),
                         "recycle when no longer needed")
        self.assertEqual(metadata.get("Content-Encoding"), "quadruple rot13")
        self.assertEqual(metadata.get("Content-Type"), "application/eggplant")

    def test_directory_in_the_way(self):
        # If "thing.txt" is a nonempty directory, we get an error that the
        # middleware turns into a 409 Conflict response.
        wsgi_input = StringIO("Celestine-malleal")

        def mock_RpcPutComplete_isdir(head_container_req):
            # This is what you get when there's a nonempty directory in
            # place of your file.
            return {
                "error": "errno: 21",
                "result": None}

        self.fake_rpc.register_handler(
            "Server.RpcPutComplete", mock_RpcPutComplete_isdir)

        req = swob.Request.blank("/v1/AUTH_test/a-container/d1/d2/thing.txt",
                                 environ={"REQUEST_METHOD": "PUT",
                                          "wsgi.input": wsgi_input})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '409 Conflict')

    def test_file_in_the_way(self):
        # If "thing.txt" is a nonempty directory, we get an error that the
        # middleware turns into a 409 Conflict response.
        wsgi_input = StringIO("Celestine-malleal")

        def mock_RpcPutComplete_notdir(head_container_req):
            # This is what you get when there's a file where your path
            # contains a subdirectory.
            return {
                "error": "errno: 20",
                "result": None}

        self.fake_rpc.register_handler(
            "Server.RpcPutComplete", mock_RpcPutComplete_notdir)

        req = swob.Request.blank("/v1/AUTH_test/a-container/a-file/thing.txt",
                                 environ={"REQUEST_METHOD": "PUT",
                                          "wsgi.input": wsgi_input})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '409 Conflict')

    def test_stripping_bad_headers(self):
        # Someday, we'll have to figure out how to expire objects in
        # proxyfs. For now, though, we remove X-Delete-At and X-Delete-After
        # because having a log segment expire will do bad things to our
        # file's integrity.
        #
        # We strip ETag because Swift will treat that as the expected MD5 of
        # the log segment, and if we split across multiple log segments,
        # then any user-supplied ETag will be wrong. Also, this makes
        # POST-as-COPY work despite ProxyFS's ETag values not being MD5
        # checksums.
        wsgi_input = StringIO("extranean-paleophysiology")

        headers_in = {"X-Delete-After": 86400,
                      "ETag": hashlib.md5(wsgi_input.getvalue()).hexdigest()}

        req = swob.Request.blank("/v1/AUTH_test/a-container/an-object",
                                 headers=headers_in,
                                 environ={"REQUEST_METHOD": "PUT",
                                          "wsgi.input": wsgi_input})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '201 Created')

        serialized_metadata = self.fake_rpc.calls[3][1][0]["Metadata"]
        metadata = json.loads(base64.b64decode(serialized_metadata))
        # it didn't get saved in metadata (not that it matters too much)
        self.assertNotIn("X-Delete-At", metadata)
        self.assertNotIn("X-Delete-After", metadata)
        self.assertNotIn("ETag", metadata)

        # it didn't make it to Swift (this is important)
        put_method, put_path, put_headers = self.app.calls[-1]
        self.assertEqual(put_method, 'PUT')  # sanity check
        self.assertNotIn("X-Delete-At", put_headers)
        self.assertNotIn("X-Delete-After", put_headers)
        self.assertNotIn("ETag", put_headers)

    def test_etag_checking(self):
        wsgi_input = StringIO("unsplashed-comprest")
        right_etag = hashlib.md5(wsgi_input.getvalue()).hexdigest()
        wrong_etag = hashlib.md5(wsgi_input.getvalue() + "abc").hexdigest()
        non_checksum_etag = "pfsv1/AUTH_test/2226116/4341333"

        wsgi_input.seek(0)
        req = swob.Request.blank("/v1/AUTH_test/a-container/an-object",
                                 environ={"REQUEST_METHOD": "PUT",
                                          "wsgi.input": wsgi_input},
                                 headers={"ETag": right_etag})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '201 Created')

        wsgi_input.seek(0)
        req = swob.Request.blank("/v1/AUTH_test/a-container/an-object",
                                 environ={"REQUEST_METHOD": "PUT",
                                          "wsgi.input": wsgi_input},
                                 headers={"ETag": wrong_etag})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '422 Unprocessable Entity')

        wsgi_input.seek(0)
        req = swob.Request.blank("/v1/AUTH_test/a-container/an-object",
                                 environ={"REQUEST_METHOD": "PUT",
                                          "wsgi.input": wsgi_input},
                                 headers={"ETag": non_checksum_etag})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '201 Created')


class TestObjectPost(BaseMiddlewareTest):
    def test_missing_object(self):
        def mock_RpcHead(_):
            return {"error": "errno: 2", "result": None}

        self.fake_rpc.register_handler(
            "Server.RpcHead", mock_RpcHead)

        req = swob.Request.blank(
            "/v1/AUTH_test/con/obj",
            environ={"REQUEST_METHOD": "POST"},
            headers={"X-Object-Meta-One-Fish": "two fish"})
        status, _, _ = self.call_pfs(req)
        self.assertEqual("404 Not Found", status)

        rpc_calls = self.fake_rpc.calls
        self.assertEqual(2, len(rpc_calls))

        method, args = rpc_calls[0]
        self.assertEqual(method, "Server.RpcIsAccountBimodal")
        self.assertEqual(args[0]["AccountName"], "AUTH_test")

        method, args = rpc_calls[1]
        self.assertEqual(method, "Server.RpcHead")
        self.assertEqual(args[0]["VirtPath"], "/v1/AUTH_test/con/obj")

    def test_existing_object(self):
        old_meta = json.dumps({
            "Content-Type": "application/fishy",
            "X-Object-Meta-One-Fish": "two fish"})

        def mock_RpcHead(_):
            return {
                "error": None,
                "result": {
                    "Metadata": base64.b64encode(old_meta),
                    "ModificationTime": 1482345542483719281,
                    "FileSize": 551155,
                    "IsDir": False,
                    "InodeNumber": 6519913,
                    "NumWrites": 381}}

        self.fake_rpc.register_handler(
            "Server.RpcHead", mock_RpcHead)

        self.fake_rpc.register_handler(
            "Server.RpcPost", lambda *a: {"error": None, "result": {}})

        req = swob.Request.blank(
            "/v1/AUTH_test/con/obj",
            environ={"REQUEST_METHOD": "POST"},
            headers={"X-Object-Meta-Red-Fish": "blue fish"})
        status, headers, _ = self.call_pfs(req)

        # For reference, the result of a real object POST request. The
        # request contained new metadata items, but notice that those are
        # not reflected in the response.
        #
        # HTTP/1.1 202 Accepted
        # Last-Modified: Wed, 21 Dec 2016 18:46:44 GMT
        # Content-Length: 0
        # Etag: 340d45cdfb0f8be0862deb2cf21cc08c
        # Content-Type: text/html; charset=UTF-8
        # X-Trans-Id: tx4500a321080445a4841c9-00585ace13
        # Date: Wed, 21 Dec 2016 18:46:43 GMT
        self.assertEqual("202 Accepted", status)
        self.assertEqual("Wed, 21 Dec 2016 18:39:03 GMT",
                         headers["Last-Modified"])
        self.assertEqual("0", headers["Content-Length"])
        self.assertEqual('"pfsv1/AUTH_test/00637C69/0000017D"',
                         headers["Etag"])
        self.assertEqual("text/html; charset=UTF-8", headers["Content-Type"])
        # Date and X-Trans-Id are added in by other parts of the WSGI stack

        rpc_calls = self.fake_rpc.calls
        self.assertEqual(3, len(rpc_calls))

        method, args = rpc_calls[0]
        self.assertEqual(method, "Server.RpcIsAccountBimodal")
        self.assertEqual(args[0]["AccountName"], "AUTH_test")

        method, args = rpc_calls[1]
        self.assertEqual(method, "Server.RpcHead")
        self.assertEqual(args[0]["VirtPath"], "/v1/AUTH_test/con/obj")

        method, args = rpc_calls[2]
        self.assertEqual(method, "Server.RpcPost")
        self.assertEqual(args[0]["VirtPath"], "/v1/AUTH_test/con/obj")
        self.assertEqual(base64.b64decode(args[0]["OldMetaData"]), old_meta)
        new_meta = json.loads(base64.b64decode(args[0]["NewMetaData"]))
        # old Content-Type persists, all else is replaced
        self.assertEqual(new_meta["X-Object-Meta-Red-Fish"], "blue fish")
        self.assertEqual(new_meta["Content-Type"], "application/fishy")
        self.assertNotIn("X-Object-Meta-One-Fish", new_meta)

    def test_change_content_type(self):
        old_meta = json.dumps({"Content-Type": "old/type"})

        def mock_RpcHead(_):
            return {
                "error": None,
                "result": {
                    "Metadata": base64.b64encode(old_meta),
                    "ModificationTime": 1482345542483719281,
                    "FileSize": 551155,
                    "IsDir": False,
                    "InodeNumber": 6519913,
                    "NumWrites": 381}}

        self.fake_rpc.register_handler(
            "Server.RpcHead", mock_RpcHead)

        self.fake_rpc.register_handler(
            "Server.RpcPost", lambda *a: {"error": None, "result": {}})

        req = swob.Request.blank(
            "/v1/AUTH_test/con/obj",
            environ={"REQUEST_METHOD": "POST"},
            headers={"Content-Type": "new/type"})
        status, _, _ = self.call_pfs(req)
        self.assertEqual("202 Accepted", status)

        rpc_calls = self.fake_rpc.calls

        method, args = rpc_calls[2]
        self.assertEqual(method, "Server.RpcPost")
        self.assertEqual(args[0]["VirtPath"], "/v1/AUTH_test/con/obj")
        self.assertEqual(base64.b64decode(args[0]["OldMetaData"]), old_meta)
        new_meta = json.loads(base64.b64decode(args[0]["NewMetaData"]))
        self.assertEqual(new_meta["Content-Type"], "new/type")


class TestObjectDelete(BaseMiddlewareTest):
    def test_success(self):
        def fake_RpcDelete(delete_request):
            return {"error": None, "result": {}}

        self.fake_rpc.register_handler("Server.RpcDelete", fake_RpcDelete)

        req = swob.Request.blank("/v1/AUTH_test/con/obj",
                                 environ={"REQUEST_METHOD": "DELETE"})

        status, _, _ = self.call_pfs(req)
        self.assertEqual(status, "204 No Content")

        # the first call is a call to RpcIsAccountBimodal
        self.assertEqual(len(self.fake_rpc.calls), 2)
        self.assertEqual(self.fake_rpc.calls[1][1][0]["VirtPath"],
                         "/v1/AUTH_test/con/obj")

    def test_not_found(self):
        def fake_RpcDelete(delete_request):
            return {"error": "errno: 2",  # NotFoundError / ENOENT
                    "result": None}

        self.fake_rpc.register_handler("Server.RpcDelete", fake_RpcDelete)

        req = swob.Request.blank("/v1/AUTH_test/con/obj",
                                 environ={"REQUEST_METHOD": "DELETE"})

        status, _, _ = self.call_pfs(req)
        self.assertEqual(status, "404 Not Found")

    def test_not_empty(self):
        def fake_RpcDelete(delete_request):
            return {"error": "errno: 39",  # NotEmptyError / ENOTEMPTY
                    "result": None}

        self.fake_rpc.register_handler("Server.RpcDelete", fake_RpcDelete)

        req = swob.Request.blank("/v1/AUTH_test/con/obj",
                                 environ={"REQUEST_METHOD": "DELETE"})

        status, _, _ = self.call_pfs(req)
        self.assertEqual(status, "409 Conflict")

    def test_other_failure(self):
        def fake_RpcDelete(delete_request):
            return {"error": "errno: 9",  # BadFileError / EBADF
                    "result": None}

        self.fake_rpc.register_handler("Server.RpcDelete", fake_RpcDelete)

        req = swob.Request.blank("/v1/AUTH_test/con/obj",
                                 environ={"REQUEST_METHOD": "DELETE"})

        status, _, _ = self.call_pfs(req)
        self.assertEqual(status, "500 Internal Error")


class TestObjectHead(BaseMiddlewareTest):
    def setUp(self):
        super(TestObjectHead, self).setUp()

        self.serialized_object_metadata = ""

        # All these tests run against the same object.
        def mock_RpcHead(head_object_req):
            self.assertEqual(head_object_req['VirtPath'],
                             '/v1/AUTH_test/c/an-object.png')

            if self.serialized_object_metadata:
                md = base64.b64encode(self.serialized_object_metadata)
            else:
                md = ""

            return {
                "error": None,
                "result": {
                    "Metadata": md,
                    "ModificationTime": 1479173168018879490,
                    "FileSize": 2641863,
                    "IsDir": False,
                    "InodeNumber": 4591,
                    "NumWrites": 874,
                }}

        self.fake_rpc.register_handler(
            "Server.RpcHead", mock_RpcHead)

        # For reference: the result of a real HEAD request
        #
        # swift@saio:~/swift$ curl -I -H "X-Auth-Token: $TOKEN" \
        #    http://localhost:8080/v1/AUTH_test/s/tox.ini
        # HTTP/1.1 200 OK
        # Content-Length: 3954
        # Content-Type: application/octet-stream
        # Accept-Ranges: bytes
        # Last-Modified: Mon, 14 Nov 2016 23:29:00 GMT
        # Etag: ceffb3138058597bd7f8a09bdd3865d0
        # X-Timestamp: 1479166139.38308
        # X-Object-Meta-Mtime: 1476487400.000000
        # X-Trans-Id: txeaa367b1809a4583af3c8-00582a4a6c
        # Date: Mon, 14 Nov 2016 23:36:12 GMT
        #

    def test_no_meta(self):
        self.serialized_object_metadata = ""

        req = swob.Request.blank("/v1/AUTH_test/c/an-object.png",
                                 environ={"REQUEST_METHOD": "HEAD"})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '204 No Content')

        self.assertEqual(headers["Content-Length"], "2641863")
        self.assertEqual(headers["Content-Type"], "image/png")
        self.assertEqual(headers["Accept-Ranges"], "bytes")
        self.assertEqual(headers["ETag"],
                         '"pfsv1/AUTH_test/000011EF/0000036A"')
        self.assertEqual(headers["Last-Modified"],
                         "Tue, 15 Nov 2016 01:26:09 GMT")
        self.assertEqual(headers["X-Timestamp"], "1479173168.01888")

    def test_explicit_content_type(self):
        self.serialized_object_metadata = json.dumps({
            "Content-Type": "Pegasus/inartistic"})

        req = swob.Request.blank("/v1/AUTH_test/c/an-object.png",
                                 environ={"REQUEST_METHOD": "HEAD"})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '204 No Content')
        self.assertEqual(headers["Content-Type"], "Pegasus/inartistic")

    def test_bogus_meta(self):
        self.serialized_object_metadata = "{[{[{[{[{[[(((!"

        req = swob.Request.blank("/v1/AUTH_test/c/an-object.png",
                                 environ={"REQUEST_METHOD": "HEAD"})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '204 No Content')

    def test_meta(self):
        self.serialized_object_metadata = json.dumps({
            "X-Object-Sysmeta-Fish": "cod",
            "X-Object-Meta-Fish": "trout"})

        req = swob.Request.blank("/v1/AUTH_test/c/an-object.png",
                                 environ={"REQUEST_METHOD": "HEAD"})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '204 No Content')
        self.assertEqual(headers["X-Object-Sysmeta-Fish"], "cod")
        self.assertEqual(headers["X-Object-Meta-Fish"], "trout")

    def test_none_meta(self):
        # Sometimes we get a null in the response instead of an empty
        # string. No idea why.
        self.serialized_object_metadata = None

        req = swob.Request.blank("/v1/AUTH_test/c/an-object.png",
                                 environ={"REQUEST_METHOD": "HEAD"})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '204 No Content')

    def test_special_chars(self):
        def mock_RpcHead(head_object_req):
            self.assertEqual(head_object_req['VirtPath'],
                             '/v1/AUTH_test/c/a cat.png')
            return {"error": "errno: 2", "result": None}

        self.fake_rpc.register_handler(
            "Server.RpcHead", mock_RpcHead)

        req = swob.Request.blank("/v1/AUTH_test/c/a cat.png",
                                 environ={"REQUEST_METHOD": "HEAD"})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '404 Not Found')

    def test_not_found(self):
        def mock_RpcHead(head_object_req):
            self.assertEqual(head_object_req['VirtPath'],
                             '/v1/AUTH_test/c/an-object.png')
            return {"error": "errno: 2", "result": None}

        self.fake_rpc.register_handler(
            "Server.RpcHead", mock_RpcHead)

        req = swob.Request.blank("/v1/AUTH_test/c/an-object.png",
                                 environ={"REQUEST_METHOD": "HEAD"})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '404 Not Found')

    def test_other_error(self):
        def mock_RpcHead(head_object_req):
            self.assertEqual(head_object_req['VirtPath'],
                             '/v1/AUTH_test/c/an-object.png')
            return {"error": "errno: 7581", "result": None}

        self.fake_rpc.register_handler(
            "Server.RpcHead", mock_RpcHead)

        req = swob.Request.blank("/v1/AUTH_test/c/an-object.png",
                                 environ={"REQUEST_METHOD": "HEAD"})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '500 Internal Error')


class TestObjectHeadDir(BaseMiddlewareTest):
    def test_dir(self):
        def mock_RpcHead(head_object_req):
            self.assertEqual(head_object_req['VirtPath'],
                             '/v1/AUTH_test/c/a-dir')

            return {
                "error": None,
                "result": {
                    "Metadata": "",
                    "ModificationTime": 1479173168018879490,
                    "FileSize": 0,
                    "IsDir": True,
                    "InodeNumber": 1254,
                    "NumWrites": 896,
                }}

        self.fake_rpc.register_handler(
            "Server.RpcHead", mock_RpcHead)
        req = swob.Request.blank("/v1/AUTH_test/c/a-dir",
                                 environ={"REQUEST_METHOD": "HEAD"})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '204 No Content')
        self.assertEqual(headers["Content-Length"], "0")
        self.assertEqual(headers["Content-Type"], "application/directory")


class TestObjectCoalesce(BaseMiddlewareTest):
    def setUp(self):
        super(TestObjectCoalesce, self).setUp()

        def mock_RpcHead(head_container_req):
            return {
                "error": None,
                "result": {
                    "Metadata": "",
                    "ModificationTime": 1485814697697650000,
                    "FileSize": 0,
                    "IsDir": True,
                    "InodeNumber": 1828,
                    "NumWrites": 893,
                }}

        self.fake_rpc.register_handler(
            "Server.RpcHead", mock_RpcHead)

    def test_success(self):
        def mock_RpcCoalesce(coalese_req):
            return {
                "error": None,
                "result": {
                    "ModificationTime": 1488323796002909000,
                    "InodeNumber": 283253,
                    "NumWrites": 6,
                }}

        self.fake_rpc.register_handler(
            "Server.RpcCoalesce", mock_RpcCoalesce)

        request_body = json.dumps({
            "elements": [
                "c1/seg1a",
                "c1/seg1b",
                "c2/seg space 2a",
                "c2/seg space 2b",
                "c3/seg3",
            ]})
        req = swob.Request.blank(
            "/v1/AUTH_test/con/obj",
            environ={"REQUEST_METHOD": "COALESCE",
                     "wsgi.input": StringIO(request_body)})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '201 Created')
        self.assertEqual(headers["Etag"],
                         '"pfsv1/AUTH_test/00045275/00000006"')

        # The first call is a call to RpcIsAccountBimodal, the second is a
        # call to RpcHead, and the third and final call is the one we care
        # about: RpcCoalesce.
        self.assertEqual(len(self.fake_rpc.calls), 2)
        method, args = self.fake_rpc.calls[1]
        self.assertEqual(method, "Server.RpcCoalesce")
        self.assertEqual(args[0]["VirtPath"], "/v1/AUTH_test/con/obj")
        self.assertEqual(args[0]["ElementAccountRelativePaths"], [
            "c1/seg1a",
            "c1/seg1b",
            "c2/seg space 2a",
            "c2/seg space 2b",
            "c3/seg3",
        ])

    def test_malformed_json(self):
        request_body = "{{{[[[((("
        req = swob.Request.blank(
            "/v1/AUTH_test/con/obj",
            environ={"REQUEST_METHOD": "COALESCE",
                     "wsgi.input": StringIO(request_body)})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '400 Bad Request')

    def test_incorrect_json(self):
        request_body = "{}"
        req = swob.Request.blank(
            "/v1/AUTH_test/con/obj",
            environ={"REQUEST_METHOD": "COALESCE",
                     "wsgi.input": StringIO(request_body)})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '400 Bad Request')

    def test_incorrect_json_wrong_type(self):
        request_body = "[]"
        req = swob.Request.blank(
            "/v1/AUTH_test/con/obj",
            environ={"REQUEST_METHOD": "COALESCE",
                     "wsgi.input": StringIO(request_body)})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '400 Bad Request')

    def test_incorrect_json_wrong_elements_type(self):
        request_body = json.dumps({"elements": {1: 2}})
        req = swob.Request.blank(
            "/v1/AUTH_test/con/obj",
            environ={"REQUEST_METHOD": "COALESCE",
                     "wsgi.input": StringIO(request_body)})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '400 Bad Request')

    def test_incorrect_json_wrong_element_type(self):
        request_body = json.dumps({"elements": [1, "two", {}]})
        req = swob.Request.blank(
            "/v1/AUTH_test/con/obj",
            environ={"REQUEST_METHOD": "COALESCE",
                     "wsgi.input": StringIO(request_body)})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '400 Bad Request')

    def test_incorrect_json_subtle(self):
        request_body = '["elements"]'
        req = swob.Request.blank(
            "/v1/AUTH_test/con/obj",
            environ={"REQUEST_METHOD": "COALESCE",
                     "wsgi.input": StringIO(request_body)})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '400 Bad Request')

    def test_too_big(self):
        request_body = '{' * (self.pfs.max_coalesce_request_size + 1)
        req = swob.Request.blank(
            "/v1/AUTH_test/con/obj",
            environ={"REQUEST_METHOD": "COALESCE",
                     "wsgi.input": StringIO(request_body)})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '413 Request Entity Too Large')

    def test_too_many_elements(self):
        request_body = json.dumps({
            "elements": ["/c/o"] * (self.pfs.max_coalesce + 1)})
        req = swob.Request.blank(
            "/v1/AUTH_test/con/obj",
            environ={"REQUEST_METHOD": "COALESCE",
                     "wsgi.input": StringIO(request_body)})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '413 Request Entity Too Large')

    def test_not_found(self):
        # Note: this test covers all sorts of thing-not-found errors, as the
        # Server.RpcCoalesce remote procedure does not distinguish between
        # them. In particular, this covers the case when an element is not
        # found as well as the case when the destination container is not
        # found.
        def mock_RpcCoalesce(coalese_req):
            return {
                "error": "errno: 2",
                "result": None,
            }

        self.fake_rpc.register_handler(
            "Server.RpcCoalesce", mock_RpcCoalesce)

        request_body = json.dumps({
            "elements": [
                "some/stuff",
            ]})
        req = swob.Request.blank(
            "/v1/AUTH_test/con/obj",
            environ={"REQUEST_METHOD": "COALESCE",
                     "wsgi.input": StringIO(request_body)})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '404 Not Found')

    def test_coalesce_directory(self):
        # This happens when one of the named elements is a directory.
        def mock_RpcCoalesce(coalese_req):
            return {
                "error": "errno: 21",
                "result": None,
            }

        self.fake_rpc.register_handler(
            "Server.RpcCoalesce", mock_RpcCoalesce)

        request_body = json.dumps({
            "elements": [
                "some/stuff",
            ]})
        req = swob.Request.blank(
            "/v1/AUTH_test/con/obj",
            environ={"REQUEST_METHOD": "COALESCE",
                     "wsgi.input": StringIO(request_body)})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '409 Conflict')

    def test_coalesce_file_instead_of_dir(self):
        # This happens when one of the named elements is not a regular file.
        # Yes, you get IsDirError for not-a-file, even if it's a symlink.
        def mock_RpcCoalesce(coalese_req):
            return {
                "error": "errno: 20",
                "result": None,
            }

        self.fake_rpc.register_handler(
            "Server.RpcCoalesce", mock_RpcCoalesce)

        request_body = json.dumps({
            "elements": [
                "some/stuff",
            ]})
        req = swob.Request.blank(
            "/v1/AUTH_test/con/obj",
            environ={"REQUEST_METHOD": "COALESCE",
                     "wsgi.input": StringIO(request_body)})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '409 Conflict')

    def test_element_has_multiple_links(self):
        # If a file has multiple links to it (hard links, not symlinks),
        # then we can't coalesce it.
        def mock_RpcCoalesce(coalese_req):
            return {
                "error": "errno: 31",
                "result": None,
            }

        self.fake_rpc.register_handler(
            "Server.RpcCoalesce", mock_RpcCoalesce)

        request_body = json.dumps({
            "elements": [
                "some/stuff",
            ]})
        req = swob.Request.blank(
            "/v1/AUTH_test/con/obj",
            environ={"REQUEST_METHOD": "COALESCE",
                     "wsgi.input": StringIO(request_body)})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '409 Conflict')

    def test_other_error(self):
        # If a file has multiple links to it (hard links, not symlinks),
        # then we can't coalesce it.
        def mock_RpcCoalesce(coalese_req):
            return {
                "error": "errno: 1159268",
                "result": None,
            }

        self.fake_rpc.register_handler(
            "Server.RpcCoalesce", mock_RpcCoalesce)

        request_body = json.dumps({
            "elements": [
                "thing/one",
                "thing/two",
            ]})
        req = swob.Request.blank(
            "/v1/AUTH_test/con/obj",
            environ={"REQUEST_METHOD": "COALESCE",
                     "wsgi.input": StringIO(request_body)})
        status, headers, body = self.call_pfs(req)
        self.assertEqual(status, '500 Internal Error')


class TestAuth(BaseMiddlewareTest):
    def setUp(self):
        super(TestAuth, self).setUp()

        def mock_RpcHead(get_container_req):
            return {
                "error": None,
                "result": {
                    "Metadata": base64.b64encode(json.dumps({
                        "X-Container-Read": "the-x-con-read",
                        "X-Container-Write": "the-x-con-write"})),
                    "ModificationTime": 1479240451156825194,
                    "FileSize": 0,
                    "IsDir": True,
                    "InodeNumber": 1255,
                    "NumWrites": 897,
                }}

        self.fake_rpc.register_handler(
            "Server.RpcHead", mock_RpcHead)

    def test_auth_callback_args(self):
        want_x_container_read = (('GET', '/v1/AUTH_test/con/obj'),
                                 ('HEAD', '/v1/AUTH_test/con/obj'),
                                 ('GET', '/v1/AUTH_test/con'),
                                 ('HEAD', '/v1/AUTH_test/con'))

        want_x_container_write = (('PUT', '/v1/AUTH_test/con/obj'),
                                  ('POST', '/v1/AUTH_test/con/obj'),
                                  ('DELETE', '/v1/AUTH_test/con/obj'))
        got_acls = []

        def capture_acl_and_deny(request):
            got_acls.append(request.acl)
            return swob.HTTPForbidden(request=request)

        for method, path in want_x_container_read:
            req = swob.Request.blank(
                path, environ={'REQUEST_METHOD': method,
                               'swift.authorize': capture_acl_and_deny})
            status, _, _ = self.call_pfs(req)
            self.assertEqual(status, '403 Forbidden')
        self.assertEqual(
            got_acls, ["the-x-con-read"] * len(want_x_container_read))

        del got_acls[:]
        for method, path in want_x_container_write:
            req = swob.Request.blank(
                path, environ={'REQUEST_METHOD': method,
                               'swift.authorize': capture_acl_and_deny})
            status, _, _ = self.call_pfs(req)
            self.assertEqual(status, '403 Forbidden')
        self.assertEqual(
            got_acls, ["the-x-con-write"] * len(want_x_container_write))

    def test_auth_override(self):
        def auth_nope(request):
            return swob.HTTPForbidden(request=request)

        req = swob.Request.blank(
            "/v1/AUTH_test/con",
            environ={'REQUEST_METHOD': 'HEAD',
                     'swift.authorize': auth_nope,
                     'swift.authorize_override': True})
        status, _, _ = self.call_pfs(req)
        self.assertEqual(status, '204 No Content')

    def test_auth_allowed(self):
        def auth_its_fine(request):
            return None

        req = swob.Request.blank(
            "/v1/AUTH_test/con",
            environ={'REQUEST_METHOD': 'HEAD',
                     'swift.authorize': auth_its_fine})
        status, _, _ = self.call_pfs(req)
        self.assertEqual(status, '204 No Content')


class TestRetry(unittest.TestCase):
    def setUp(self):
        self.app = FakeProxy()
        self.pfs = mware.PfsMiddleware(self.app, {
            'bimodal_recheck_interval': '5.0',
            'proxyfsd_host': '10.1.1.1, 10.2.2.2',
        })
        self.fake_rpc = FakeJsonRpcWithErrors()
        patcher = mock.patch('pfs_middleware.middleware.JsonRpcClient',
                             lambda *_: self.fake_rpc)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.app.register('HEAD', '/v1/AUTH_test', 204, {}, '')

        def fake_RpcIsAccountBimodal(request):
            return {
                "error": None,
                "result": {
                    "IsBimodal": True,
                    "ActivePeerPrivateIPAddr": "10.9.8.7",
                }}

        self.fake_rpc.register_handler("Server.RpcIsAccountBimodal",
                                       fake_RpcIsAccountBimodal)

    def test_retry_socketerror(self):
        self.fake_rpc.add_call_error(
            socket.error(errno.ECONNREFUSED, os.strerror(errno.ECONNREFUSED)))

        req = swob.Request.blank(
            "/v1/AUTH_test",
            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.pfs)
        self.assertEqual(resp.status_int, 204)

        self.assertEqual(self.fake_rpc.calls, (
            ('Server.RpcIsAccountBimodal', ({'AccountName': 'AUTH_test'},)),
            ('Server.RpcIsAccountBimodal', ({'AccountName': 'AUTH_test'},))))

    def test_no_retry_timeout(self):
        err = eventlet.Timeout(None)  # don't really time anything out
        self.fake_rpc.add_call_error(err)

        req = swob.Request.blank(
            "/v1/AUTH_test",
            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.pfs)
        self.assertEqual(resp.status_int, 500)

        self.assertEqual(self.fake_rpc.calls, (
            ('Server.RpcIsAccountBimodal', ({'AccountName': 'AUTH_test'},)),))

    def test_no_catch_other_error(self):
        self.fake_rpc.add_call_error(ZeroDivisionError)

        req = swob.Request.blank(
            "/v1/AUTH_test",
            environ={'REQUEST_METHOD': 'HEAD'})

        self.assertRaises(ZeroDivisionError, req.get_response, self.pfs)
        self.assertEqual(self.fake_rpc.calls, (
            ('Server.RpcIsAccountBimodal', ({'AccountName': 'AUTH_test'},)),))


class TestBimodalCaching(unittest.TestCase):
    def setUp(self):
        self.app = FakeProxy()
        self.pfs = mware.PfsMiddleware(self.app, {
            'bimodal_recheck_interval': '5.0',
        })
        self.fake_rpc = FakeJsonRpc()
        patcher = mock.patch('pfs_middleware.middleware.JsonRpcClient',
                             lambda *_: self.fake_rpc)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.app.register('HEAD', '/v1/alice', 204, {}, '')
        self.app.register('HEAD', '/v1/bob', 204, {}, '')
        self.app.register('HEAD', '/v1/carol', 204, {}, '')
        self.app.register('HEAD', '/v1/david', 204, {}, '')

    def test_caching(self):
        the_time = [12345.6]
        rpc_iab_calls = []

        def fake_time_function():
            now = the_time[0]
            the_time[0] += 0.001
            return now

        fake_time_module = mock.Mock(time=fake_time_function)

        def fake_RpcIsAccountBimodal(request):
            acc = request["AccountName"]
            rpc_iab_calls.append(acc)

            if acc == 'bob':
                # Normal, happy bimodal account
                return {
                    "error": None,
                    "result": {
                        "IsBimodal": True,
                        "ActivePeerPrivateIPAddr": "10.221.76.210"}}
            elif acc == 'david':
                # Temporarily in limbo as it's being moved from one proxyfsd
                # to another
                return {
                    "error": None,
                    "result": {
                        "IsBimodal": True,
                        "ActivePeerPrivateIPAddr": ""}}
            else:
                return {
                    "error": None,
                    "result": {
                        "IsBimodal": False,
                        "ActivePeerPrivateIPAddr": ""}}
        self.fake_rpc.register_handler("Server.RpcIsAccountBimodal",
                                       fake_RpcIsAccountBimodal)

        status = [None]

        def start_response(s, h, ei=None):
            status[0] = s

        with mock.patch('pfs_middleware.middleware.time', fake_time_module):
            a_req = swob.Request.blank("/v1/alice",
                                       environ={"REQUEST_METHOD": "HEAD"})
            b_req = swob.Request.blank("/v1/bob",
                                       environ={"REQUEST_METHOD": "HEAD"})
            d_req = swob.Request.blank("/v1/david",
                                       environ={"REQUEST_METHOD": "HEAD"})

            # First time, we have a completely empty cache, so an RPC is made
            list(self.pfs(a_req.environ, start_response))
            self.assertEqual(status[0], '204 No Content')  # sanity check
            self.assertEqual(rpc_iab_calls, ["alice"])

            # A couple seconds later, a second request for the same account
            # comes in, and is handled from cache
            the_time[0] += 2
            del rpc_iab_calls[:]
            list(self.pfs(a_req.environ, start_response))
            self.assertEqual(status[0], '204 No Content')  # sanity check
            self.assertEqual(rpc_iab_calls, [])

            # If a request for another account comes in, it is cached
            # separately.
            del rpc_iab_calls[:]
            list(self.pfs(b_req.environ, start_response))
            self.assertEqual(status[0], '204 No Content')  # sanity check
            self.assertEqual(rpc_iab_calls, ["bob"])

            # Each account has its own cache time
            the_time[0] += 3  # "alice" is now invalid, "bob" remains valid
            del rpc_iab_calls[:]
            list(self.pfs(a_req.environ, start_response))
            list(self.pfs(b_req.environ, start_response))
            self.assertEqual(rpc_iab_calls, ["alice"])

            # In-transit accounts don't get cached
            del rpc_iab_calls[:]
            list(self.pfs(d_req.environ, start_response))
            list(self.pfs(d_req.environ, start_response))
            self.assertEqual(rpc_iab_calls, ["david", "david"])
