# Copyright (c) 2017 SwiftStack, Inc.
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

import errno
import eventlet
import mock
import os
import socket
import unittest

from swift.common import swob
from . import helpers

import pfs_middleware.bimodal_checker as bimodal_checker


class FakeJsonRpcWithErrors(helpers.FakeJsonRpc):
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


class TestBimodalCaching(unittest.TestCase):
    def setUp(self):
        self.app = helpers.FakeProxy()
        self.bc = bimodal_checker.BimodalChecker(self.app, {
            'bimodal_recheck_interval': '5.0',
        })
        self.fake_rpc = helpers.FakeJsonRpc()
        patcher = mock.patch('pfs_middleware.utils.JsonRpcClient',
                             lambda *_: self.fake_rpc)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.app.register('HEAD', '/v1/alice', 204, {}, '')
        self.app.register('HEAD', '/v1/bob', 204, {}, '')
        self.app.register('HEAD', '/v1/carol', 204, {}, '')

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

            if acc == 'alice':
                # Not bimodal
                return {
                    "error": None,
                    "result": {
                        "IsBimodal": False,
                        "ActivePeerPrivateIPAddr": ""}}
            elif acc == 'bob':
                # Normal, happy bimodal account
                return {
                    "error": None,
                    "result": {
                        "IsBimodal": True,
                        "ActivePeerPrivateIPAddr": "10.221.76.210"}}
            elif acc == 'carol':
                # Temporarily in limbo as it's being moved from one proxyfsd
                # to another
                return {
                    "error": None,
                    "result": {
                        "IsBimodal": True,
                        "ActivePeerPrivateIPAddr": ""}}
            else:
                raise ValueError("test helper can't handle %r" % (acc,))
        self.fake_rpc.register_handler("Server.RpcIsAccountBimodal",
                                       fake_RpcIsAccountBimodal)

        status = [None]

        def start_response(s, h, ei=None):
            status[0] = s

        with mock.patch('pfs_middleware.bimodal_checker.time',
                        fake_time_module):
            a_req = swob.Request.blank("/v1/alice",
                                       environ={"REQUEST_METHOD": "HEAD"})
            b_req = swob.Request.blank("/v1/bob",
                                       environ={"REQUEST_METHOD": "HEAD"})
            d_req = swob.Request.blank("/v1/carol",
                                       environ={"REQUEST_METHOD": "HEAD"})

            # First time, we have a completely empty cache, so an RPC is made
            list(self.bc(a_req.environ, start_response))
            self.assertEqual(status[0], '204 No Content')  # sanity check
            self.assertEqual(rpc_iab_calls, ["alice"])

            # A couple seconds later, a second request for the same account
            # comes in, and is handled from cache
            the_time[0] += 2
            del rpc_iab_calls[:]
            list(self.bc(a_req.environ, start_response))
            self.assertEqual(status[0], '204 No Content')  # sanity check
            self.assertEqual(rpc_iab_calls, [])

            # If a request for another account comes in, it is cached
            # separately.
            del rpc_iab_calls[:]
            list(self.bc(b_req.environ, start_response))
            self.assertEqual(status[0], '204 No Content')  # sanity check
            self.assertEqual(rpc_iab_calls, ["bob"])

            # Each account has its own cache time
            the_time[0] += 3  # "alice" is now invalid, "bob" remains valid
            del rpc_iab_calls[:]
            list(self.bc(a_req.environ, start_response))
            list(self.bc(b_req.environ, start_response))
            self.assertEqual(rpc_iab_calls, ["alice"])

            # In-transit accounts don't get cached
            del rpc_iab_calls[:]
            list(self.bc(d_req.environ, start_response))
            list(self.bc(d_req.environ, start_response))
            self.assertEqual(rpc_iab_calls, ["carol", "carol"])


class TestRetry(unittest.TestCase):
    def setUp(self):
        self.app = helpers.FakeProxy()
        self.bc = bimodal_checker.BimodalChecker(self.app, {
            'bimodal_recheck_interval': '5.0',
            'proxyfsd_host': '10.1.1.1, 10.2.2.2',
        })
        self.fake_rpc = FakeJsonRpcWithErrors()
        patcher = mock.patch('pfs_middleware.utils.JsonRpcClient',
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
        resp = req.get_response(self.bc)
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
        resp = req.get_response(self.bc)
        self.assertEqual(resp.status_int, 503)

        self.assertEqual(self.fake_rpc.calls, (
            ('Server.RpcIsAccountBimodal', ({'AccountName': 'AUTH_test'},)),))

    def test_no_catch_other_error(self):
        self.fake_rpc.add_call_error(ZeroDivisionError)

        req = swob.Request.blank(
            "/v1/AUTH_test",
            environ={'REQUEST_METHOD': 'HEAD'})

        self.assertRaises(ZeroDivisionError, req.get_response, self.bc)
        self.assertEqual(self.fake_rpc.calls, (
            ('Server.RpcIsAccountBimodal', ({'AccountName': 'AUTH_test'},)),))
