# Copyright (c) 2016-2017 SwiftStack, Inc.
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

import collections
from swift.common import swob


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

        if method in ('PUT', 'COALESCE'):
            bytes_read = 0
            # consume the whole request body, just like a PUT would
            for chunk in iter(env['wsgi.input'].read, b''):
                bytes_read += len(chunk)
            cl = req.headers.get('Content-Length')

            if cl is not None and int(cl) != bytes_read:
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

    def call(self, rpc_request, _timeout, raise_on_rpc_error=True):
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
