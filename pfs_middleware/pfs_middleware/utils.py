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

import contextlib
import eventlet
import json
import socket
import re
from six.moves.urllib import parse as urllib_parse


ENV_IS_BIMODAL = 'pfs.is_bimodal'
ENV_OWNING_PROXYFS = 'pfs.owning_proxyfs'
ENV_BIMODAL_CHECKER = 'pfs.bimodal_checker'


class RpcError(Exception):
    def __init__(self, errno, *a, **kw):
        self.errno = errno
        super(RpcError, self).__init__(*a, **kw)


class RpcTimeout(Exception):
    pass


class NoSuchHostnameError(Exception):
    pass


def parse_path(path):
    """
    Takes the path component of an URL and returns a 4-element list of (API
    version, account, container, object).

    Similar to, but distinct from, swift.common.utils.split_path().

    >>> parse_path("/v1/AUTH_test/con/obj")
    ("v1", "AUTH_test", "con", "obj")
    >>> parse_path("/v1/AUTH_test/con")
    ("v1", "AUTH_test", "con", None)
    >>> parse_path("/info")
    ("info", None, None, None)
    """
    segs = urllib_parse.unquote(path).split('/', 4)
    # throw away the first segment; paths start with /, so the first segment
    # is always empty
    segs = [(s if s else None) for s in segs[1:]]
    if len(segs) < 4:
        segs.extend([None] * (4 - len(segs)))
    return segs


PFS_ERRNO_RE = re.compile("^errno: (\d+)$")


def extract_errno(errstr):
    """
    Given an error response from a proxyfs RPC, extracts the error number
    from it, or None if the error isn't in the usual format.
    """
    # A proxyfs error response looks like "errno: 18"
    m = re.match(PFS_ERRNO_RE, errstr)
    if m:
        return int(m.group(1))


class JsonRpcClient(object):
    def __init__(self, addrinfo):
        self.addrinfo = addrinfo

    def call(self, rpc_request, timeout):
        """
        Call a remote procedure using JSON-RPC.

        :param rpc_request: Python dictionary containing the request
            (method, args, etc.) in JSON-RPC format.

        :returns: the (deserialized) result of the RPC, whatever that looks
            like

        :raises: socket.error if the TCP connection breaks
        :raises: RpcTimeout if the RPC takes too long
        :raises: RpcError if the RPC returns an error
        """
        serialized_req = json.dumps(rpc_request).encode("utf-8")

        # for the format of self.addrinfo, see the docs for
        # socket.getaddrinfo()
        addr_family = self.addrinfo[0]
        sock_type = self.addrinfo[1]
        sock_proto = self.addrinfo[2]
        # self.addrinfo[3] is the canonical name, which isn't useful here
        addr = self.addrinfo[4]

        # XXX TODO keep sockets around in a pool or something?
        sock = socket.socket(addr_family, sock_type, sock_proto)
        with eventlet.Timeout(timeout):
            sock.connect(addr)
            with contextlib.closing(sock):
                sock.send(serialized_req)
                # This is JSON-RPC over TCP: we write a JSON document to
                # the socket, then read a JSON document back. The only
                # way we know when we're done is when what we've read is
                # valid JSON.
                #
                # Of course, Python's builtin JSON can't consume just
                # one document from a file, so for now, we'll use the
                # fact that the sender is sending one JSON object per
                # line and just call readline(). At some point, we
                # should replace this with a real incremental JSON
                # parser like ijson.
                sock_filelike = sock.makefile("r")
                with contextlib.closing(sock_filelike):
                    line = sock_filelike.readline()
                    try:
                        response = json.loads(line)
                    except ValueError as err:
                        raise ValueError(
                            "Error decoding JSON: %s (tried to decode %r)"
                            % (err, line))

                errstr = response.get("error")
                if errstr:
                    errno = extract_errno(errstr)
                    raise RpcError(errno, "Error in %s: %s" % (
                        rpc_request.get("method", "<unknown method>"),
                        errstr))
                return response
