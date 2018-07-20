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

"""
Middleware to check whether a given Swift account corresponds to a
ProxyFS volume or not.

This is part of the ProxyFS suite of middlewares needed to enable Swift-API
access to ProxyFS volumes. It doesn't have any user-visible functionality on
its own, but it is required for the other middlewares to work.
"""

import eventlet
import socket
import time

from swift.common import swob, constraints
from swift.common.utils import get_logger
from swift.proxy.controllers.base import get_account_info

from . import rpc, utils, swift_code


# full header is "X-Account-Sysmeta-Proxyfs-Bimodal"
SYSMETA_BIMODAL_INDICATOR = 'proxyfs-bimodal'

RPC_FINDER_TIMEOUT_DEFAULT = 3.0


class BimodalChecker(object):
    def __init__(self, app, conf, logger=None):
        self.app = app
        self.conf = conf
        self._cached_is_bimodal = {}
        self.logger = logger or get_logger(
            conf, log_route='pfs.bimodal_checker')

        proxyfsd_hosts = [h.strip() for h
                          in conf.get('proxyfsd_host', '127.0.0.1').split(',')]
        self.proxyfsd_port = int(conf.get('proxyfsd_port', '12345'))

        self.proxyfsd_addrinfos = []
        for host in proxyfsd_hosts:
            try:
                # If hostname resolution fails, we'll cause the proxy to
                # fail to start. This is probably better than returning 500
                # to every single request, but maybe not.
                #
                # To ensure that proxy startup works, use IP addresses for
                # proxyfsd_host. Then socket.getaddrinfo() will always work.
                addrinfo = socket.getaddrinfo(
                    host, self.proxyfsd_port,
                    socket.AF_UNSPEC, socket.SOCK_STREAM)[0]
                self.proxyfsd_addrinfos.append(addrinfo)
            except socket.gaierror:
                self.logger.error("Error resolving hostname %r", host)
                raise

        self.proxyfsd_rpc_timeout = float(conf.get('rpc_finder_timeout',
                                                   RPC_FINDER_TIMEOUT_DEFAULT))
        self.bimodal_recheck_interval = float(conf.get(
            'bimodal_recheck_interval', '60.0'))

    def _rpc_call(self, addrinfos, rpc_request):
        addrinfos = set(addrinfos)

        # We can get fast errors or slow errors here; we retry across all
        # hosts on fast errors, but immediately raise a slow error. HTTP
        # clients won't wait forever for a response, so we can't retry slow
        # errors across all hosts.
        #
        # Fast errors are things like "connection refused" or "no route to
        # host". Slow errors are timeouts.
        result = None
        while addrinfos:
            addrinfo = addrinfos.pop()
            rpc_client = utils.JsonRpcClient(addrinfo)
            try:
                result = rpc_client.call(rpc_request,
                                         self.proxyfsd_rpc_timeout)
            except socket.error as err:
                if addrinfos:
                    self.logger.debug("Error communicating with %r: %s. "
                                      "Trying again with another host.",
                                      addrinfo, err)
                    continue
                else:
                    raise
            except eventlet.Timeout:
                errstr = "Timeout ({0:.6f}s) calling {1}".format(
                    self.proxyfsd_rpc_timeout,
                    rpc_request.get("method", "<unknown method>"))
                raise utils.RpcTimeout(errstr)

            errstr = result.get("error")
            if errstr:
                errno = utils.extract_errno(errstr)
                raise utils.RpcError(errno, errstr)

            return result["result"]

    def _fetch_owning_proxyfs(self, req, account_name):
        """
        Checks to see if an account is bimodal or not, and if so, which proxyfs
        daemon owns it. Performs any necessary DNS resolution on the owner's
        address, raising an error if the owner is an unresolvable hostname.

        Will check a local cache first, falling back to a ProxyFS RPC call
        if necessary.

        Results are cached in memory locally, not memcached. ProxyFS has an
        in-memory list (or whatever data structure it uses) of known
        accounts, so it can answer the RPC request very quickly. Retrieving
        that result from memcached wouldn't be any faster than asking
        ProxyFS.

        :returns: 2-tuple (is-bimodal, proxyfsd-addrinfo).

        :raises utils.NoSuchHostnameError: if owning proxyfsd is
            unresolvable

        :raises utils.RpcTimeout: if proxyfsd is too slow in responding

        :raises utils.RpcError: if proxyfsd's response indicates an error
        """
        cached_result = self._cached_is_bimodal.get(account_name)
        if cached_result:
            res, res_time = cached_result
            if res_time + self.bimodal_recheck_interval >= time.time():
                # cache is populated and fresh; use it
                return res

        # First, ask Swift if the account is bimodal. This lets us keep
        # non-ProxyFS accounts functional during a ProxyFS outage.
        env_copy = req.environ.copy()
        env_copy[utils.ENV_IS_BIMODAL] = False
        account_info = get_account_info(env_copy, self.app,
                                        swift_source="PFS")
        if not swift_code.config_true_value(account_info["sysmeta"].get(
                SYSMETA_BIMODAL_INDICATOR)):
            res = (False, None)
            self._cached_is_bimodal[account_name] = (res, time.time())
            return res

        # We know where one proxyfsd is, and they'll all respond identically
        # to the same query.
        iab_req = rpc.is_account_bimodal_request(account_name)
        is_bimodal, proxyfsd_ip_or_hostname = \
            rpc.parse_is_account_bimodal_response(
                self._rpc_call(self.proxyfsd_addrinfos, iab_req))

        if not is_bimodal:
            # Swift account says bimodal, ProxyFS says otherwise.
            raise swob.HTTPServiceUnavailable(
                request=req,
                headers={"Content-Type": "text/plain"},
                body=("The Swift account says it has bimodal access, but "
                      "ProxyFS disagrees. Unable to proceed."))

        if not proxyfsd_ip_or_hostname:
            # When an account is moving between proxyfsd nodes, it's
            # bimodal but nobody owns it. This is usually a
            # very-short-lived temporary condition, so we don't cache
            # this result.
            return (True, None)

        # Just run whatever we got through socket.getaddrinfo(). If we
        # got an IPv4 or IPv6 address, it'll come out the other side
        # unchanged. If we got a hostname, it'll get resolved.
        try:
            # Someday, ProxyFS will probably start giving us port
            # numbers, too. Until then, assume they all use the same
            # port.
            addrinfos = socket.getaddrinfo(
                proxyfsd_ip_or_hostname, self.proxyfsd_port,
                socket.AF_UNSPEC, socket.SOCK_STREAM)
        except socket.gaierror:
            raise utils.NoSuchHostnameError(
                "Owning ProxyFS is at %s, but that could not "
                "be resolved to an IP address" % (
                    proxyfsd_ip_or_hostname))

        # socket.getaddrinfo returns things already sorted according
        # to the various rules in RFC 3484, so instead of thinking
        # we're smarter than Python and glibc, we'll just take the
        # first (i.e. best) one.
        #
        # Since we didn't get an exception, we resolved the hostname
        # to *something*, which means there's at least one element
        # in addrinfos.
        res = (True, addrinfos[0])
        self._cached_is_bimodal[account_name] = (res, time.time())
        return res

    @swob.wsgify
    def __call__(self, req):
        vrs, acc, con, obj = utils.parse_path(req.path)

        if not acc or not constraints.valid_api_version(vrs):
            # could be a GET /info request or something made up by some
            # other middleware; get out of the way.
            return self.app

        try:
            is_bimodal, owner_addrinfo = self._fetch_owning_proxyfs(req, acc)
        except (utils.RpcError, utils.RpcTimeout) as err:
            return swob.HTTPServiceUnavailable(
                request=req,
                headers={'Content-Type': 'text/plain'},
                body=str(err))

        # Other middlewares will find and act on this
        req.environ[utils.ENV_IS_BIMODAL] = is_bimodal
        req.environ[utils.ENV_OWNING_PROXYFS] = owner_addrinfo
        req.environ[utils.ENV_BIMODAL_CHECKER] = self

        return self.app
