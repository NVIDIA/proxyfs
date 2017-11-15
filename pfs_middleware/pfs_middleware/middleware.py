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

"""
Middleware that will provide a Swift-ish API for a ProxyFS account.

It tries to mimic the Swift API as much as possible, but there are some
differences that must be called out.

* ETags are sometimes different. In Swift, an object's ETag is the MD5
  checksum of its contents. With this middleware, an object's ETag is
  sometimes an opaque value sufficient to provide a strong identifier as per
  RFC 7231, but it is not the MD5 checksum of the object's contents.

  ETags start out as MD5 checksums, but if files are subsequently modified
  via SMB or NFS, the ETags become opaque values.

* Container listings lack object count. To get an object count, it would be
  necessary to traverse the entire directory structure underneath the
  container directory. This would be unbearably slow and resource-intensive.

* Account HEAD responses contain a header "ProxyFS-Enabled: yes". This way,
  clients can know what they're dealing with.

* Unlimited object size. A PUT request is not limited to 5 GiB, but can be
  arbitrarily large.

* Support for the COALESCE verb. Since static large objects will not work
  with this middleware's ETag values, another solution was found. A client
  can combine (or coalesce, if you will) a number of small files together
  into one large file, allowing for parallel uploads like a client can get
  with static large objects.

  Once the small files are uploaded, one makes a COALESCE request to the
  destination path. The request body is a JSON object containing a key
  "elements" whose value is a list of account-relative object names.

  Example:

      COALESCE /v1/AUTH_me/c/rainbow HTTP/1.1
      X-Auth-Token: t
      ... more headers ...

      {
          "elements": [
              "/c/red",
              "/c/orange",
              "/c/yellow",
              "/c/green",
              "/c/blue",
              "/c/indigo",
              "/c/violet"
          ]
      }

  This will combine the files /c/red, ..., /c/violet into a single file
  /c/rainbow. The files /c/red et cetera will be deleted as a result of this
  request.
"""
import contextlib
import datetime
import eventlet
import eventlet.queue
import hashlib
import itertools
import json
import math
import mimetypes
import re
import six
import socket
import time
import xml.etree.ElementTree as ET
from six.moves.urllib import parse as urllib_parse
from StringIO import StringIO

from . import pfs_errno, rpc, swift_code, utils

# Generally speaking, let's try to keep the use of Swift code to a
# reasonable level. Using dozens of random functions from swift.common.utils
# will ensure that this middleware breaks with every new Swift release. On
# the other hand, there's some really good, actually-works-in-production
# code in Swift that does things we need.

# Were we to make an account HEAD request instead of calling
# get_account_info, we'd lose the benefit of Swift's caching. This would
# slow down requests *a lot*. Same for containers.
from swift.proxy.controllers.base import get_account_info, get_container_info

# Plain WSGI is annoying to work with, and nobody wants a dependency on
# webob.
from swift.common import swob

# Our logs should go to the same place as everyone else's. Plus, this logger
# works well in an eventlet-ified process, and SegmentedIterable needs one.
from swift.common.utils import get_logger


# Used for content type of directories in container listings
DIRECTORY_CONTENT_TYPE = "application/directory"

ZERO_FILL_PATH = "/0"

LEASE_RENEWAL_INTERVAL = 5  # seconds

FORBIDDEN_OBJECT_HEADERS = {"X-Delete-At", "X-Delete-After", "Etag"}

SPECIAL_OBJECT_METADATA_HEADERS = {
    "Content-Type",
    "Content-Disposition",
    "Content-Encoding",
    "X-Object-Manifest",
    "X-Static-Large-Object"}

SPECIAL_CONTAINER_METADATA_HEADERS = {
    "X-Container-Read",
    "X-Container-Write",
    "X-Container-Sync-Key",
    "X-Container-Sync-To",
    "X-Versions-Location"}

MD5_ETAG_RE = re.compile("^[a-f0-9]{32}$")

ORIGINAL_MD5_HEADER = "X-Object-Sysmeta-ProxyFS-Initial-MD5"


def listing_iter_from_read_plan(read_plan):
    """
    Takes a read plan from proxyfsd and turns it into an iterable of
    tuples suitable for passing to SegmentedIterable.

    Example read plan:

    [
        {
            "Length": 4,
            "ObjectPath": "/v1/AUTH_test/Replicated3Way_1/0000000000000074",
            "Offset": 0
        },
        {
            "Length": 17,
            "ObjectPath": "/v1/AUTH_test/Replicated3Way_1/0000000000000076",
            "Offset": 0
        },
        {
            "Length": 19,
            "ObjectPath": "/v1/AUTH_test/Replicated3Way_1/0000000000000078",
            "Offset": 0
        },
        {
            "Length": 89,
            "ObjectPath": "/v1/AUTH_test/Replicated3Way_1/000000000000007A",
            "Offset": 0
        }
    ]

    Example return value:

    [
        ("/v1/AUTH_test/Replicated3Way_1/0000000000000074", None, None, 0, 3),
        ("/v1/AUTH_test/Replicated3Way_1/0000000000000076", None, None, 0, 16),
        ("/v1/AUTH_test/Replicated3Way_1/0000000000000078", None, None, 0, 18),
        ("/v1/AUTH_test/Replicated3Way_1/000000000000007A", None, None, 0, 88),
    ]
    """
    if read_plan is None:
        # ProxyFS likes to send null values instead of empty lists.
        read_plan = ()

    # It's a little ugly that the GoCase field names escape from the
    # RPC-response parser all the way to here, but it's inefficient, in both
    # CPU cycles and programmer brainpower, to create some intermediate
    # representation just to avoid GoCase.
    return [(rpe["ObjectPath"] or ZERO_FILL_PATH,
             None,  # we don't know the segment's ETag
             None,  # we don't know the segment's length
             rpe["Offset"],
             rpe["Offset"] + rpe["Length"] - 1)
            for rpe in read_plan]


def x_timestamp_from_epoch_ns(epoch_ns):
    """
    Convert a ProxyFS-style Unix timestamp to a Swift X-Timestamp header.

    ProxyFS uses an integral number of nanoseconds since the epoch, while
    Swift uses a floating-point number with centimillisecond (10^-5 second)
    precision.

    :param epoch_ns: Unix time, expressed as an integral number of
                     nanoseconds since the epoch. Note that this is not the
                     usual Unix convention of a *real* number of *seconds*
                     since the epoch.

    :returns: ISO-8601 timestamp (like those found in Swift's container
              listings), e.g. '2016-08-05T00:55:16.966920'
    """
    return "{0:.5f}".format(float(epoch_ns) / 1000000000)


def iso_timestamp_from_epoch_ns(epoch_ns):
    """
    Convert a Unix timestamp to an ISO-8601 timestamp.

    :param epoch_ns: Unix time, expressed as an integral number of
                     nanoseconds since the epoch. Note that this is not the
                     usual Unix convention of a *real* number of *seconds*
                     since the epoch.

    :returns: ISO-8601 timestamp (like those found in Swift's container
              listings), e.g. '2016-08-05T00:55:16.966920'
    """
    iso_timestamp = datetime.datetime.utcfromtimestamp(
        epoch_ns / 1000000000.0).isoformat()

    # Convieniently (ha!), isoformat() method omits the
    # fractional-seconds part if it's equal to 0. The Swift proxy server
    # does not, so we match its output.
    if iso_timestamp[-7] != ".":
        iso_timestamp += ".000000"
    return iso_timestamp


def last_modified_from_epoch_ns(epoch_ns):
    """
    Convert a Unix timestamp to an ISO-8601 timestamp.

    :param epoch_ns: Unix time, expressed as an integral number of
                     nanoseconds since the epoch. Note that this is not the
                     usual Unix convention of a *real* number of *seconds*
                     since the epoch.

    :returns: Last-Modified header value in IMF-Fixdate format as specified
        in RFC 7231 section 7.1.1.1.
    """
    return time.strftime(
        '%a, %d %b %Y %H:%M:%S GMT',
        time.gmtime(math.ceil(epoch_ns / 1000000000.0)))


def guess_content_type(filename, is_dir):
    if is_dir:
        return DIRECTORY_CONTENT_TYPE

    content_type, _ = mimetypes.guess_type(filename)
    if not content_type:
        content_type = "application/octet-stream"
    return content_type


def looks_like_md5(a_string):
    return re.search(MD5_ETAG_RE, a_string)


@contextlib.contextmanager
def pop_and_restore(hsh, key, default=None):
    """
    Temporarily remove and yield a value from a hash. Restores that
    key/value pair to its original state in the hash on exit.
    """
    if key in hsh:
        value = hsh.pop(key)
        was_there = True
    else:
        value = default
        was_there = False

    yield value

    if was_there:
        hsh[key] = value


def deserialize_metadata(raw_metadata):
    if raw_metadata:
        try:
            metadata = json.loads(raw_metadata)
        except ValueError:
            metadata = {}
    else:
        metadata = {}
    encoded_metadata = {}
    for k, v in metadata.iteritems():
        key = unicode(k).encode("utf-8") if isinstance(k, basestring) else k
        value = unicode(v).encode("utf-8") if isinstance(v, basestring) else v
        encoded_metadata[key] = value
    return encoded_metadata


serialize_metadata = json.dumps


def merge_container_metadata(old, new):
    merged = old.copy()
    for k, v in new.items():
        merged[k] = v
    return merged


def merge_object_metadata(old, new):
    merged = new.copy()
    old_ct = old.get("Content-Type")
    new_ct = new.get("Content-Type")
    if old_ct is not None and new_ct is None:
        merged["Content-Type"] = old_ct
    return merged


def extract_object_metadata_from_headers(headers):
    """
    Find and return the key/value pairs containing object metadata.

    This tries to do the same thing as the Swift object server: save only
    relevant headers. If the user sends in "X-Fungus-Amungus: shroomy" in
    the PUT request's headers, we'll ignore it, just like plain old Swift
    would.

    :param headers: request headers (a dictionary)

    :returns: dictionary containing object-metadata headers
    """
    meta_headers = {}
    for header, value in headers.items():
        header = header.title()

        if (header.startswith("X-Object-Meta-") or
                header.startswith("X-Object-Sysmeta-") or
                header in SPECIAL_OBJECT_METADATA_HEADERS):
            meta_headers[header] = value
    return meta_headers


def extract_container_metadata_from_headers(headers):
    """
    Find and return the key/value pairs containing object metadata.

    This tries to do the same thing as the Swift object server: save only
    relevant headers. If the user sends in "X-Fungus-Amungus: shroomy" in
    the PUT request's headers, we'll ignore it, just like plain old Swift
    would.

    :param headers: request headers (a dictionary)

    :returns: dictionary containing object-metadata headers
    """
    meta_headers = {}
    for header, value in headers.items():
        header = header.title()

        if (header.startswith("X-Container-Meta-") or
                header.startswith("X-Container-Sysmeta-") or
                header in SPECIAL_CONTAINER_METADATA_HEADERS):
            meta_headers[header] = value
    return meta_headers


def best_possible_etag(obj_metadata, account_name, ino, num_writes):
    if ORIGINAL_MD5_HEADER in obj_metadata:
        val = obj_metadata[ORIGINAL_MD5_HEADER]
        try:
            stored_num_writes, md5sum = val.split(':', 1)
            if int(stored_num_writes) == num_writes:
                return md5sum
        except ValueError:
            pass
    return construct_etag(account_name, ino, num_writes)


def construct_etag(account_name, ino, num_writes):
    # We append -32 in an attempt to placate S3 clients. In S3, the ETag of
    # a multipart object looks like "hash-N" where <hash> is the MD5 of the
    # MD5s of the segments and <N> is the number of segments.
    #
    # Since this etag is not an MD5 digest value, we append -32 here in
    # hopes that some S3 clients will be able to download ProxyFS files via
    # S3 API without complaining about checksums.
    #
    # 32 was chosen because it was the ticket number of the author's lunch
    # order on the day this code was written. It has no significance.
    return '"pfsv2/{}/{:08X}/{:08X}-32"'.format(
        urllib_parse.quote(account_name), ino, num_writes)


def iterator_posthook(iterable, posthook, *posthook_args, **posthook_kwargs):
    try:
        for x in iterable:
            yield x
    finally:
        posthook(*posthook_args, **posthook_kwargs)


class ZeroFiller(object):
    """
    Internal middleware to handle the zero-fill portions of sparse files for
    object GET responses.
    """
    ZEROES = "\x00" * 4096

    def __init__(self, app):
        self.app = app

    @swob.wsgify
    def __call__(self, req):
        if req.path == ZERO_FILL_PATH:
            # We know we can do this since the creator of these requests is
            # also in this library.
            start, end = req.range.ranges[0]
            nbytes = end - start + 1
            resp = swob.Response(
                request=req, status=206,
                headers={"Content-Length": nbytes,
                         "Content-Range": "%d-%d/%d" % (start, end, nbytes)},
                app_iter=self.yield_n_zeroes(nbytes))
            return resp
        else:
            return self.app

    def yield_n_zeroes(self, n):
        # It's a little clunky, but it does avoid creating new strings of
        # zeroes over and over again, and it uses only a small amount of
        # memory. This becomes important if a file contains hundreds of
        # megabytes or more of zeroes; allocating a single string of zeroes
        # would do Bad Things(tm) to our memory usage.
        zlen = len(self.ZEROES)
        while n > zlen:
            yield self.ZEROES
            n -= zlen
        if n > 0:
            yield self.ZEROES[:n]


class SnoopingInput(object):
    """
    Wrap WSGI input and call a provided callback every time data is read.
    """
    def __init__(self, wsgi_input, callback):
        self.wsgi_input = wsgi_input
        self.callback = callback

    def read(self, *a, **kw):
        chunk = self.wsgi_input.read(*a, **kw)
        self.callback(chunk)
        return chunk

    def readline(self, *a, **kw):
        line = self.wsgi_input.readline(*a, **kw)
        self.callback(line)
        return line


class LimitedInput(object):
    """
    Wrap WSGI input and limit the consumer to taking at most N bytes.

    Also count bytes read. This lets us tell ProxyFS how big an object is
    after an object PUT completes.
    """
    def __init__(self, wsgi_input, limit):
        self._peeked_data = ""
        self.limit = self.orig_limit = limit
        self.bytes_read = 0
        self.wsgi_input = wsgi_input
        self.hit_eof = False

    @property
    def finished_the_input_stream(self):
        return (self.bytes_read < self.orig_limit) and self.hit_eof

    def read(self, length=None, *args, **kwargs):
        if length is None:
            to_read = self.limit
        else:
            to_read = min(self.limit, length)
        to_read -= len(self._peeked_data)

        chunk = self.wsgi_input.read(to_read, *args, **kwargs)
        chunk = self._peeked_data + chunk
        self._peeked_data = ""

        self.bytes_read += len(chunk)
        self.limit -= len(chunk)
        if len(chunk) == 0:
            self.hit_eof = True
        return chunk

    def readline(self, size=None, *args, **kwargs):
        if size is None:
            to_read = self.limit
        else:
            to_read = min(self.limit, size)
        to_read -= len(self._peeked_data)

        line = self.wsgi_input.readline(to_read, *args, **kwargs)
        line = self._peeked_data + line
        self._peeked_data = ""

        self.bytes_read += len(line)
        self.limit -= len(line)
        if len(line) == 0:
            self.hit_eof = True
        return line

    @property
    def has_more_to_read(self):
        if not self._peeked_data:
            self._peek()
        return len(self._peeked_data) > 0

    def _peek(self):
        self._peeked_data = self.wsgi_input.read(1)


class RequestContext(object):
    """
    Stuff we need to service the current request.

    Basically a pile of data with a name.
    """
    def __init__(self, req, proxyfsd_addrinfo,
                 account_name, container_name, object_name):
        # swob.Request object
        self.req = req

        # address info for proxyfsd, as returned from socket.getaddrinfo()
        #
        # NB: this is only used for Server.IsAccountBimodal requests; the
        # return value there tells us where to go for this particular
        # account. That may or may not be the same as
        # self.proxyfsd_addrinfo.
        self.proxyfsd_addrinfo = proxyfsd_addrinfo

        # account/container/object names
        self.account_name = account_name
        self.container_name = container_name
        self.object_name = object_name


class PfsMiddleware(object):
    def __init__(self, app, conf, logger=None):
        self._cached_proxy_info = None
        self.app = app
        self.zero_filler_app = ZeroFiller(app)
        self.conf = conf

        self.logger = logger or get_logger(conf, log_route='pfs')

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

        self.proxyfsd_rpc_timeout = float(conf.get('rpc_timeout', '3.0'))
        self.bimodal_recheck_interval = float(conf.get(
            'bimodal_recheck_interval', '60.0'))
        self.max_get_time = int(conf.get('max_get_time', '86400'))
        self.max_log_segment_size = int(conf.get(
            'max_log_segment_size', '2147483648'))  # 2 GiB
        self.max_coalesce = int(conf.get('max_coalesce', '1000'))

        # Assume a max object length of the Swift default of 1024 bytes plus
        # a few extra for JSON quotes, commas, et cetera.
        self.max_coalesce_request_size = self.max_coalesce * 1100

    @swob.wsgify
    def __call__(self, req):
        vrs, acc, con, obj = utils.parse_path(req.path)

        if not acc:
            # could be a GET /info request or something made up by some
            # other middleware; get out of the way.
            return self.app

        try:
            # Check account to see if this is a bimodal-access account or
            # not. ProxyFS is the sole source of truth on this matter.
            is_bimodal, proxyfsd_addrinfo = self._unpack_owning_proxyfs(req)
            if not is_bimodal and proxyfsd_addrinfo is None:
                # This is a plain old Swift account, so we get out of the
                # way.
                return self.app
            elif proxyfsd_addrinfo is None:
                # This is a bimodal account, but there is currently no
                # proxyfsd responsible for it. This can happen during a move
                # of a ProxyFS volume from one proxyfsd to another, and
                # should be cleared up quickly. Nevertheless, all we can do
                # here is return an error to the client.
                return swob.HTTPServiceUnavailable(request=req)

            ctx = RequestContext(req, proxyfsd_addrinfo, acc, con, obj)
            # For requests that we make to Swift, we have to ensure that any
            # auth callback is not present in the WSGI environment.
            # Authorization typically uses the object path as an input, and
            # letting that loose on log-segment object paths is not likely to
            # end well.
            #
            # We are careful to restore any auth to the environment when we
            # exit, though, as this lets authentication work on the segments of
            # Swift large objects. The SLO or DLO middleware is to the left of
            # us in the pipeline, and it will make multiple subrequests, and
            # auth is required for each one.
            with pop_and_restore(req.environ,
                                 'swift.authorize') as auth_cb, \
                    pop_and_restore(req.environ, 'swift.authorize_override',
                                    False) as auth_override:
                # If someone's set swift.authorize_override, then they've
                # already handled auth, and all we should do is get out of the
                # way.
                if auth_cb and not auth_override:
                    req.acl = self._fetch_appropriate_acl(ctx)
                    denial_response = auth_cb(ctx.req)
                    if denial_response:
                        return denial_response

                # Authorization succeeded; dispatch to a helper method
                method = req.method
                if method == 'GET' and obj:
                    return self.get_object(ctx)
                elif method == 'HEAD' and obj:
                    return self.head_object(ctx)
                elif method == 'PUT' and obj:
                    return self.put_object(ctx)
                elif method == 'POST' and obj:
                    return self.post_object(ctx)
                elif method == 'DELETE' and obj:
                    return self.delete_object(ctx)
                elif method == 'COALESCE' and obj:
                    return self.coalesce_object(ctx)

                elif method == 'GET' and con:
                    return self.get_container(ctx)
                elif method == 'HEAD' and con:
                    return self.head_container(ctx)
                elif method == 'PUT' and con:
                    return self.put_container(ctx)
                elif method == 'POST' and con:
                    return self.post_container(ctx)
                elif method == 'DELETE' and con:
                    return self.delete_container(ctx)

                elif method == 'GET':
                    return self.get_account(ctx)
                elif method == 'HEAD':
                    return self.head_account(ctx)
                # account HEAD, PUT, POST, and DELETE are just passed
                # through to Swift
                else:
                    return self.app

        # Provide some top-level exception handling and logging for
        # exceptional exceptions. Non-exceptional exceptions will be handled
        # closer to where they were raised.
        except utils.RpcTimeout as err:
            self.logger.error("RPC timeout: %s", err)
            return swob.HTTPInternalServerError(
                request=req,
                headers={"Content-Type": "text/plain"},
                body="RPC timeout: {0}".format(err))
        except utils.RpcError as err:
            self.logger.error(
                "RPC error: %s; consulting proxyfsd logs may be helpful", err)
            return swob.HTTPInternalServerError(
                request=req,
                headers={"Content-Type": "text/plain"},
                body="RPC error: {0}".format(err))

    def _fetch_appropriate_acl(self, ctx):
        """
        Return the appropriate ACL to handle authorization for the given
        request. This method may make a subrequest if necessary.

        The ACL will be one of three things:

        * for object/container GET/HEAD, it's the container's read ACL
          (X-Container-Read).

        * for object PUT/POST/DELETE, it's the container's write ACL
          (X-Container-Write).

        * for all other requests, it's None

        Some authentication systems, of course, also have account-level
        ACLs. However, in the fine tradition of having two possible courses
        of action and choosing both, the loading of the account-level ACLs
        is handled by the auth callback.
        """

        bimodal_checker = ctx.req.environ[utils.ENV_BIMODAL_CHECKER]

        if ctx.req.method in ('GET', 'HEAD') and ctx.container_name:
            container_info = get_container_info(
                ctx.req.environ, bimodal_checker,
                swift_source="PFS")
            return container_info['read_acl']
        elif ctx.req.method in ('PUT', 'POST', 'DELETE') and ctx.object_name:
            container_info = get_container_info(
                ctx.req.environ, bimodal_checker,
                swift_source="PFS")
            return container_info['write_acl']
        else:
            return None

    def get_account(self, ctx):
        req = ctx.req
        limit = self._get_listing_limit(
            req, self._default_account_listing_limit())
        marker = req.params.get('marker', '')
        get_account_request = rpc.get_account_request(
            urllib_parse.unquote(req.path), marker, limit)
        # If the account does not exist, then __call__ just falls through to
        # self.app, so we never even get here. If we got here, then the
        # account does exist, so we don't have to worry about not-found
        # versus other-error here. Any error counts as "completely busted".
        #
        # We let the top-level RpcError handler catch this.
        get_account_response = self.rpc_call(ctx, get_account_request)
        container_names = rpc.parse_get_account_response(get_account_response)

        resp_content_type = swift_code.get_listing_content_type(req)
        resp = swob.HTTPOk(content_type=resp_content_type, charset="utf-8",
                           request=req)
        if resp_content_type == "text/plain":
            resp.body = self._plaintext_account_get_response(container_names)
        elif resp_content_type == "application/json":
            resp.body = self._json_account_get_response(container_names)
        elif resp_content_type.endswith("/xml"):
            resp.body = self._xml_account_get_response(container_names,
                                                       ctx.account_name)
        else:
            raise Exception("unexpected content type %r" %
                            (resp_content_type,))

        # For accounts, the meta/sysmeta is stored in the account DB in
        # Swift, not in ProxyFS.
        account_info = get_account_info(
            req.environ, req.environ[utils.ENV_BIMODAL_CHECKER],
            swift_source="PFS")
        for key, value in account_info["meta"].items():
            resp.headers["X-Account-Meta-" + key] = value
        for key, value in account_info["sysmeta"].items():
            resp.headers["X-Account-Sysmeta-" + key] = value

        return resp

    def _plaintext_account_get_response(self, container_names):
        chunks = []
        for container_name in container_names:
            chunks.append(container_name)
            chunks.append("\n")
        return ''.join(chunks)

    def _json_account_get_response(self, container_names):
        json_entries = []
        for name in container_names:
            json_entry = {
                "name": name,
                # proxyfsd can't compute these without recursively walking
                # the entire filesystem, so rather than have a built-in DoS
                # attack, we just put out zeros here.
                #
                # These stats aren't particularly up-to-date in Swift
                # anyway, so there aren't going to be working clients that
                # rely on them for accuracy.
                "count": 0,
                "bytes": 0}
            json_entries.append(json_entry)
        return json.dumps(json_entries)

    def _xml_account_get_response(self, container_names, account_name):
        root_node = ET.Element('account', name=account_name)

        for container_name in container_names:
            container_node = ET.Element('container')

            name_node = ET.Element('name')
            name_node.text = container_name
            container_node.append(name_node)

            count_node = ET.Element('count')
            count_node.text = '0'
            container_node.append(count_node)

            bytes_node = ET.Element('bytes')
            bytes_node.text = '0'
            container_node.append(bytes_node)

            root_node.append(container_node)

        buf = StringIO()
        ET.ElementTree(root_node).write(
            buf, encoding="utf-8", xml_declaration=True)
        return buf.getvalue()

    def head_account(self, ctx):
        req = ctx.req
        resp = req.get_response(self.app)
        resp.headers["ProxyFS-Enabled"] = "yes"
        return resp

    def head_container(self, ctx):
        head_request = rpc.head_request(urllib_parse.unquote(ctx.req.path))
        try:
            head_response = self.rpc_call(ctx, head_request)
        except utils.RpcError as err:
            if err.errno == pfs_errno.NotFoundError:
                return swob.HTTPNotFound(request=ctx.req)
            else:
                raise

        raw_metadata, _, _, _, _, _ = rpc.parse_head_response(head_response)
        metadata = deserialize_metadata(raw_metadata)
        return swob.HTTPNoContent(request=ctx.req, headers=metadata)

    def put_container(self, ctx):
        req = ctx.req
        container_path = urllib_parse.unquote(req.path)
        new_metadata = extract_container_metadata_from_headers(req.headers)

        try:
            head_response = self.rpc_call(
                ctx, rpc.head_request(container_path))
            raw_old_metadata, _, _, _, _, _ = rpc.parse_head_response(
                head_response)
        except utils.RpcError as err:
            if err.errno == pfs_errno.NotFoundError:
                self.rpc_call(ctx, rpc.put_container_request(
                    container_path,
                    "",
                    serialize_metadata(new_metadata)))
                return swob.HTTPCreated(request=req)
            else:
                raise

        old_metadata = deserialize_metadata(raw_old_metadata)
        merged_metadata = merge_container_metadata(
            old_metadata, new_metadata)
        raw_merged_metadata = serialize_metadata(merged_metadata)

        self.rpc_call(ctx, rpc.put_container_request(
            container_path, raw_old_metadata, raw_merged_metadata))

        return swob.HTTPAccepted(request=req)

    def post_container(self, ctx):
        req = ctx.req
        container_path = urllib_parse.unquote(req.path)
        new_metadata = extract_container_metadata_from_headers(req.headers)

        try:
            head_response = self.rpc_call(
                ctx, rpc.head_request(container_path))
            raw_old_metadata, _, _, _, _, _ = rpc.parse_head_response(
                head_response)
        except utils.RpcError as err:
            if err.errno == pfs_errno.NotFoundError:
                return swob.HTTPNotFound(request=req)
            else:
                raise

        old_metadata = deserialize_metadata(raw_old_metadata)
        merged_metadata = merge_container_metadata(
            old_metadata, new_metadata)
        raw_merged_metadata = serialize_metadata(merged_metadata)

        self.rpc_call(ctx, rpc.post_request(
            container_path, raw_old_metadata, raw_merged_metadata))

        return swob.HTTPNoContent(request=req)

    def delete_container(self, ctx):
        # Turns out these are the same RPC with the same error handling, so
        # why not?
        return self.delete_object(ctx)

    def _get_listing_limit(self, req, default_limit):
        raw_limit = req.params.get('limit')

        if raw_limit is not None:
            try:
                limit = int(raw_limit)
            except ValueError:
                limit = default_limit

            if limit > default_limit:
                err = "Maximum limit is %d" % default_limit
                raise swob.HTTPPreconditionFailed(request=req, body=err)
            elif limit < 0:
                limit = default_limit
        else:
            limit = default_limit

        return limit

    def _default_account_listing_limit(self):
        proxy_info = self._proxy_info()
        return proxy_info["swift"]["account_listing_limit"]

    def _default_container_listing_limit(self):
        proxy_info = self._proxy_info()
        return proxy_info["swift"]["container_listing_limit"]

    def _proxy_info(self):
        if self._cached_proxy_info is None:
            req = swob.Request.blank("/info")
            resp = req.get_response(self.app)
            self._cached_proxy_info = json.loads(resp.body)
        return self._cached_proxy_info

    def get_container(self, ctx):
        req = ctx.req
        limit = self._get_listing_limit(
            req, self._default_container_listing_limit())
        marker = req.params.get('marker', '')
        prefix = req.params.get('prefix', '')
        get_container_request = rpc.get_container_request(
            urllib_parse.unquote(req.path), marker, limit, prefix)
        try:
            get_container_response = self.rpc_call(ctx, get_container_request)
        except utils.RpcError as err:
            if err.errno == pfs_errno.NotFoundError:
                return swob.HTTPNotFound(request=req)
            else:
                raise

        container_ents, raw_metadata = rpc.parse_get_container_response(
            get_container_response)

        resp_content_type = swift_code.get_listing_content_type(req)
        resp = swob.HTTPOk(content_type=resp_content_type, charset="utf-8",
                           request=req)
        if resp_content_type == "text/plain":
            resp.body = self._plaintext_container_get_response(
                container_ents)
        elif resp_content_type == "application/json":
            resp.body = self._json_container_get_response(
                container_ents, ctx.account_name)
        elif resp_content_type.endswith("/xml"):
            resp.body = self._xml_container_get_response(
                container_ents, ctx.account_name, ctx.container_name)
        else:
            raise Exception("unexpected content type %r" %
                            (resp_content_type,))

        metadata = deserialize_metadata(raw_metadata)
        resp.headers.update(metadata)

        return resp

    def _plaintext_container_get_response(self, container_entries):
        chunks = []
        for ent in container_entries:
            chunks.append(ent["Basename"])
            chunks.append("\n")
        return ''.join(chunks)

    def _json_container_get_response(self, container_entries, account_name):
        json_entries = []
        for ent in container_entries:
            name = ent["Basename"]
            size = ent["FileSize"]
            last_modified = iso_timestamp_from_epoch_ns(
                ent["ModificationTime"])
            obj_metadata = deserialize_metadata(ent["Metadata"])
            content_type = obj_metadata.get("Content-Type")
            if content_type is None:
                content_type = guess_content_type(ent["Basename"],
                                                  ent["IsDir"])
            etag = best_possible_etag(
                obj_metadata, account_name,
                ent["InodeNumber"], ent["NumWrites"])
            json_entry = {
                "name": name,
                "bytes": size,
                "content_type": content_type,
                "hash": etag,
                "last_modified": last_modified}
            json_entries.append(json_entry)
        return json.dumps(json_entries)

    def _xml_container_get_response(self, container_entries, account_name,
                                    container_name):
        root_node = ET.Element('container', name=container_name)

        for container_entry in container_entries:
            obj_name = container_entry['Basename']
            obj_metadata = deserialize_metadata(container_entry["Metadata"])
            content_type = obj_metadata.get("Content-Type")
            if content_type is None:
                content_type = guess_content_type(
                    container_entry["Basename"], container_entry["IsDir"])
            etag = best_possible_etag(
                obj_metadata, account_name,
                container_entry["InodeNumber"],
                container_entry["NumWrites"])
            container_node = ET.Element('object')

            name_node = ET.Element('name')
            name_node.text = obj_name
            container_node.append(name_node)

            hash_node = ET.Element('hash')
            hash_node.text = etag
            container_node.append(hash_node)

            bytes_node = ET.Element('bytes')
            bytes_node.text = str(container_entry["FileSize"])
            container_node.append(bytes_node)

            ct_node = ET.Element('content_type')
            ct_node.text = content_type
            container_node.append(ct_node)

            lm_node = ET.Element('last_modified')
            lm_node.text = iso_timestamp_from_epoch_ns(
                container_entry["ModificationTime"])
            container_node.append(lm_node)

            root_node.append(container_node)

        buf = StringIO()
        ET.ElementTree(root_node).write(
            buf, encoding="utf-8", xml_declaration=True)
        return buf.getvalue()

    def put_object(self, ctx):
        req = ctx.req
        # Make sure the (virtual) container exists
        #
        # We have to dig out an earlier-in-the-chain middleware here because
        # Swift's get_container_info() function has an internal whitelist of
        # environ keys that it'll keep, and our is-bimodal stuff isn't
        # included. To work around this, we pass in the middleware chain
        # starting with the bimodal checker so it can repopulate our environ
        # keys. At least the RpcIsBimodal response is cached, so this
        # shouldn't be too slow.
        container_info = get_container_info(
            req.environ, req.environ[utils.ENV_BIMODAL_CHECKER],
            swift_source="PFS")
        if not 200 <= container_info["status"] < 300:
            return swob.HTTPNotFound(request=req)

        if (req.headers.get('Content-Type') == DIRECTORY_CONTENT_TYPE and
                req.headers.get('Content-Length') == '0'):
            return self.put_object_as_directory(ctx)
        else:
            return self.put_object_as_file(ctx)

    def put_object_as_directory(self, ctx):
        """
        Create an object as a directory.
        """
        req = ctx.req

        path = urllib_parse.unquote(req.path)
        obj_metadata = serialize_metadata(extract_object_metadata_from_headers(
            req.headers))

        rpc_req = rpc.middleware_mkdir_request(path, obj_metadata)
        rpc_resp = self.rpc_call(ctx, rpc_req)
        mtime_ns, inode, num_writes = rpc.parse_middleware_mkdir_response(
            rpc_resp)

        resp_headers = {
            "Etag": construct_etag(ctx.account_name, inode, num_writes),
            "Content-Type": DIRECTORY_CONTENT_TYPE,
            "Last-Modified": last_modified_from_epoch_ns(mtime_ns)}
        return swob.HTTPCreated(request=req, headers=resp_headers, body="")

    def put_object_as_file(self, ctx):
        """
        ProxyFS has the concepts of "virtual" and "physical" path. The
        virtual path is the one that the user sees, i.e. /v1/acc/con/obj.
        A physical path is the location of an underlying log-segment
        object, e.g. /v1/acc/ContainerPoolName_1/00000000501B7321.

        An object in ProxyFS is backed by one or more physical objects. In
        the case of an object PUT, we ask proxyfsd for one or more suitable
        physical-object names for the object, write the data there
        ourselves, then tell proxyfsd what we've done.
        """
        req = ctx.req

        virtual_path = urllib_parse.unquote(req.path)
        put_location_req = rpc.put_location_request(virtual_path)

        request_etag = req.headers.get("ETag", "")
        hasher = hashlib.md5()
        wsgi_input = SnoopingInput(req.environ["wsgi.input"], hasher.update)

        # TODO: when the upload size is known (i.e. Content-Length is set),
        # ask for enough locations up front that we can consume the whole
        # request with only one call to RpcPutLocation(s).

        # TODO: ask to validate the path a bit better; if we are putting an
        # object at /v1/a/c/kitten.png/whoops.txt (where kitten.png is a
        # file), we should probably catch that before reading any input so
        # that, if the client sent "Expect: 100-continue", we can give them
        # an error early.

        physical_path_gen = (
            rpc.parse_put_location_response(
                self.rpc_call(ctx, put_location_req))
            for _ in itertools.repeat(None))

        error_response = swift_code.check_object_creation(req)
        if error_response:
            return error_response

        # If these make it to Swift, they can goof up our log segment behind
        # proxyfs's back.
        for forbidden_header in FORBIDDEN_OBJECT_HEADERS:
            req.headers.pop(forbidden_header, None)

        # Since this upload can be arbitrarily large, we split it across
        # multiple log segments.
        log_segments = []
        more_to_upload = True
        i = 0
        while more_to_upload:
            subreq = swob.Request(req.environ.copy())  # no cross-contamination

            # This ensures that (a) every subrequest has its own unique
            # txid, and (b) a log search for the txid in the response finds
            # all of the subrequests.
            if 'X-Trans-Id' in subreq.headers:
                subreq.headers['X-Trans-Id'] += ("-%03x" % i)
            subreq.environ['wsgi.input'] = subinput = LimitedInput(
                wsgi_input, self.max_log_segment_size)
            subreq.headers.pop('Content-Length', None)
            subreq.headers["Transfer-Encoding"] = "chunked"

            if not subinput.has_more_to_read:
                break

            # Ask ProxyFS for the next log segment we can use
            phys_path = next(physical_path_gen)
            subreq.environ["PATH_INFO"] = phys_path

            # Actually put one chunk of the data into Swift
            subresp = subreq.get_response(self.app)
            if not 200 <= subresp.status_int < 299:
                # Something went wrong; may as well bail out now
                return subresp

            log_segments.append((phys_path, subinput.bytes_read))

            # If the underlying wsgi.input hit EOF, then there's no more
            # data to upload.
            more_to_upload = not subinput.finished_the_input_stream
            i += 1

        if looks_like_md5(request_etag) and hasher.hexdigest() != request_etag:
            return swob.HTTPUnprocessableEntity(request=req)

        # All the data is now in Swift; we just have to tell proxyfsd about
        # it. We save off the original MD5 checksum and the number of log
        # segments (this later becomes the NumWrites value) so that we can
        # provide an ETag that's an MD5 checksum unless the file has been
        # subsequently written.
        obj_metadata = extract_object_metadata_from_headers(req.headers)
        obj_metadata[ORIGINAL_MD5_HEADER] = "%d:%s" % (len(log_segments),
                                                       hasher.hexdigest())

        put_complete_req = rpc.put_complete_request(
            virtual_path, log_segments, serialize_metadata(obj_metadata))
        try:
            # Ignore the return value. On success, there's nothing
            # useful in the response.
            mtime_ns, inode, num_writes = rpc.parse_put_complete_response(
                self.rpc_call(ctx, put_complete_req))
        except utils.RpcError as err:
            # We deliberately don't try to clean up our log segments on
            # failure. ProxyFS is responsible for cleaning up unreferenced
            # log segments.
            if err.errno == pfs_errno.IsDirError:
                return swob.HTTPConflict(
                    request=req,
                    headers={"Content-Type": "text/plain"},
                    body="This is a directory")
            elif err.errno == pfs_errno.NotDirError:
                return swob.HTTPConflict(
                    request=req,
                    headers={"Content-Type": "text/plain"},
                    body="Path element is a file, not a directory")
            else:
                # punt to top-level error handler
                raise

        # For reference, an object PUT response to plain Swift looks like:
        # HTTP/1.1 201 Created
        # Last-Modified: Thu, 08 Dec 2016 22:51:13 GMT
        # Content-Length: 0
        # Etag: 9303a8d23189779e71f347032d633327
        # Content-Type: text/html; charset=UTF-8
        # X-Trans-Id: tx7b3e2b88df2f4975a5476-005849e3e0dfw1
        # Date: Thu, 08 Dec 2016 22:51:12 GMT
        #
        # We get Content-Length, X-Trans-Id, and Date for free, but we need
        # to fill in the rest.
        resp_headers = {
            "Etag": hasher.hexdigest(),
            "Content-Type": guess_content_type(req.path, False),
            "Last-Modified": last_modified_from_epoch_ns(mtime_ns)}
        return swob.HTTPCreated(request=req, headers=resp_headers, body="")

    def post_object(self, ctx):
        req = ctx.req
        path = urllib_parse.unquote(req.path)
        new_metadata = extract_object_metadata_from_headers(req.headers)

        try:
            head_response = self.rpc_call(ctx, rpc.head_request(path))
            raw_old_metadata, mtime, _, _, inode_number, num_writes = \
                rpc.parse_head_response(head_response)
        except utils.RpcError as err:
            if err.errno == pfs_errno.NotFoundError:
                return swob.HTTPNotFound(request=req)
            else:
                raise

        old_metadata = deserialize_metadata(raw_old_metadata)
        merged_metadata = merge_object_metadata(old_metadata, new_metadata)
        raw_merged_metadata = serialize_metadata(merged_metadata)

        self.rpc_call(ctx, rpc.post_request(
            path, raw_old_metadata, raw_merged_metadata))

        resp = swob.HTTPAccepted(request=req, body="")
        resp.headers["ETag"] = best_possible_etag(
            old_metadata, ctx.account_name, inode_number, num_writes)
        resp.headers["Last-Modified"] = last_modified_from_epoch_ns(mtime)
        return resp

    def get_object(self, ctx):
        req = ctx.req
        byteranges = req.range.ranges if req.range else ()

        try:
            object_response = self.rpc_call(ctx, rpc.get_object_request(
                urllib_parse.unquote(req.path), byteranges))
        except utils.RpcError as err:
            if err.errno == pfs_errno.NotFoundError:
                return swob.HTTPNotFound(request=req)
            elif err.errno == pfs_errno.IsDirError:
                return swob.HTTPOk(
                    request=req, body="",
                    headers={"Content-Type": DIRECTORY_CONTENT_TYPE})

            else:
                # punt to top-level exception handler
                raise

        read_plan, raw_metadata, size, mtime_ns, ino, num_writes, lease_id = \
            rpc.parse_get_object_response(object_response)
        if size > 0 and read_plan is None:
            return swob.HTTPRequestedRangeNotSatisfiable(request=req)

        # NB: this is a size-0 queue, so it acts as a channel: a put()
        # blocks until another greenthread does a get(). This lets us use it
        # for (very limited) bidirectional communication.
        channel = eventlet.queue.Queue(0)
        eventlet.spawn_n(self._keep_lease_alive, ctx, channel, lease_id)

        headers = deserialize_metadata(raw_metadata)
        headers["Last-Modified"] = last_modified_from_epoch_ns(
            mtime_ns)
        headers["X-Timestamp"] = x_timestamp_from_epoch_ns(
            mtime_ns)
        headers["Etag"] = best_possible_etag(headers, ctx.account_name,
                                             ino, num_writes)

        listing_iter = listing_iter_from_read_plan(read_plan)
        # Make sure that nobody (like our __call__ method) messes with this
        # environment once we've started. Otherwise, the auth callback may
        # reappear, causing log-segment GET requests to fail. This may be
        # seen with container ACLs; since the underlying container name
        # differs from the user-presented one, without copying the
        # environment, all object GET requests for objects in a public
        # container would fail.
        copied_req = swob.Request(req.environ.copy())

        # Ideally we'd wrap seg_iter instead, but swob.Response relies on
        # its app_iter supporting certain methods for conditional responses
        # to work, and forwarding all those methods through the wrapper is
        # prone to failure whenever a new method is added.
        #
        # Wrapping the listing iterator is just as good. After
        # SegmentedIterable exhausts it, we can safely release the lease.
        def done_with_object_get():
            channel.put("you can stop now")
            # It's not technically necessary for us to wait for the other
            # greenthread here; we could use one-way notification. However,
            # doing things this way lets us ensure that, once this function
            # returns, there are no more background actions taken by the
            # greenthread. This makes testing a lot easier; we can call the
            # middleware, let it return, and then assert things. Were we to
            # use a fire-and-forget style, we'd never be sure when all the
            # RPCs had been called, and the tests would end up flaky.
            channel.get()

        wrapped_listing_iter = iterator_posthook(
            listing_iter, done_with_object_get)

        seg_iter = swift_code.SegmentedIterable(
            copied_req, self.zero_filler_app, wrapped_listing_iter,
            self.max_get_time,
            self.logger, 'PFS', 'PFS',
            name=req.path)

        return swob.HTTPOk(app_iter=seg_iter, conditional_response=True,
                           request=req,
                           headers=headers,
                           content_length=size)

    def _keep_lease_alive(self, ctx, channel, lease_id):
        keep_going = [True]
        lease_error = [False]

        def renew():
            if lease_error[0]:
                return

            try:
                self.rpc_call(ctx, rpc.renew_lease_request(lease_id))
            except (utils.RpcError, utils.RpcTimeout):
                # If there's an error renewing the lease, stop pestering
                # proxyfsd about it. We'll keep serving the object
                # anyway, and we'll just hope no log segments vanish
                # while we do it.
                keep_going[0] = False
                lease_error[0] = True

        # It could have been a while since this greenthread was created.
        # Let's renew first just to be sure.
        renew()

        while keep_going[0]:
            try:
                channel.get(block=True, timeout=LEASE_RENEWAL_INTERVAL)
                # When we get a message here, we should stop.
                keep_going[0] = False
            except eventlet.queue.Empty:
                # Nobody told us we're done, so renew the lease and loop
                # around again.
                renew()

        if not lease_error[0]:
            # Tell proxyfsd that we're done with the lease, but only if
            # there were no errors keeping it renewed.
            self.rpc_call(ctx, rpc.release_lease_request(lease_id))

        channel.put("alright, it's done")

    def delete_object(self, ctx):
        try:
            self.rpc_call(ctx, rpc.delete_request(
                urllib_parse.unquote(ctx.req.path)))
        except utils.RpcError as err:
            if err.errno == pfs_errno.NotFoundError:
                return swob.HTTPNotFound(request=ctx.req)
            elif err.errno == pfs_errno.NotEmptyError:
                return swob.HTTPConflict(request=ctx.req)
            else:
                raise
        return swob.HTTPNoContent(request=ctx.req)

    def head_object(self, ctx):
        req = ctx.req
        head_request = rpc.head_request(urllib_parse.unquote(req.path))
        try:
            head_response = self.rpc_call(ctx, head_request)
        except utils.RpcError as err:
            if err.errno == pfs_errno.NotFoundError:
                return swob.HTTPNotFound(request=req)
            else:
                raise

        raw_md, last_modified_ns, file_size, is_dir, ino, num_writes = \
            rpc.parse_head_response(head_response)

        headers = swob.HeaderKeyDict(deserialize_metadata(raw_md))

        if "Content-Type" not in headers:
            headers["Content-Type"] = guess_content_type(req.path, is_dir)

        headers["Content-Length"] = file_size
        headers["Accept-Ranges"] = "bytes"
        headers["ETag"] = best_possible_etag(
            headers, ctx.account_name, ino, num_writes)
        headers["Last-Modified"] = last_modified_from_epoch_ns(
            last_modified_ns)
        headers["X-Timestamp"] = x_timestamp_from_epoch_ns(
            last_modified_ns)

        return swob.HTTPOk(request=req, headers=headers)

    def coalesce_object(self, ctx):
        req = ctx.req
        object_path = urllib_parse.unquote(req.path)

        probably_json = req.environ['wsgi.input'].read(
            self.max_coalesce_request_size + 1)

        if len(probably_json) > self.max_coalesce_request_size:
            return swob.HTTPRequestEntityTooLarge(request=req)

        try:
            decoded_json = json.loads(probably_json)
        except ValueError:
            return swob.HTTPBadRequest(request=req, body="Malformed JSON")

        if "elements" not in decoded_json:
            return swob.HTTPBadRequest(request=req, body="Malformed JSON")
        if not isinstance(decoded_json, dict):
            return swob.HTTPBadRequest(request=req, body="Malformed JSON")
        if not isinstance(decoded_json["elements"], list):
            return swob.HTTPBadRequest(request=req, body="Malformed JSON")
        if len(decoded_json["elements"]) > self.max_coalesce:
            return swob.HTTPRequestEntityTooLarge(request=req)
        for elem in decoded_json["elements"]:
            if not isinstance(elem, six.string_types):
                return swob.HTTPBadRequest(request=req, body="Malformed JSON")

        try:
            coalesce_response = self.rpc_call(
                ctx, rpc.coalesce_object_request(
                    object_path, decoded_json["elements"]))
        except utils.RpcError as err:
            if err.errno == pfs_errno.NotFoundError:
                return swob.HTTPNotFound(
                    request=req,
                    headers={"Content-Type": "text/plain"},
                    body="One or more path elements not found")
            elif err.errno in (pfs_errno.NotDirError, pfs_errno.IsDirError):
                return swob.HTTPConflict(
                    request=req,
                    headers={"Content-Type": "text/plain"},
                    body="Elements must be plain files, not directories")
            elif err.errno == pfs_errno.TooManyLinksError:
                return swob.HTTPConflict(
                    request=req,
                    headers={"Content-Type": "text/plain"},
                    body=("One or more path elements has multiple links; "
                          "only singly-linked files can be combined"))
            else:
                raise

        last_modified_ns, ino, num_writes = \
            rpc.parse_coalesce_object_response(coalesce_response)

        headers = {}
        headers["Etag"] = construct_etag(ctx.account_name, ino, num_writes)
        headers["Last-Modified"] = last_modified_from_epoch_ns(
            last_modified_ns)
        headers["X-Timestamp"] = x_timestamp_from_epoch_ns(
            last_modified_ns)

        return swob.HTTPCreated(request=req, headers=headers)

    def _unpack_owning_proxyfs(self, req):
        """
        Checks to see if an account is bimodal or not, and if so, which proxyfs
        daemon is responsible for it.

        This is done by looking in the request environment; there's another
        middleware (BimodalChecker) that populates these fields.

        :returns: 2-tuple (is-bimodal, proxyfsd-addrinfo).
        """

        return (req.environ.get(utils.ENV_IS_BIMODAL),
                req.environ.get(utils.ENV_OWNING_PROXYFS))

    def rpc_call(self, ctx, rpc_request):
        """
        Call a remote procedure in proxyfsd.

        :param ctx: context for the current HTTP request

        :param rpc_request: Python dictionary containing the request
            (method, args, etc.) in JSON-RPC format.

        :returns: the result of the RPC, whatever that looks like

        :raises: utils.RpcTimeout if the RPC takes too long

        :raises: utils.RpcError if the RPC returns an error. Inspecting this
            exception's "errno" attribute may be useful. However, errno may
            not always be set; if the error returned from proxyfsd does not
            have an errno in it, then the exception's errno attribute will
            be None.
        """
        return self._rpc_call([ctx.proxyfsd_addrinfo], rpc_request)

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
