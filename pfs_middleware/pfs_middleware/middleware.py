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
import uuid
import xml.etree.ElementTree as ET
from six.moves.urllib import parse as urllib_parse
from io import BytesIO

from swift.common.middleware.acl import parse_acl, format_acl
from . import pfs_errno, rpc, swift_code, utils

# Generally speaking, let's try to keep the use of Swift code to a
# reasonable level. Using dozens of random functions from swift.common.utils
# will ensure that this middleware breaks with every new Swift release. On
# the other hand, there's some really good, actually-works-in-production
# code in Swift that does things we need.

# Were we to make an account HEAD request instead of calling
# get_account_info, we'd lose the benefit of Swift's caching. This would
# slow down requests *a lot*. Same for containers.
from swift.proxy.controllers.base import (
    get_account_info, get_container_info, clear_info_cache)

# Plain WSGI is annoying to work with, and nobody wants a dependency on
# webob.
from swift.common import swob, constraints

# Our logs should go to the same place as everyone else's. Plus, this logger
# works well in an eventlet-ified process, and SegmentedIterable needs one.
from swift.common.utils import config_true_value, get_logger, Timestamp


# POSIX file-path limits. Taken from Linux's limits.h, which is also where
# ProxyFS gets them.
NAME_MAX = 255
PATH_MAX = 4096

# Used for content type of directories in container listings
DIRECTORY_CONTENT_TYPE = "application/directory"

ZERO_FILL_PATH = "/0"

LEASE_RENEWAL_INTERVAL = 5  # seconds

# Beware: ORIGINAL_MD5_HEADER is random case, not title case, but is
# stored on-disk just as defined here.  Care must be taken when comparing
# it to incoming headers which are title case.
ORIGINAL_MD5_HEADER = "X-Object-Sysmeta-ProxyFS-Initial-MD5"
S3API_ETAG_HEADER = "X-Object-Sysmeta-S3Api-Etag"
LISTING_ETAG_OVERRIDE_HEADER = \
    "X-Object-Sysmeta-Container-Update-Override-Etag"

# They don't start with X-Object-(Meta|Sysmeta)-, but we save them anyway.
SPECIAL_OBJECT_METADATA_HEADERS = {
    "Content-Type",
    "Content-Disposition",
    "Content-Encoding",
    "X-Object-Manifest",
    "X-Static-Large-Object"}

# These are not mutated on object POST.
STICKY_OBJECT_METADATA_HEADERS = {
    "X-Static-Large-Object",
    ORIGINAL_MD5_HEADER}

SPECIAL_CONTAINER_METADATA_HEADERS = {
    "X-Container-Read",
    "X-Container-Write",
    "X-Container-Sync-Key",
    "X-Container-Sync-To",
    "X-Versions-Location"}

# ProxyFS directories don't know how many objects are under them, nor how
# many bytes each one uses. (Yes, a directory knows how many files and
# subdirectories it contains, but that doesn't include things in those
# subdirectories.)
CONTAINER_HEADERS_WE_LIE_ABOUT = {
    "X-Container-Object-Count": "0",
    "X-Container-Bytes-Used": "0",
}

SWIFT_OWNER_HEADERS = {
    "X-Container-Read",
    "X-Container-Write",
    "X-Container-Sync-Key",
    "X-Container-Sync-To",
    "X-Account-Meta-Temp-Url-Key",
    "X-Account-Meta-Temp-Url-Key-2",
    "X-Container-Meta-Temp-Url-Key",
    "X-Container-Meta-Temp-Url-Key-2",
    "X-Account-Access-Control"}

MD5_ETAG_RE = re.compile("^[a-f0-9]{32}$")

EMPTY_OBJECT_ETAG = "d41d8cd98f00b204e9800998ecf8427e"

RPC_TIMEOUT_DEFAULT = 30.0
MAX_RPC_BODY_SIZE = 2 ** 20


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
    Convert a Unix timestamp to an IMF-Fixdate timestamp.

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


def should_validate_etag(a_string):
    if not a_string:
        return False
    return not a_string.strip('"').startswith('pfsv')


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
    else:
        hsh.pop(key, None)


def deserialize_metadata(raw_metadata):
    """Deserialize JSON-encoded metadata to WSGI strings"""
    if raw_metadata:
        try:
            metadata = json.loads(raw_metadata)
        except ValueError:
            metadata = {}
    else:
        metadata = {}
    encoded_metadata = {}
    for k, v in metadata.items():
        if six.PY2:
            key = k.encode('utf8') if isinstance(
                k, six.text_type) else str(k)
            value = v.encode('utf8') if isinstance(
                v, six.text_type) else str(v)
        else:
            key = swift_code.str_to_wsgi(k) if isinstance(k, str) else str(k)
            value = swift_code.str_to_wsgi(v) if isinstance(v, str) else str(v)
        encoded_metadata[key] = value
    return encoded_metadata


def serialize_metadata(wsgi_metadata):
    return json.dumps({
        swift_code.wsgi_to_str(key): (
            swift_code.wsgi_to_str(value)
            if isinstance(value, six.string_types) else value)
        for key, value in wsgi_metadata.items()})


def merge_container_metadata(old, new):
    merged = old.copy()
    for k, v in new.items():
        merged[k] = v
    return {k: v for k, v in merged.items() if v}


def merge_object_metadata(old, new):
    '''
    Merge the existing metadata for an object with new metadata passed
    in as a result of a POST operation.  X-Object-Sysmeta- and similar
    metadata cannot be changed by a POST.
    '''

    merged = new.copy()

    for header, value in merged.items():
        if (header.startswith("X-Object-Sysmeta-") or
                header in STICKY_OBJECT_METADATA_HEADERS):
            del merged[header]

    for header, value in old.items():
        if (header.startswith("X-Object-Sysmeta-") or
                header in STICKY_OBJECT_METADATA_HEADERS):
            merged[header] = value

    old_ct = old.get("Content-Type")
    new_ct = new.get("Content-Type")
    if old_ct is not None:
        if not new_ct:
            merged["Content-Type"] = old_ct
        elif ';swift_bytes=' in old_ct:
            merged["Content-Type"] = '%s;swift_bytes=%s' % (
                new_ct, old_ct.rsplit(';swift_bytes=', 1)[1])

    return {k: v for k, v in merged.items() if v}


def extract_object_metadata_from_headers(headers):
    """
    Find and return the key/value pairs containing object metadata.

    This tries to do the same thing as the Swift object server: save only
    relevant headers. If the user sends in "X-Fungus-Amungus: shroomy" in
    the PUT request's headers, we'll ignore it, just like plain old Swift
    would.

    :param headers: request headers (a dictionary)

    :returns: dictionary containing object-metadata headers (and not a
    swob.HeaderKeyDict or similar object)
    """
    meta_headers = {}
    for header, value in headers.items():
        header = header.title()

        if (header.startswith("X-Object-Meta-") or
                header.startswith("X-Object-Sysmeta-") or
                header in SPECIAL_OBJECT_METADATA_HEADERS):

            # do not let a client pass in ORIGINAL_MD5_HEADER
            if header not in (ORIGINAL_MD5_HEADER,
                              ORIGINAL_MD5_HEADER.title()):
                meta_headers[header] = value

    return meta_headers


def extract_container_metadata_from_headers(req):
    """
    Find and return the key/value pairs containing container metadata.

    This tries to do the same thing as the Swift container server: save only
    relevant headers. If the user sends in "X-Fungus-Amungus: shroomy" in
    the PUT request's headers, we'll ignore it, just like plain old Swift
    would.

    :param req: a swob Request

    :returns: dictionary containing container-metadata headers
    """
    meta_headers = {}
    for header, value in req.headers.items():
        header = header.title()

        if ((header.startswith("X-Container-Meta-") or
                header.startswith("X-Container-Sysmeta-") or
                header in SPECIAL_CONTAINER_METADATA_HEADERS) and
                (req.environ.get('swift_owner', False) or
                 header not in SWIFT_OWNER_HEADERS)):
            meta_headers[header] = value

        if header.startswith("X-Remove-"):
            header = header.replace("-Remove", "", 1)
            if ((header.startswith("X-Container-Meta-") or
                    header in SPECIAL_CONTAINER_METADATA_HEADERS) and
                    (req.environ.get('swift_owner', False) or
                     header not in SWIFT_OWNER_HEADERS)):
                meta_headers[header] = ""
    return meta_headers


def mung_etags(obj_metadata, etag, num_writes):
    '''
    Mung the ETag headers that will be stored with an object.  The
    goal is to preserve ETag metadata passed down by other filters but
    to do so in such a way that it will be invalidated if there is a
    write to or truncate of the object via the ProxyFS file API.

    The mechanism is to prepend a counter to the ETag header values
    that is incremented each time the object is modified is modified.
    When the object is read, if the value for the counter has changed,
    the ETag is assumed to be invalid.  The counter is typically the
    number of writes to the object.

    etag is either None or the value that should be returned as the
    ETag for the object (in the absence of other considerations).

    This assumes that all headers have been converted to "titlecase",
    except ORIGINAL_MD5_HEADER which is the random case string
    "X-Object-Sysmeta-ProxyFS-Initial-MD5".

    This ignores SLO headers because it assumes they have already been
    stripped.
    '''
    if LISTING_ETAG_OVERRIDE_HEADER in obj_metadata:
        obj_metadata[LISTING_ETAG_OVERRIDE_HEADER] = "%d:%s" % (
            num_writes, obj_metadata[LISTING_ETAG_OVERRIDE_HEADER])

    if S3API_ETAG_HEADER in obj_metadata:
        obj_metadata[S3API_ETAG_HEADER] = "%d:%s" % (
            num_writes, obj_metadata[S3API_ETAG_HEADER])

    if etag is not None:
        obj_metadata[ORIGINAL_MD5_HEADER] = "%d:%s" % (num_writes, etag)

    return


def unmung_etags(obj_metadata, num_writes):
    '''
    Unmung the ETag headers associated with an object to return
    them to the state they were in when passed to pfs_middleware.

    Delete them if the object has changed or the header value
    does not parse correctly.

    This assumes that all headers have been converted to "titlecase",
    which means, among other things, that "ETag" will show up as
    "Etag".
    '''

    # if the header is invalid or stale it is not added back after the pop
    if LISTING_ETAG_OVERRIDE_HEADER in obj_metadata:
        val = obj_metadata.pop(LISTING_ETAG_OVERRIDE_HEADER)
        try:
            stored_num_writes, rest = val.split(':', 1)
            if int(stored_num_writes) == num_writes:
                obj_metadata[LISTING_ETAG_OVERRIDE_HEADER] = rest
        except ValueError:
            pass

    if S3API_ETAG_HEADER in obj_metadata:
        val = obj_metadata.pop(S3API_ETAG_HEADER)
        try:
            stored_num_writes, rest = val.split(':', 1)
            if int(stored_num_writes) == num_writes:
                obj_metadata[S3API_ETAG_HEADER] = rest
        except ValueError:
            pass

    if ORIGINAL_MD5_HEADER in obj_metadata:
        val = obj_metadata.pop(ORIGINAL_MD5_HEADER)
        try:
            stored_num_writes, rest = val.split(':', 1)
            if int(stored_num_writes) == num_writes:
                obj_metadata[ORIGINAL_MD5_HEADER] = rest
        except ValueError:
            pass


def best_possible_etag(obj_metadata, account_name, inum, num_writes,
                       is_dir=False, container_listing=False):
    '''
    Return the ETag that is most likely to be correct for the query,
    but leave other valid ETags values in the metadata, in case a
    higher layer filter wants to use them to override the value
    returned here.

    If the ETags in the metadata are invalid, construct and return a
    new ProxyFS ETag based on the account name, inode number, and
    number of writes.

    obj_metadata may be a Python dictionary, a swob.HeaderKeyDict, or a
    swob.HeaderEnvironProxy.  ORIGINAL_MD5_HEADER is random case, not
    title case, but if obj_metadata is a Python dictionary it will
    preserve the same random case.  The other two types do case folding so
    we don't need to map ORIGINAL_MD5_HEADER to title case.
    '''
    if is_dir:
        return EMPTY_OBJECT_ETAG

    if container_listing and LISTING_ETAG_OVERRIDE_HEADER in obj_metadata:
        return obj_metadata[LISTING_ETAG_OVERRIDE_HEADER]

    if ORIGINAL_MD5_HEADER in obj_metadata:
        return obj_metadata[ORIGINAL_MD5_HEADER]

    return construct_etag(account_name, inum, num_writes)


def construct_etag(account_name, inum, num_writes):
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
        urllib_parse.quote(account_name), inum, num_writes)


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
    ZEROES = b"\x00" * 4096

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
        self._peeked_data = b""
        self.limit = self.orig_limit = limit
        self.bytes_read = 0
        self.wsgi_input = wsgi_input

    def read(self, length=None, *args, **kwargs):
        if length is None:
            to_read = self.limit
        else:
            to_read = min(self.limit, length)
        to_read -= len(self._peeked_data)

        chunk = self.wsgi_input.read(to_read, *args, **kwargs)
        chunk = self._peeked_data + chunk
        self._peeked_data = b""

        self.bytes_read += len(chunk)
        self.limit -= len(chunk)
        return chunk

    def readline(self, size=None, *args, **kwargs):
        if size is None:
            to_read = self.limit
        else:
            to_read = min(self.limit, size)
        to_read -= len(self._peeked_data)

        line = self.wsgi_input.readline(to_read, *args, **kwargs)
        line = self._peeked_data + line
        self._peeked_data = b""

        self.bytes_read += len(line)
        self.limit -= len(line)
        return line

    @property
    def has_more_to_read(self):
        if not self._peeked_data:
            self._peeked_data = self.wsgi_input.read(1)
        return len(self._peeked_data) > 0


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

        self.proxyfsd_rpc_timeout = float(conf.get('rpc_timeout',
                                                   RPC_TIMEOUT_DEFAULT))
        self.bimodal_recheck_interval = float(conf.get(
            'bimodal_recheck_interval', '60.0'))
        self.max_get_time = int(conf.get('max_get_time', '86400'))
        self.max_log_segment_size = int(conf.get(
            'max_log_segment_size', '2147483648'))  # 2 GiB
        self.max_coalesce = int(conf.get('max_coalesce', '1000'))

        # Assume a max object length of the Swift default of 1024 bytes plus
        # a few extra for JSON quotes, commas, et cetera.
        self.max_coalesce_request_size = self.max_coalesce * 1100

        self.bypass_mode = conf.get('bypass_mode', 'off')
        if self.bypass_mode not in ('off', 'read-only', 'read-write'):
            raise ValueError('Expected bypass_mode to be one of off, '
                             'read-only, or read-write')

    @swob.wsgify
    def __call__(self, req):
        vrs, acc, con, obj = utils.parse_path(req.path)

        # The only way to specify bypass mode: /proxyfs/AUTH_acct/...
        proxyfs_path = False
        if vrs == 'proxyfs':
            proxyfs_path = True
            req.path_info = req.path_info.replace('/proxyfs/', '/v1/', 1)
            vrs = 'v1'

        if not acc or not constraints.valid_api_version(vrs) or (
                obj and not con):
            # could be a GET /info request or something made up by some
            # other middleware; get out of the way.
            return self.app
        if not constraints.check_utf8(req.path_info):
            return swob.HTTPPreconditionFailed(
                body='Invalid UTF8 or contains NULL')

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

            if con in ('.', '..') or con and len(con) > NAME_MAX:
                if req.method == 'PUT' and not obj:
                    return swob.HTTPBadRequest(
                        request=req,
                        body='Container name cannot be "." or "..", '
                             'or be more than 255 bytes long')
                else:
                    return swob.HTTPNotFound(request=req)
            elif obj and any(p in ('', '.', '..') or len(p) > NAME_MAX
                             for p in obj.split('/')):
                if req.method == 'PUT':
                    return swob.HTTPBadRequest(
                        request=req,
                        body='No path component may be "", ".", "..", or '
                             'more than 255 bytes long')
                else:
                    return swob.HTTPNotFound(request=req)

            ctx = RequestContext(req, proxyfsd_addrinfo, acc, con, obj)
            is_bypass_request = (
                proxyfs_path and
                self.bypass_mode in ('read-only', 'read-write') and
                req.method != "PROXYFS")

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
                                    False):
                if auth_cb and req.environ.get('swift.source') != 'PFS':
                    if not is_bypass_request:
                        req.acl = self._fetch_appropriate_acl(ctx)
                    # else, user needs to be swift owner
                    denial_response = auth_cb(ctx.req)
                    if denial_response:
                        return denial_response

                # Authorization succeeded
                method = req.method

                # Check whether we ought to bypass. Note that swift_owner
                # won't be set until we call authorize
                if is_bypass_request and req.environ.get('swift_owner'):
                    if self.bypass_mode == 'read-only' and method not in (
                            'GET', 'HEAD'):
                        return swob.HTTPMethodNotAllowed(request=req)
                    return self.app

                # Otherwise, dispatch to a helper method
                if method == 'GET' and obj:
                    resp = self.get_object(ctx)
                elif method == 'HEAD' and obj:
                    resp = self.head_object(ctx)
                elif method == 'PUT' and obj:
                    resp = self.put_object(ctx)
                elif method == 'POST' and obj:
                    resp = self.post_object(ctx)
                elif method == 'DELETE' and obj:
                    resp = self.delete_object(ctx)
                elif method == 'COALESCE' and obj:
                    resp = self.coalesce_object(ctx, auth_cb)
                elif method == 'GET' and con:
                    resp = self.get_container(ctx)
                elif method == 'HEAD' and con:
                    resp = self.head_container(ctx)
                elif method == 'PUT' and con:
                    resp = self.put_container(ctx)
                elif method == 'POST' and con:
                    resp = self.post_container(ctx)
                elif method == 'DELETE' and con:
                    resp = self.delete_container(ctx)

                elif method == 'GET':
                    resp = self.get_account(ctx)
                elif method == 'HEAD':
                    resp = self.head_account(ctx)
                elif method == 'PROXYFS' and not con and not obj:
                    if not (req.environ.get('swift_owner') and
                            self.bypass_mode in ('read-only', 'read-write')):
                        return swob.HTTPMethodNotAllowed(request=req)
                    resp = self.proxy_rpc(ctx)
                # account PUT, POST, and DELETE are just passed
                # through to Swift
                else:
                    return self.app

                if req.method in ('GET', 'HEAD'):
                    resp.headers["Accept-Ranges"] = "bytes"
                if not req.environ.get('swift_owner', False):
                    for key in SWIFT_OWNER_HEADERS:
                        if key in resp.headers:
                            del resp.headers[key]

                return resp

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

        * for object COALESCE, it's the container's write ACL
          (X-Container-Write). Separately, we *also* need to authorize
          all the "segments" against both read *and* write ACLs
          (see coalesce_object).

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
        elif ctx.object_name and ctx.req.method in (
                'PUT', 'POST', 'DELETE', 'COALESCE'):
            container_info = get_container_info(
                ctx.req.environ, bimodal_checker,
                swift_source="PFS")
            return container_info['write_acl']
        else:
            return None

    def proxy_rpc(self, ctx):
        req = ctx.req
        ct = req.headers.get('Content-Type')
        if not ct or ct.split(';', 1)[0].strip() != 'application/json':
            msg = 'RPC body must have Content-Type application/json'
            if ct:
                msg += ', not %s' % ct
            return swob.Response(status=415, request=req, body=msg)

        cl = req.content_length
        if cl is None:
            if req.headers.get('Transfer-Encoding') != 'chunked':
                return swob.HTTPLengthRequired(request=req)
            json_payloads = req.body_file.read(MAX_RPC_BODY_SIZE).split(b'\n')
            if req.body_file.read(1):
                return swob.HTTPRequestEntityTooLarge(request=req)
        else:
            if cl > MAX_RPC_BODY_SIZE:
                return swob.HTTPRequestEntityTooLarge(request=req)
            json_payloads = req.body.split(b'\n')

        if self.bypass_mode == 'read-write':
            allowed_methods = rpc.allow_read_write
        else:
            allowed_methods = rpc.allow_read_only

        payloads = []
        for i, json_payload in enumerate(x for x in json_payloads
                                         if x.strip()):
            try:
                payload = json.loads(json_payload.decode('utf8'))
                if payload['jsonrpc'] != '2.0':
                    raise ValueError(
                        'expected JSONRPC 2.0, got %s' % payload['jsonrpc'])
                if not isinstance(payload['method'], six.string_types):
                    raise ValueError(
                        'expected string, got %s' % type(payload['method']))
                if payload['method'] not in allowed_methods:
                    raise ValueError(
                        'method %s not allowed' % payload['method'])
                if not (isinstance(payload['params'], list) and
                        len(payload['params']) == 1 and
                        isinstance(payload['params'][0], dict)):
                    raise ValueError
            except (TypeError, KeyError, ValueError) as err:
                return swob.HTTPBadRequest(
                    request=req,
                    body=(b'Could not parse/validate JSON payload #%d %s: %s' %
                          (i, json_payload, str(err).encode('utf8'))))
            payloads.append(payload)

        # TODO: consider allowing more than one payload per request
        if len(payloads) != 1:
            return swob.HTTPBadRequest(
                request=req,
                body='Expected exactly one JSON payload')

        # Our basic validation is done; spin up a connection and send requests
        client = utils.JsonRpcClient(ctx.proxyfsd_addrinfo)
        payload = payloads[0]
        try:
            if 'id' not in payload:
                payload['id'] = str(uuid.uuid4())
            payload['params'][0]['AccountName'] = ctx.account_name
            response = client.call(payload, self.proxyfsd_rpc_timeout,
                                   raise_on_rpc_error=False)
        except utils.RpcTimeout as err:
            self.logger.debug(str(err))
            return swob.HTTPBadGateway(request=req)
        except socket.error as err:
            self.logger.debug("Error communicating with %r: %s.",
                              ctx.proxyfsd_addrinfo, err)
            return swob.HTTPBadGateway(request=req)
        else:
            return swob.HTTPOk(
                request=req, body=json.dumps(response),
                headers={'Content-Type': 'application/json'})

    def get_account(self, ctx):
        req = ctx.req
        limit = self._get_listing_limit(
            req, self._default_account_listing_limit())
        marker = req.params.get('marker', '')
        end_marker = req.params.get('end_marker', '')
        get_account_request = rpc.get_account_request(
            urllib_parse.unquote(req.path), marker, end_marker, limit)
        # If the account does not exist, then __call__ just falls through to
        # self.app, so we never even get here. If we got here, then the
        # account does exist, so we don't have to worry about not-found
        # versus other-error here. Any error counts as "completely busted".
        #
        # We let the top-level RpcError handler catch this.
        get_account_response = self.rpc_call(ctx, get_account_request)
        account_mtime, account_entries = rpc.parse_get_account_response(
            get_account_response)

        resp_content_type = swift_code.get_listing_content_type(req)
        if resp_content_type == "text/plain":
            body = self._plaintext_account_get_response(account_entries)
        elif resp_content_type == "application/json":
            body = self._json_account_get_response(account_entries)
        elif resp_content_type.endswith("/xml"):
            body = self._xml_account_get_response(account_entries,
                                                  ctx.account_name)
        else:
            raise Exception("unexpected content type %r" %
                            (resp_content_type,))

        resp_class = swob.HTTPOk if body else swob.HTTPNoContent
        resp = resp_class(content_type=resp_content_type, charset="utf-8",
                          request=req, body=body)

        # For accounts, the meta/sysmeta is stored in the account DB in
        # Swift, not in ProxyFS.
        account_info = get_account_info(
            req.environ, req.environ[utils.ENV_BIMODAL_CHECKER],
            swift_source="PFS")
        for key, value in account_info["meta"].items():
            resp.headers["X-Account-Meta-" + key] = value
        for key, value in account_info["sysmeta"].items():
            resp.headers["X-Account-Sysmeta-" + key] = value
        acc_acl = resp.headers.get("X-Account-Sysmeta-Core-Access-Control")
        parsed_acc_acl = parse_acl(version=2, data=acc_acl)
        if parsed_acc_acl:
            acc_acl = format_acl(version=2, acl_dict=parsed_acc_acl)
            resp.headers["X-Account-Access-Control"] = acc_acl

        resp.headers["X-Timestamp"] = x_timestamp_from_epoch_ns(account_mtime)

        # Pretend the object counts are 0 and that all containers have the
        # default storage policy. Until (a) containers have some support for
        # the X-Storage-Policy header, and (b) we get container metadata
        # back from Server.RpcGetAccount, this is the best we can do.
        policy = self._default_storage_policy()
        resp.headers["X-Account-Object-Count"] = "0"
        resp.headers["X-Account-Bytes-Used"] = "0"
        resp.headers["X-Account-Container-Count"] = str(len(account_entries))

        resp.headers["X-Account-Storage-Policy-%s-Object-Count" % policy] = "0"
        resp.headers["X-Account-Storage-Policy-%s-Bytes-Used" % policy] = "0"
        k = "X-Account-Storage-Policy-%s-Container-Count" % policy
        resp.headers[k] = str(len(account_entries))

        return resp

    def _plaintext_account_get_response(self, account_entries):
        chunks = []
        for entry in account_entries:
            chunks.append(entry["Basename"].encode('utf-8'))
            chunks.append(b"\n")
        return b''.join(chunks)

    def _json_account_get_response(self, account_entries):
        json_entries = []
        for entry in account_entries:
            json_entry = {
                "name": entry["Basename"],
                # Older versions of proxyfsd only returned mtime, but ctime
                # better reflects the semantics of X-Timestamp
                "last_modified": iso_timestamp_from_epoch_ns(entry.get(
                    "AttrChangeTime", entry["ModificationTime"])),
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

    def _xml_account_get_response(self, account_entries, account_name):
        root_node = ET.Element('account', name=account_name)

        for entry in account_entries:
            container_node = ET.Element('container')

            name_node = ET.Element('name')
            name_node.text = entry["Basename"]
            container_node.append(name_node)

            count_node = ET.Element('count')
            count_node.text = '0'
            container_node.append(count_node)

            bytes_node = ET.Element('bytes')
            bytes_node.text = '0'
            container_node.append(bytes_node)

            lm_node = ET.Element('last_modified')
            # Older versions of proxyfsd only returned mtime, but ctime
            # better reflects the semantics of X-Timestamp
            lm_node.text = iso_timestamp_from_epoch_ns(entry.get(
                "AttrChangeTime", entry["ModificationTime"]))
            container_node.append(lm_node)

            root_node.append(container_node)

        buf = BytesIO()
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

        raw_metadata, mtime_ns, _, _, _, _ = rpc.parse_head_response(
            head_response)
        metadata = deserialize_metadata(raw_metadata)
        resp = swob.HTTPNoContent(request=ctx.req, headers=metadata)
        resp.headers["X-Timestamp"] = x_timestamp_from_epoch_ns(mtime_ns)
        resp.headers["Last-Modified"] = last_modified_from_epoch_ns(mtime_ns)
        resp.headers["Content-Type"] = swift_code.get_listing_content_type(
            ctx.req)
        resp.charset = "utf-8"
        self._add_required_container_headers(resp)
        return resp

    def put_container(self, ctx):
        req = ctx.req
        container_path = urllib_parse.unquote(req.path)
        err = constraints.check_metadata(req, 'container')
        if err:
            return err
        err = swift_code.clean_acls(req)
        if err:
            return err
        new_metadata = extract_container_metadata_from_headers(req)

        # Check name's length. The account name is checked separately (by
        # Swift, not by this middleware) and has its own limit; we are
        # concerned only with the container portion of the path.
        _, _, container_name, _ = utils.parse_path(req.path)
        maxlen = self._max_container_name_length()
        if len(container_name) > maxlen:
            return swob.HTTPBadRequest(
                request=req,
                body=('Container name length of %d longer than %d' %
                      (len(container_name), maxlen)))

        try:
            head_response = self.rpc_call(
                ctx, rpc.head_request(container_path))
            raw_old_metadata, _, _, _, _, _ = rpc.parse_head_response(
                head_response)
        except utils.RpcError as err:
            if err.errno == pfs_errno.NotFoundError:
                clear_info_cache(None, ctx.req.environ, ctx.account_name,
                                 container=ctx.container_name)
                self.rpc_call(ctx, rpc.put_container_request(
                    container_path,
                    "",
                    serialize_metadata({
                        k: v for k, v in new_metadata.items() if v})))
                return swob.HTTPCreated(request=req)
            else:
                raise

        old_metadata = deserialize_metadata(raw_old_metadata)
        merged_metadata = merge_container_metadata(
            old_metadata, new_metadata)
        raw_merged_metadata = serialize_metadata(merged_metadata)

        clear_info_cache(None, ctx.req.environ, ctx.account_name,
                         container=ctx.container_name)
        self.rpc_call(ctx, rpc.put_container_request(
            container_path, raw_old_metadata, raw_merged_metadata))

        return swob.HTTPAccepted(request=req)

    def post_container(self, ctx):
        req = ctx.req
        container_path = urllib_parse.unquote(req.path)
        err = constraints.check_metadata(req, 'container')
        if err:
            return err
        err = swift_code.clean_acls(req)
        if err:
            return err
        new_metadata = extract_container_metadata_from_headers(req)

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

        # Check that we're still within overall limits
        req.headers.clear()
        req.headers.update(merged_metadata)
        err = constraints.check_metadata(req, 'container')
        if err:
            return err
        # reset it...
        req.headers.clear()
        req.headers.update(new_metadata)

        clear_info_cache(None, req.environ, ctx.account_name,
                         container=ctx.container_name)
        self.rpc_call(ctx, rpc.post_request(
            container_path, raw_old_metadata, raw_merged_metadata))

        return swob.HTTPNoContent(request=req)

    def delete_container(self, ctx):
        # Turns out these are the same RPC with the same error handling, so
        # why not?
        clear_info_cache(None, ctx.req.environ, ctx.account_name,
                         container=ctx.container_name)
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

    def _max_container_name_length(self):
        proxy_info = self._proxy_info()
        swift_max = proxy_info["swift"]["max_container_name_length"]
        return min(swift_max, NAME_MAX)

    def _default_account_listing_limit(self):
        proxy_info = self._proxy_info()
        return proxy_info["swift"]["account_listing_limit"]

    def _default_container_listing_limit(self):
        proxy_info = self._proxy_info()
        return proxy_info["swift"]["container_listing_limit"]

    def _default_storage_policy(self):
        proxy_info = self._proxy_info()
        # Swift guarantees that exactly one default storage policy exists.
        return [pol["name"]
                for pol in proxy_info["swift"]["policies"]
                if pol.get("default", False)][0]

    def _proxy_info(self):
        if self._cached_proxy_info is None:
            req = swob.Request.blank("/info")
            resp = req.get_response(self.app)
            self._cached_proxy_info = json.loads(resp.body)
        return self._cached_proxy_info

    def _add_required_container_headers(self, resp):
        resp.headers.update(CONTAINER_HEADERS_WE_LIE_ABOUT)
        resp.headers["X-Storage-Policy"] = self._default_storage_policy()

    def get_container(self, ctx):
        req = ctx.req
        if req.environ.get('swift.source') in ('DLO', 'SW', 'VW'):
            # Middlewares typically want json, but most *assume* it following
            # https://github.com/openstack/swift/commit/4806434
            # TODO: maybe replace with `if req.environ.get('swift.source'):` ??
            params = req.params
            params['format'] = 'json'
            req.params = params

        limit = self._get_listing_limit(
            req, self._default_container_listing_limit())
        marker = req.params.get('marker', '')
        end_marker = req.params.get('end_marker', '')
        prefix = req.params.get('prefix', '')
        delimiter = req.params.get('delimiter', '')
        # For now, we only support "/" as a delimiter
        if delimiter not in ("", "/"):
            return swob.HTTPBadRequest(request=req)
        get_container_request = rpc.get_container_request(
            urllib_parse.unquote(req.path),
            marker, end_marker, limit, prefix, delimiter)
        try:
            get_container_response = self.rpc_call(ctx, get_container_request)
        except utils.RpcError as err:
            if err.errno == pfs_errno.NotFoundError:
                return swob.HTTPNotFound(request=req)
            else:
                raise

        container_ents, raw_metadata, mtime_ns = \
            rpc.parse_get_container_response(get_container_response)

        resp_content_type = swift_code.get_listing_content_type(req)
        resp = swob.HTTPOk(content_type=resp_content_type, charset="utf-8",
                           request=req)
        if resp_content_type == "text/plain":
            resp.body = self._plaintext_container_get_response(
                container_ents)
        elif resp_content_type == "application/json":
            resp.body = self._json_container_get_response(
                container_ents, ctx.account_name, delimiter)
        elif resp_content_type.endswith("/xml"):
            resp.body = self._xml_container_get_response(
                container_ents, ctx.account_name, ctx.container_name)
        else:
            raise Exception("unexpected content type %r" %
                            (resp_content_type,))

        metadata = deserialize_metadata(raw_metadata)
        resp.headers.update(metadata)
        self._add_required_container_headers(resp)
        resp.headers["X-Timestamp"] = x_timestamp_from_epoch_ns(mtime_ns)
        resp.headers["Last-Modified"] = last_modified_from_epoch_ns(mtime_ns)

        return resp

    def _plaintext_container_get_response(self, container_entries):
        chunks = []
        for ent in container_entries:
            chunks.append(ent["Basename"].encode('utf-8'))
            chunks.append(b"\n")
        return b''.join(chunks)

    def _json_container_get_response(self, container_entries, account_name,
                                     delimiter):
        json_entries = []
        for ent in container_entries:
            name = ent["Basename"]
            size = ent["FileSize"]
            # Older versions of proxyfsd only returned mtime, but ctime
            # better reflects the semantics of X-Timestamp
            last_modified = iso_timestamp_from_epoch_ns(ent.get(
                "AttrChangeTime", ent["ModificationTime"]))
            obj_metadata = deserialize_metadata(ent["Metadata"])
            unmung_etags(obj_metadata, ent["NumWrites"])

            content_type = swift_code.wsgi_to_str(
                obj_metadata.get("Content-Type"))
            if content_type is None:
                content_type = guess_content_type(ent["Basename"],
                                                  ent["IsDir"])
            content_type, swift_bytes = content_type.partition(
                ';swift_bytes=')[::2]

            etag = best_possible_etag(
                obj_metadata, account_name,
                ent["InodeNumber"], ent["NumWrites"], is_dir=ent["IsDir"],
                container_listing=True)
            json_entry = {
                "name": name,
                "bytes": int(swift_bytes or size),
                "content_type": content_type,
                "hash": etag,
                "last_modified": last_modified}
            json_entries.append(json_entry)

            if delimiter != "" and "IsDir" in ent and ent["IsDir"]:
                json_entries.append({"subdir": ent["Basename"] + delimiter})

        return json.dumps(json_entries).encode('ascii')

    # TODO: This method is usually non reachable, because at some point in the
    # pipeline, we convert JSON to XML. We should either remove this or update
    # it to support delimiters in case it's really needed.
    # Same thing probably applies to plain text responses.
    def _xml_container_get_response(self, container_entries, account_name,
                                    container_name):
        root_node = ET.Element('container', name=container_name)

        for container_entry in container_entries:
            obj_name = container_entry['Basename']
            obj_metadata = deserialize_metadata(container_entry["Metadata"])
            unmung_etags(obj_metadata, container_entry["NumWrites"])

            content_type = swift_code.wsgi_to_str(
                obj_metadata.get("Content-Type"))
            if content_type is None:
                content_type = guess_content_type(
                    container_entry["Basename"], container_entry["IsDir"])
            content_type, swift_bytes = content_type.partition(
                ';swift_bytes=')[::2]

            etag = best_possible_etag(
                obj_metadata, account_name,
                container_entry["InodeNumber"],
                container_entry["NumWrites"],
                is_dir=container_entry["IsDir"])
            container_node = ET.Element('object')

            name_node = ET.Element('name')
            name_node.text = obj_name
            container_node.append(name_node)

            hash_node = ET.Element('hash')
            hash_node.text = etag
            container_node.append(hash_node)

            bytes_node = ET.Element('bytes')
            bytes_node.text = swift_bytes or str(container_entry["FileSize"])
            container_node.append(bytes_node)

            ct_node = ET.Element('content_type')
            if six.PY2:
                ct_node.text = content_type.decode('utf-8')
            else:
                ct_node.text = content_type
            container_node.append(ct_node)

            lm_node = ET.Element('last_modified')
            # Older versions of proxyfsd only returned mtime, but ctime
            # better reflects the semantics of X-Timestamp
            lm_node.text = iso_timestamp_from_epoch_ns(container_entry.get(
                "AttrChangeTime", container_entry["ModificationTime"]))
            container_node.append(lm_node)

            root_node.append(container_node)

        buf = BytesIO()
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

        if 'x-timestamp' in req.headers:
            try:
                req_timestamp = Timestamp(req.headers['X-Timestamp'])
            except ValueError:
                return swob.HTTPBadRequest(
                    request=req, content_type='text/plain',
                    body='X-Timestamp should be a UNIX timestamp float value; '
                         'was %r' % req.headers['x-timestamp'])
            req.headers['X-Timestamp'] = req_timestamp.internal
        else:
            req.headers['X-Timestamp'] = Timestamp(time.time()).internal

        if not req.headers.get('Content-Type'):
            req.headers['Content-Type'] = guess_content_type(
                req.path, is_dir=ctx.object_name.endswith('/'))
        err = constraints.check_object_creation(req, ctx.object_name)
        if err:
            return err

        if (req.headers['Content-Type'] == DIRECTORY_CONTENT_TYPE and
                req.headers.get('Content-Length') == '0'):
            return self.put_object_as_directory(ctx)
        else:
            return self.put_object_as_file(ctx)

    def put_object_as_directory(self, ctx):
        """
        Create or update an object as a directory.
        """
        req = ctx.req

        request_etag = req.headers.get("ETag", "")
        if should_validate_etag(request_etag) and \
                request_etag != EMPTY_OBJECT_ETAG:
            return swob.HTTPUnprocessableEntity(request=req)

        path = urllib_parse.unquote(req.path)
        obj_metadata = extract_object_metadata_from_headers(req.headers)

        # mung the passed etags, if any (NumWrites for a directory is
        # always zero)
        mung_etags(obj_metadata, request_etag, 0)

        rpc_req = rpc.middleware_mkdir_request(
            path, serialize_metadata(obj_metadata))
        rpc_resp = self.rpc_call(ctx, rpc_req)
        mtime_ns, inode, num_writes = rpc.parse_middleware_mkdir_response(
            rpc_resp)

        # currently best_possible_etag() returns EMPTY_OBJECT_ETAG for
        # all directories, but that might change in the future.
        # unmung the obj_metadata so best_possible_etag() can use it
        # if its behavior changes (note that num_writes is forced to 0).
        unmung_etags(obj_metadata, 0)
        resp_headers = {
            "Content-Type": DIRECTORY_CONTENT_TYPE,
            "Last-Modified": last_modified_from_epoch_ns(mtime_ns)}
        resp_headers["ETag"] = best_possible_etag(
            obj_metadata, ctx.account_name, inode, 0, is_dir=True)

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

        # Since this upload can be arbitrarily large, we split it across
        # multiple log segments.
        log_segments = []
        i = 0
        while True:
            # First, make sure there's more data to read from the client. No
            # sense allocating log segments and whatnot if we're not going
            # to use them.
            subinput = LimitedInput(wsgi_input, self.max_log_segment_size)
            if not subinput.has_more_to_read:
                break

            # Ask ProxyFS for the next log segment we can use
            phys_path = next(physical_path_gen)

            # Set up the subrequest with the bare minimum of useful headers.
            # This lets us avoid headers that will break the PUT immediately
            # (ETag), headers that may complicate GETs of this object
            # (X-Static-Large-Object, X-Object-Manifest), things that will
            # break the GET some time in the future (X-Delete-At,
            # X-Delete-After), and things that take up xattr space for no
            # real gain (user metadata).
            subreq = swob.Request.blank(phys_path)
            subreq.method = 'PUT'
            subreq.environ['wsgi.input'] = subinput
            subreq.headers["Transfer-Encoding"] = "chunked"

            # This ensures that (a) every subrequest has its own unique
            # txid, and (b) a log search for the txid in the response finds
            # all of the subrequests.
            trans_id = req.headers.get('X-Trans-Id')
            if trans_id:
                subreq.headers['X-Trans-Id'] = trans_id + ("-%03x" % i)

            # Actually put one chunk of the data into Swift
            subresp = subreq.get_response(self.app)
            if not 200 <= subresp.status_int < 299:
                # Something went wrong; may as well bail out now
                return subresp

            log_segments.append((phys_path, subinput.bytes_read))
            i += 1

        if should_validate_etag(request_etag) and \
                hasher.hexdigest() != request_etag:
            return swob.HTTPUnprocessableEntity(request=req)

        # All the data is now in Swift; we just have to tell proxyfsd
        # about it.  Mung any passed ETags values to include the
        # number of writes to the file (basically, the object's update
        # count) and supply the MD5 hash computed here which becomes
        # object's future ETag value until the object updated.
        obj_metadata = extract_object_metadata_from_headers(req.headers)
        mung_etags(obj_metadata, hasher.hexdigest(), len(log_segments))

        put_complete_req = rpc.put_complete_request(
            virtual_path, log_segments, serialize_metadata(obj_metadata))
        try:
            mtime_ns, inode, __writes = rpc.parse_put_complete_response(
                self.rpc_call(ctx, put_complete_req))
        except utils.RpcError as err:
            # We deliberately don't try to clean up our log segments on
            # failure. ProxyFS is responsible for cleaning up unreferenced
            # log segments.
            if err.errno == pfs_errno.NotEmptyError:
                return swob.HTTPConflict(
                    request=req,
                    headers={"Content-Type": "text/plain"},
                    body="This is a non-empty directory")
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
        err = constraints.check_metadata(req, 'object')
        if err:
            return err
        path = urllib_parse.unquote(req.path)
        new_metadata = extract_object_metadata_from_headers(req.headers)

        try:
            head_response = self.rpc_call(ctx, rpc.head_request(path))
            raw_old_metadata, mtime, _, _, inode_number, _ = \
                rpc.parse_head_response(head_response)
        except utils.RpcError as err:
            if err.errno in (pfs_errno.NotFoundError, pfs_errno.NotDirError):
                return swob.HTTPNotFound(request=req)
            else:
                raise

        # There is no need to call unmung_etags() before the merge and
        # mung_etags() after because the merge cannot change the several
        # possible ETag headers.
        #
        # This might be an opportunity to drop an ETAG header that has
        # become stale due to num_writes changing, but that does not
        # seem important to address.
        old_metadata = deserialize_metadata(raw_old_metadata)
        merged_metadata = merge_object_metadata(old_metadata, new_metadata)
        raw_merged_metadata = serialize_metadata(merged_metadata)

        self.rpc_call(ctx, rpc.post_request(
            path, raw_old_metadata, raw_merged_metadata))

        resp = swob.HTTPAccepted(request=req, body="")
        return resp

    def get_object(self, ctx):
        req = ctx.req
        byteranges = req.range.ranges if req.range else ()

        try:
            object_response = self.rpc_call(ctx, rpc.get_object_request(
                urllib_parse.unquote(req.path), byteranges))
        except utils.RpcError as err:
            if err.errno in (pfs_errno.NotFoundError, pfs_errno.NotDirError):
                return swob.HTTPNotFound(request=req)
            elif err.errno == pfs_errno.IsDirError:
                return swob.HTTPOk(
                    request=req, body="",
                    headers={"Content-Type": DIRECTORY_CONTENT_TYPE,
                             "ETag": EMPTY_OBJECT_ETAG})

            else:
                # punt to top-level exception handler
                raise

        (read_plan, raw_metadata, size, mtime_ns,
         is_dir, ino, num_writes, lease_id) = \
            rpc.parse_get_object_response(object_response)

        metadata = deserialize_metadata(raw_metadata)
        unmung_etags(metadata, num_writes)
        headers = swob.HeaderKeyDict(metadata)

        if "Content-Type" not in headers:
            headers["Content-Type"] = guess_content_type(req.path, is_dir)
        else:
            headers['Content-Type'] = headers['Content-Type'].split(
                ';swift_bytes=')[0]

        headers["Accept-Ranges"] = "bytes"
        headers["Last-Modified"] = last_modified_from_epoch_ns(
            mtime_ns)
        headers["X-Timestamp"] = x_timestamp_from_epoch_ns(
            mtime_ns)
        headers["Etag"] = best_possible_etag(
            headers, ctx.account_name, ino, num_writes, is_dir=is_dir)

        get_read_plan = req.params.get("get-read-plan", "no")
        if get_read_plan == "":
            get_read_plan = "yes"
        if self.bypass_mode != 'off' and req.environ.get('swift_owner') and \
                config_true_value(get_read_plan):
            headers.update({
                # Flag that pfs_middleware correctly interpretted this request
                "X-ProxyFS-Read-Plan": "True",
                # Stash the "real" content type...
                "X-Object-Content-Type": headers["Content-Type"],
                # ... so we can indicate that *this* data is coming out JSON
                "Content-Type": "application/json",
                # Also include the total object size
                # (since the read plan respects Range requests)
                "X-Object-Content-Length": size,
            })
            return swob.HTTPOk(request=req, body=json.dumps(read_plan),
                               headers=headers)

        if size > 0 and read_plan is None:
            headers["Content-Range"] = "bytes */%d" % size
            return swob.HTTPRequestedRangeNotSatisfiable(
                request=req, headers=headers)

        # NB: this is a size-0 queue, so it acts as a channel: a put()
        # blocks until another greenthread does a get(). This lets us use it
        # for (very limited) bidirectional communication.
        channel = eventlet.queue.Queue(0)
        eventlet.spawn_n(self._keep_lease_alive, ctx, channel, lease_id)

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

        resp = swob.HTTPOk(app_iter=seg_iter, conditional_response=True,
                           request=req,
                           headers=headers,
                           content_length=size)

        # Support conditional if-match/if-none-match requests for SLOs
        resp._conditional_etag = swift_code.resolve_etag_is_at_header(
            req, resp.headers)
        return resp

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
            if err.errno in (pfs_errno.NotFoundError, pfs_errno.NotDirError):
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
            if err.errno in (pfs_errno.NotFoundError, pfs_errno.NotDirError):
                return swob.HTTPNotFound(request=req)
            else:
                raise

        raw_md, last_modified_ns, file_size, is_dir, ino, num_writes = \
            rpc.parse_head_response(head_response)

        metadata = deserialize_metadata(raw_md)
        unmung_etags(metadata, num_writes)
        headers = swob.HeaderKeyDict(metadata)

        if "Content-Type" not in headers:
            headers["Content-Type"] = guess_content_type(req.path, is_dir)
        else:
            headers['Content-Type'] = headers['Content-Type'].split(
                ';swift_bytes=')[0]

        headers["Content-Length"] = file_size
        headers["ETag"] = best_possible_etag(
            headers, ctx.account_name, ino, num_writes, is_dir=is_dir)
        headers["Last-Modified"] = last_modified_from_epoch_ns(
            last_modified_ns)
        headers["X-Timestamp"] = x_timestamp_from_epoch_ns(
            last_modified_ns)

        resp = swob.HTTPOk(request=req, headers=headers,
                           conditional_response=True)

        # Support conditional if-match/if-none-match requests for SLOs
        resp._conditional_etag = swift_code.resolve_etag_is_at_header(
            req, resp.headers)
        return resp

    def coalesce_object(self, ctx, auth_cb):

        # extract and verify the object list for the new object
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
        authed_containers = set()
        ctx.req.environ.setdefault('swift.infocache', {})
        for elem in decoded_json["elements"]:
            if not isinstance(elem, six.string_types):
                return swob.HTTPBadRequest(request=req, body="Malformed JSON")

            normalized_elem = elem
            if normalized_elem.startswith('/'):
                normalized_elem = normalized_elem[1:]
            if any(p in ('', '.', '..') for p in normalized_elem.split('/')):
                return swob.HTTPBadRequest(request=req,
                                           body="Bad element path: %s" % elem)
            elem_container = normalized_elem.split('/', 1)[0]
            elem_container_path = '/v1/%s/%s' % (
                ctx.account_name, elem_container)
            if auth_cb and elem_container_path not in authed_containers:
                # Gotta check auth for all of the segments, too
                bimodal_checker = ctx.req.environ[utils.ENV_BIMODAL_CHECKER]
                acl_env = ctx.req.environ.copy()
                acl_env['PATH_INFO'] = swift_code.text_to_wsgi(
                    elem_container_path)
                container_info = get_container_info(
                    acl_env, bimodal_checker,
                    swift_source="PFS")
                for acl in ('read_acl', 'write_acl'):
                    req.acl = container_info[acl]
                    denial_response = auth_cb(ctx.req)
                    if denial_response:
                        return denial_response
                authed_containers.add(elem_container_path)

        # proxyfs treats the number of objects as the number of writes
        num_writes = len(decoded_json["elements"])

        # validate the metadata for the new object (further munging
        # of ETags will be done later)
        err = constraints.check_metadata(req, 'object')
        if err:
            return err

        # retrieve the ETag value in the request, or None
        req_etag = req.headers.get('ETag')

        # strip out user supplied and other unwanted headers
        obj_metadata = extract_object_metadata_from_headers(req.headers)

        # strip out headers that apply only to SLO objects
        unwanted_headers = ['X-Static-Large-Object']
        for header in obj_metadata.keys():
            if header.startswith("X-Object-Sysmeta-Slo-"):
                unwanted_headers.append(header)
        for header in unwanted_headers:
            if header in obj_metadata:
                del obj_metadata[header]

        # Now that we know the number of writes (really number of objects) we
        # can mung the sundry ETag headers.
        mung_etags(obj_metadata, req_etag, num_writes)

        raw_obj_metadata = serialize_metadata(obj_metadata)

        # now get proxyfs to coalesce the objects and set initial headers
        try:
            coalesce_response = self.rpc_call(
                ctx, rpc.coalesce_object_request(
                    object_path, decoded_json["elements"], raw_obj_metadata))

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

        last_modified_ns, inum, num_writes = \
            rpc.parse_coalesce_object_response(coalesce_response)

        unmung_etags(obj_metadata, num_writes)
        headers = {}
        headers["Etag"] = best_possible_etag(
            obj_metadata, ctx.account_name, inum, num_writes)
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
        rpc_method = rpc_request['method']
        start_time = time.time()
        try:
            return self._rpc_call([ctx.proxyfsd_addrinfo], rpc_request)
        finally:
            duration = time.time() - start_time
            self.logger.debug("RPC %s took %.6fs", rpc_method, duration)

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

            errstr = result.get("error")
            if errstr:
                errno = utils.extract_errno(errstr)
                raise utils.RpcError(errno, errstr)

            return result["result"]
