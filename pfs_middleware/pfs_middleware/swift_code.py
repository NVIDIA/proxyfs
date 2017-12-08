# Copyright (c) 2010-2013 OpenStack Foundation
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
Code copied from OpenStack Swift.

We import some things from Swift if we think they're in stable-enough
locations to depend on. When it turns out we're wrong, the things get copied
into here.

This is all Apache-licensed code, so copying it into here is fine.

The copyright header in this file was taken from
swift/common/request_helpers.py in the OpenStack Swift source distribution.
"""

import email.parser
import hashlib
import itertools
import re
import six
import sys
import time
from six import BytesIO
from six.moves.urllib.parse import unquote
from swift.common.swob import HTTPBadRequest, HTTPNotAcceptable, \
    HTTPNotImplemented, HTTPLengthRequired, Request, Range, \
    multi_range_iterator, HeaderKeyDict


# Taken from swift/common/constraints.py, commit d2e32b3
#: Query string format= values to their corresponding content-type values
FORMAT2CONTENT_TYPE = {'plain': 'text/plain', 'json': 'application/json',
                       'xml': 'application/xml'}


# Taken from swift/common/request_helpers.py, commit d2e32b3
def get_param(req, name, default=None):
    """
    Get parameters from an HTTP request ensuring proper handling UTF-8
    encoding.

    :param req: request object
    :param name: parameter name
    :param default: result to return if the parameter is not found
    :returns: HTTP request parameter value
              (as UTF-8 encoded str, not unicode object)
    :raises HTTPBadRequest: if param not valid UTF-8 byte sequence
    """
    value = req.params.get(name, default)
    if value and not isinstance(value, six.text_type):
        try:
            value.decode('utf8')    # Ensure UTF8ness
        except UnicodeDecodeError:
            raise HTTPBadRequest(
                request=req, content_type='text/plain',
                body='"%s" parameter not valid UTF-8' % name)
    return value


# Taken from swift/common/request_helpers.py, commit d2e32b3
def get_listing_content_type(req):
    """
    Determine the content type to use for an account or container listing
    response.

    :param req: request object
    :returns: content type as a string (e.g. text/plain, application/json)
    :raises HTTPNotAcceptable: if the requested content type is not acceptable
    :raises HTTPBadRequest: if the 'format' query param is provided and
             not valid UTF-8
    """
    query_format = get_param(req, 'format')
    if query_format:
        req.accept = FORMAT2CONTENT_TYPE.get(
            query_format.lower(), FORMAT2CONTENT_TYPE['plain'])
    out_content_type = req.accept.best_match(
        ['text/plain', 'application/json', 'application/xml', 'text/xml'])
    if not out_content_type:
        raise HTTPNotAcceptable(request=req)
    return out_content_type


# Taken from swift/common/utils.py, commit d2e32b3
# Used when reading config values
TRUE_VALUES = set(('true', '1', 'yes', 'on', 't', 'y'))


def config_true_value(value):
    """
    Returns True if the value is either True or a string in TRUE_VALUES.
    Returns False otherwise.
    """
    return value is True or \
        (isinstance(value, six.string_types) and value.lower() in TRUE_VALUES)


# Taken from swift/common/constraints.py, commit 1962b18
#
# Modified to not check maximum object size; ProxyFS doesn't enforce one.
def check_object_creation(req):
    """
    Check to ensure that everything is alright about an object to be created.
    :param req: HTTP request object
    :returns: HTTPLengthRequired -- missing content-length header and not
                                    a chunked request
    :returns: HTTPBadRequest -- missing or bad content-type header, or
                                bad metadata
    :returns: HTTPNotImplemented -- unsupported transfer-encoding header value
    """
    try:
        req.message_length()
    except ValueError as e:
        return HTTPBadRequest(request=req, content_type='text/plain',
                              body=str(e))
    except AttributeError as e:
        return HTTPNotImplemented(request=req, content_type='text/plain',
                                  body=str(e))
    if req.content_length is None and \
            req.headers.get('transfer-encoding') != 'chunked':
        return HTTPLengthRequired(body='Missing Content-Length header.',
                                  request=req,
                                  content_type='text/plain')

    if 'Content-Type' not in req.headers:
        return HTTPBadRequest(request=req, content_type='text/plain',
                              body='No content type')
    return None


# Made up for SegmentedIterable
class ListingIterError(Exception):
    pass


# Made up for SegmentedIterable
class SegmentError(Exception):
    pass


# Made up for iter_multipart_mime_documents
class ChunkReadError(Exception):
    pass


# Made up for iter_multipart_mime_documents
class MimeInvalid(Exception):
    pass


# Taken from swift/common/utils.py, commit 1962b18
def close_if_possible(maybe_closable):
    close_method = getattr(maybe_closable, 'close', None)
    if callable(close_method):
        return close_method()


# Taken from swift/common/utils.py, commit 1962b18
#
# Modified to use different exception classes.
class _MultipartMimeFileLikeObject(object):

    def __init__(self, wsgi_input, boundary, input_buffer, read_chunk_size):
        self.no_more_data_for_this_file = False
        self.no_more_files = False
        self.wsgi_input = wsgi_input
        self.boundary = boundary
        self.input_buffer = input_buffer
        self.read_chunk_size = read_chunk_size

    def read(self, length=None):
        if not length:
            length = self.read_chunk_size
        if self.no_more_data_for_this_file:
            return b''

        # read enough data to know whether we're going to run
        # into a boundary in next [length] bytes
        if len(self.input_buffer) < length + len(self.boundary) + 2:
            to_read = length + len(self.boundary) + 2
            while to_read > 0:
                try:
                    chunk = self.wsgi_input.read(to_read)
                except (IOError, ValueError) as e:
                    raise ChunkReadError(str(e))
                to_read -= len(chunk)
                self.input_buffer += chunk
                if not chunk:
                    self.no_more_files = True
                    break

        boundary_pos = self.input_buffer.find(self.boundary)

        # boundary does not exist in the next (length) bytes
        if boundary_pos == -1 or boundary_pos > length:
            ret = self.input_buffer[:length]
            self.input_buffer = self.input_buffer[length:]
        # if it does, just return data up to the boundary
        else:
            ret, self.input_buffer = self.input_buffer.split(self.boundary, 1)
            self.no_more_files = self.input_buffer.startswith(b'--')
            self.no_more_data_for_this_file = True
            self.input_buffer = self.input_buffer[2:]
        return ret

    def readline(self):
        if self.no_more_data_for_this_file:
            return b''
        boundary_pos = newline_pos = -1
        while newline_pos < 0 and boundary_pos < 0:
            try:
                chunk = self.wsgi_input.read(self.read_chunk_size)
            except (IOError, ValueError) as e:
                raise ChunkReadError(str(e))
            self.input_buffer += chunk
            newline_pos = self.input_buffer.find(b'\r\n')
            boundary_pos = self.input_buffer.find(self.boundary)
            if not chunk:
                self.no_more_files = True
                break
        # found a newline
        if newline_pos >= 0 and \
                (boundary_pos < 0 or newline_pos < boundary_pos):
            # Use self.read to ensure any logic there happens...
            ret = b''
            to_read = newline_pos + 2
            while to_read > 0:
                chunk = self.read(to_read)
                # Should never happen since we're reading from input_buffer,
                # but just for completeness...
                if not chunk:
                    break
                to_read -= len(chunk)
                ret += chunk
            return ret
        else:  # no newlines, just return up to next boundary
            return self.read(len(self.input_buffer))


# Taken from swift/common/utils.py, commit 1962b18
class Spliterator(object):
    """
    Takes an iterator yielding sliceable things (e.g. strings or lists) and
    yields subiterators, each yielding up to the requested number of items
    from the source.

    >>> si = Spliterator(["abcde", "fg", "hijkl"])
    >>> ''.join(si.take(4))
    "abcd"
    >>> ''.join(si.take(3))
    "efg"
    >>> ''.join(si.take(1))
    "h"
    >>> ''.join(si.take(3))
    "ijk"
    >>> ''.join(si.take(3))
    "l"  # shorter than requested; this can happen with the last iterator

    """
    def __init__(self, source_iterable):
        self.input_iterator = iter(source_iterable)
        self.leftovers = None
        self.leftovers_index = 0
        self._iterator_in_progress = False

    def take(self, n):
        if self._iterator_in_progress:
            raise ValueError(
                "cannot call take() again until the first iterator is"
                " exhausted (has raised StopIteration)")
        self._iterator_in_progress = True

        try:
            if self.leftovers:
                # All this string slicing is a little awkward, but it's for
                # a good reason. Consider a length N string that someone is
                # taking k bytes at a time.
                #
                # With this implementation, we create one new string of
                # length k (copying the bytes) on each call to take(). Once
                # the whole input has been consumed, each byte has been
                # copied exactly once, giving O(N) bytes copied.
                #
                # If, instead of this, we were to set leftovers =
                # leftovers[k:] and omit leftovers_index, then each call to
                # take() would copy k bytes to create the desired substring,
                # then copy all the remaining bytes to reset leftovers,
                # resulting in an overall O(N^2) bytes copied.
                llen = len(self.leftovers) - self.leftovers_index
                if llen <= n:
                    n -= llen
                    to_yield = self.leftovers[self.leftovers_index:]
                    self.leftovers = None
                    self.leftovers_index = 0
                    yield to_yield
                else:
                    to_yield = self.leftovers[
                        self.leftovers_index:(self.leftovers_index + n)]
                    self.leftovers_index += n
                    n = 0
                    yield to_yield

            while n > 0:
                chunk = next(self.input_iterator)
                cl = len(chunk)
                if cl <= n:
                    n -= cl
                    yield chunk
                else:
                    self.leftovers = chunk
                    self.leftovers_index = n
                    yield chunk[:n]
                    n = 0
        finally:
            self._iterator_in_progress = False


# Taken from swift/common/request_helpers.py, commit 1962b18
def make_env(env, method=None, path=None, agent='Swift', query_string=None,
             swift_source=None):
    """
    Returns a new fresh WSGI environment.

    :param env: The WSGI environment to base the new environment on.
    :param method: The new REQUEST_METHOD or None to use the
                   original.
    :param path: The new path_info or none to use the original. path
                 should NOT be quoted. When building a url, a Webob
                 Request (in accordance with wsgi spec) will quote
                 env['PATH_INFO'].  url += quote(environ['PATH_INFO'])
    :param query_string: The new query_string or none to use the original.
                         When building a url, a Webob Request will append
                         the query string directly to the url.
                         url += '?' + env['QUERY_STRING']
    :param agent: The HTTP user agent to use; default 'Swift'. You
                  can put %(orig)s in the agent to have it replaced
                  with the original env's HTTP_USER_AGENT, such as
                  '%(orig)s StaticWeb'. You also set agent to None to
                  use the original env's HTTP_USER_AGENT or '' to
                  have no HTTP_USER_AGENT.
    :param swift_source: Used to mark the request as originating out of
                         middleware. Will be logged in proxy logs.
    :returns: Fresh WSGI environment.
    """
    newenv = {}
    for name in ('HTTP_USER_AGENT', 'HTTP_HOST', 'PATH_INFO',
                 'QUERY_STRING', 'REMOTE_USER', 'REQUEST_METHOD',
                 'SCRIPT_NAME', 'SERVER_NAME', 'SERVER_PORT',
                 'HTTP_ORIGIN', 'HTTP_ACCESS_CONTROL_REQUEST_METHOD',
                 'SERVER_PROTOCOL', 'swift.cache', 'swift.source',
                 'swift.trans_id', 'swift.authorize_override',
                 'swift.authorize', 'HTTP_X_USER_ID', 'HTTP_X_PROJECT_ID',
                 'HTTP_REFERER', 'swift.infocache'):
        if name in env:
            newenv[name] = env[name]
    if method:
        newenv['REQUEST_METHOD'] = method
    if path:
        newenv['PATH_INFO'] = path
        newenv['SCRIPT_NAME'] = ''
    if query_string is not None:
        newenv['QUERY_STRING'] = query_string
    if agent:
        newenv['HTTP_USER_AGENT'] = (
            agent % {'orig': env.get('HTTP_USER_AGENT', '')}).strip()
    elif agent == '' and 'HTTP_USER_AGENT' in newenv:
        del newenv['HTTP_USER_AGENT']
    if swift_source:
        newenv['swift.source'] = swift_source
    newenv['wsgi.input'] = BytesIO()
    if 'SCRIPT_NAME' not in newenv:
        newenv['SCRIPT_NAME'] = ''
    return newenv


# Taken from swift/common/request_helpers.py, commit 1962b18
def make_subrequest(env, method=None, path=None, body=None, headers=None,
                    agent='Swift', swift_source=None, make_env=make_env):
    """
    Makes a new swob.Request based on the current env but with the
    parameters specified.

    :param env: The WSGI environment to base the new request on.
    :param method: HTTP method of new request; default is from
                   the original env.
    :param path: HTTP path of new request; default is from the
                 original env. path should be compatible with what you
                 would send to Request.blank. path should be quoted and it
                 can include a query string. for example:
                 '/a%20space?unicode_str%E8%AA%9E=y%20es'
    :param body: HTTP body of new request; empty by default.
    :param headers: Extra HTTP headers of new request; None by
                    default.
    :param agent: The HTTP user agent to use; default 'Swift'. You
                  can put %(orig)s in the agent to have it replaced
                  with the original env's HTTP_USER_AGENT, such as
                  '%(orig)s StaticWeb'. You also set agent to None to
                  use the original env's HTTP_USER_AGENT or '' to
                  have no HTTP_USER_AGENT.
    :param swift_source: Used to mark the request as originating out of
                         middleware. Will be logged in proxy logs.
    :param make_env: make_subrequest calls this make_env to help build the
        swob.Request.
    :returns: Fresh swob.Request object.
    """
    query_string = None
    path = path or ''
    if path and '?' in path:
        path, query_string = path.split('?', 1)
    newenv = make_env(env, method, path=unquote(path), agent=agent,
                      query_string=query_string, swift_source=swift_source)
    if not headers:
        headers = {}
    if body:
        return Request.blank(path, environ=newenv, body=body, headers=headers)
    else:
        return Request.blank(path, environ=newenv, headers=headers)


# Taken from swift/common/http.py, commit 1962b18
def is_success(status):
    """
    Check if HTTP status code is successful.

    :param status: http status code
    :returns: True if status is successful, else False
    """
    return 200 <= status <= 299


# Taken from swift/common/utils.py, commit 1962b18
_rfc_token = r'[^()<>@,;:\"/\[\]?={}\x00-\x20\x7f]+'
_rfc_extension_pattern = re.compile(
    r'(?:\s*;\s*(' + _rfc_token + r")\s*(?:=\s*(" + _rfc_token +
    r'|"(?:[^"\\]|\\.)*"))?)')


# Taken from swift/common/utils.py, commit 1962b18
def parse_content_type(content_type):
    """
    Parse a content-type and its parameters into values.
    RFC 2616 sec 14.17 and 3.7 are pertinent.

    **Examples**::

        'text/plain; charset=UTF-8' -> ('text/plain', [('charset, 'UTF-8')])
        'text/plain; charset=UTF-8; level=1' ->
            ('text/plain', [('charset, 'UTF-8'), ('level', '1')])

    :param content_type: content_type to parse
    :returns: a tuple containing (content type, list of k, v parameter tuples)
    """
    parm_list = []
    if ';' in content_type:
        content_type, parms = content_type.split(';', 1)
        parms = ';' + parms
        for m in _rfc_extension_pattern.findall(parms):
            key = m[0].strip()
            value = m[1].strip()
            parm_list.append((key, value))
    return content_type, parm_list


# Taken from swift/common/utils.py, commit 1962b18
def parse_mime_headers(doc_file):
    """
    Takes a file-like object containing a MIME document and returns a
    HeaderKeyDict containing the headers. The body of the message is not
    consumed: the position in doc_file is left at the beginning of the body.

    This function was inspired by the Python standard library's
    http.client.parse_headers.

    :param doc_file: binary file-like object containing a MIME document
    :returns: a swift.common.swob.HeaderKeyDict containing the headers
    """
    headers = []
    while True:
        line = doc_file.readline()
        done = line in (b'\r\n', b'\n', b'')
        if six.PY3:
            try:
                line = line.decode('utf-8')
            except UnicodeDecodeError:
                line = line.decode('latin1')
        headers.append(line)
        if done:
            break
    if six.PY3:
        header_string = ''.join(headers)
    else:
        header_string = b''.join(headers)
    headers = email.parser.Parser().parsestr(header_string)
    return HeaderKeyDict(headers)


# Taken from swift/common/utils.py, commit 1962b18
def maybe_multipart_byteranges_to_document_iters(app_iter, content_type):
    """
    Takes an iterator that may or may not contain a multipart MIME document
    as well as content type and returns an iterator of body iterators.

    :param app_iter: iterator that may contain a multipart MIME document
    :param content_type: content type of the app_iter, used to determine
                         whether it conains a multipart document and, if
                         so, what the boundary is between documents
    """
    content_type, params_list = parse_content_type(content_type)
    if content_type != 'multipart/byteranges':
        yield app_iter
        return

    body_file = FileLikeIter(app_iter)
    boundary = dict(params_list)['boundary']
    for _headers, body in mime_to_document_iters(body_file, boundary):
        yield (chunk for chunk in iter(lambda: body.read(65536), ''))


# Taken from swift/common/utils.py, commit 1962b18
def mime_to_document_iters(input_file, boundary, read_chunk_size=4096):
    """
    Takes a file-like object containing a multipart MIME document and
    returns an iterator of (headers, body-file) tuples.

    :param input_file: file-like object with the MIME doc in it
    :param boundary: MIME boundary, sans dashes
        (e.g. "divider", not "--divider")
    :param read_chunk_size: size of strings read via input_file.read()
    """
    doc_files = iter_multipart_mime_documents(input_file, boundary,
                                              read_chunk_size)
    for i, doc_file in enumerate(doc_files):
        # this consumes the headers and leaves just the body in doc_file
        headers = parse_mime_headers(doc_file)
        yield (headers, doc_file)


# Taken from swift/common/utils.py, commit 1962b18
#
# Modified to use different exception classes.
def iter_multipart_mime_documents(wsgi_input, boundary, read_chunk_size=4096):
    """
    Given a multi-part-mime-encoded input file object and boundary,
    yield file-like objects for each part. Note that this does not
    split each part into headers and body; the caller is responsible
    for doing that if necessary.

    :param wsgi_input: The file-like object to read from.
    :param boundary: The mime boundary to separate new file-like
                     objects on.
    :returns: A generator of file-like objects for each part.
    :raises MimeInvalid: if the document is malformed
    """
    boundary = '--' + boundary
    blen = len(boundary) + 2  # \r\n
    try:
        got = wsgi_input.readline(blen)
        while got == '\r\n':
            got = wsgi_input.readline(blen)
    except (IOError, ValueError) as e:
        raise ChunkReadError(str(e))

    if got.strip() != boundary:
        raise MimeInvalid(
            'invalid starting boundary: wanted %r, got %r', (boundary, got))
    boundary = '\r\n' + boundary
    input_buffer = ''
    done = False
    while not done:
        it = _MultipartMimeFileLikeObject(wsgi_input, boundary, input_buffer,
                                          read_chunk_size)
        yield it
        done = it.no_more_files
        input_buffer = it.input_buffer


# Taken from swift/common/utils.py, commit 1962b18
class FileLikeIter(object):

    def __init__(self, iterable):
        """
        Wraps an iterable to behave as a file-like object.

        The iterable must yield bytes strings.
        """
        self.iterator = iter(iterable)
        self.buf = None
        self.closed = False

    def __iter__(self):
        return self

    def next(self):
        """
        next(x) -> the next value, or raise StopIteration
        """
        if self.closed:
            raise ValueError('I/O operation on closed file')
        if self.buf:
            rv = self.buf
            self.buf = None
            return rv
        else:
            return next(self.iterator)
    __next__ = next

    def read(self, size=-1):
        """
        read([size]) -> read at most size bytes, returned as a bytes string.

        If the size argument is negative or omitted, read until EOF is reached.
        Notice that when in non-blocking mode, less data than what was
        requested may be returned, even if no size parameter was given.
        """
        if self.closed:
            raise ValueError('I/O operation on closed file')
        if size < 0:
            return b''.join(self)
        elif not size:
            chunk = b''
        elif self.buf:
            chunk = self.buf
            self.buf = None
        else:
            try:
                chunk = next(self.iterator)
            except StopIteration:
                return b''
        if len(chunk) > size:
            self.buf = chunk[size:]
            chunk = chunk[:size]
        return chunk

    def readline(self, size=-1):
        """
        readline([size]) -> next line from the file, as a bytes string.

        Retain newline.  A non-negative size argument limits the maximum
        number of bytes to return (an incomplete line may be returned then).
        Return an empty string at EOF.
        """
        if self.closed:
            raise ValueError('I/O operation on closed file')
        data = b''
        while b'\n' not in data and (size < 0 or len(data) < size):
            if size < 0:
                chunk = self.read(1024)
            else:
                chunk = self.read(size - len(data))
            if not chunk:
                break
            data += chunk
        if b'\n' in data:
            data, sep, rest = data.partition(b'\n')
            data += sep
            if self.buf:
                self.buf = rest + self.buf
            else:
                self.buf = rest
        return data

    def readlines(self, sizehint=-1):
        """
        readlines([size]) -> list of bytes strings, each a line from the file.

        Call readline() repeatedly and return a list of the lines so read.
        The optional size argument, if given, is an approximate bound on the
        total number of bytes in the lines returned.
        """
        if self.closed:
            raise ValueError('I/O operation on closed file')
        lines = []
        while True:
            line = self.readline(sizehint)
            if not line:
                break
            lines.append(line)
            if sizehint >= 0:
                sizehint -= len(line)
                if sizehint <= 0:
                    break
        return lines

    def close(self):
        """
        close() -> None or (perhaps) an integer.  Close the file.

        Sets data attribute .closed to True.  A closed file cannot be used for
        further I/O operations.  close() may be called more than once without
        error.  Some kinds of file objects (for example, opened by popen())
        may return an exit status upon closing.
        """
        self.iterator = None
        self.closed = True


# Taken from swift/common/request_helpers.py, commit 1962b18
#
# Modified to use different exception classes.
class SegmentedIterable(object):
    """
    Iterable that returns the object contents for a large object.

    :param req: original request object
    :param app: WSGI application from which segments will come

    :param listing_iter: iterable yielding the object segments to fetch,
        along with the byte subranges to fetch, in the form of a 5-tuple
        (object-path, object-etag, object-size, first-byte, last-byte).

        If object-etag is None, no MD5 verification will be done.

        If object-size is None, no length verification will be done.

        If first-byte and last-byte are None, then the entire object will be
        fetched.

    :param max_get_time: maximum permitted duration of a GET request (seconds)
    :param logger: logger object
    :param swift_source: value of swift.source in subrequest environ
                         (just for logging)
    :param ua_suffix: string to append to user-agent.
    :param name: name of manifest (used in logging only)
    :param response_body_length: optional response body length for
                                 the response being sent to the client.
    """

    def __init__(self, req, app, listing_iter, max_get_time,
                 logger, ua_suffix, swift_source,
                 name='<not specified>', response_body_length=None):
        self.req = req
        self.app = app
        self.listing_iter = listing_iter
        self.max_get_time = max_get_time
        self.logger = logger
        self.ua_suffix = " " + ua_suffix
        self.swift_source = swift_source
        self.name = name
        self.response_body_length = response_body_length
        self.peeked_chunk = None
        self.app_iter = self._internal_iter()
        self.validated_first_segment = False
        self.current_resp = None

    def _coalesce_requests(self):
        start_time = time.time()
        pending_req = None
        pending_etag = None
        pending_size = None
        try:
            for seg_path, seg_etag, seg_size, first_byte, last_byte \
                    in self.listing_iter:
                first_byte = first_byte or 0
                go_to_end = last_byte is None or (
                    seg_size is not None and last_byte == seg_size - 1)
                if time.time() - start_time > self.max_get_time:
                    raise SegmentError(
                        'While processing manifest %s, '
                        'max LO GET time of %ds exceeded' %
                        (self.name, self.max_get_time))
                # The "multipart-manifest=get" query param ensures that the
                # segment is a plain old object, not some flavor of large
                # object; therefore, its etag is its MD5sum and hence we can
                # check it.
                path = seg_path + '?multipart-manifest=get'
                seg_req = make_subrequest(
                    self.req.environ, path=path, method='GET',
                    headers={'x-auth-token': self.req.headers.get(
                        'x-auth-token')},
                    agent=('%(orig)s ' + self.ua_suffix),
                    swift_source=self.swift_source)

                seg_req_rangeval = None
                if first_byte != 0 or not go_to_end:
                    seg_req_rangeval = "%s-%s" % (
                        first_byte, '' if go_to_end else last_byte)
                    seg_req.headers['Range'] = "bytes=" + seg_req_rangeval

                # We can only coalesce if paths match and we know the segment
                # size (so we can check that the ranges will be allowed)
                if pending_req and pending_req.path == seg_req.path and \
                        seg_size is not None:

                    # Make a new Range object so that we don't goof up the
                    # existing one in case of invalid ranges. Note that a
                    # range set with too many individual byteranges is
                    # invalid, so we can combine N valid byteranges and 1
                    # valid byterange and get an invalid range set.
                    if pending_req.range:
                        new_range_str = str(pending_req.range)
                    else:
                        new_range_str = "bytes=0-%d" % (seg_size - 1)

                    if seg_req.range:
                        new_range_str += "," + seg_req_rangeval
                    else:
                        new_range_str += ",0-%d" % (seg_size - 1)

                    if Range(new_range_str).ranges_for_length(seg_size):
                        # Good news! We can coalesce the requests
                        pending_req.headers['Range'] = new_range_str
                        continue
                    # else, Too many ranges, or too much backtracking, or ...

                if pending_req:
                    yield pending_req, pending_etag, pending_size
                pending_req = seg_req
                pending_etag = seg_etag
                pending_size = seg_size

        except ListingIterError:
            e_type, e_value, e_traceback = sys.exc_info()
            if time.time() - start_time > self.max_get_time:
                raise SegmentError(
                    'While processing manifest %s, '
                    'max LO GET time of %ds exceeded' %
                    (self.name, self.max_get_time))
            if pending_req:
                yield pending_req, pending_etag, pending_size
            six.reraise(e_type, e_value, e_traceback)

        if time.time() - start_time > self.max_get_time:
            raise SegmentError(
                'While processing manifest %s, '
                'max LO GET time of %ds exceeded' %
                (self.name, self.max_get_time))
        if pending_req:
            yield pending_req, pending_etag, pending_size

    def _internal_iter(self):
        bytes_left = self.response_body_length

        try:
            for seg_req, seg_etag, seg_size in self._coalesce_requests():
                seg_resp = seg_req.get_response(self.app)
                if not is_success(seg_resp.status_int):
                    close_if_possible(seg_resp.app_iter)
                    raise SegmentError(
                        'While processing manifest %s, '
                        'got %d while retrieving %s' %
                        (self.name, seg_resp.status_int, seg_req.path))

                elif ((seg_etag and (seg_resp.etag != seg_etag)) or
                        (seg_size and (seg_resp.content_length != seg_size) and
                         not seg_req.range)):
                    # The content-length check is for security reasons. Seems
                    # possible that an attacker could upload a >1mb object and
                    # then replace it with a much smaller object with same
                    # etag. Then create a big nested SLO that calls that
                    # object many times which would hammer our obj servers. If
                    # this is a range request, don't check content-length
                    # because it won't match.
                    close_if_possible(seg_resp.app_iter)
                    raise SegmentError(
                        'Object segment no longer valid: '
                        '%(path)s etag: %(r_etag)s != %(s_etag)s or '
                        '%(r_size)s != %(s_size)s.' %
                        {'path': seg_req.path, 'r_etag': seg_resp.etag,
                         'r_size': seg_resp.content_length,
                         's_etag': seg_etag,
                         's_size': seg_size})
                else:
                    self.current_resp = seg_resp

                seg_hash = None
                if seg_resp.etag and not seg_req.headers.get('Range'):
                    # Only calculate the MD5 if it we can use it to validate
                    seg_hash = hashlib.md5()

                document_iters = maybe_multipart_byteranges_to_document_iters(
                    seg_resp.app_iter,
                    seg_resp.headers['Content-Type'])

                for chunk in itertools.chain.from_iterable(document_iters):
                    if seg_hash:
                        seg_hash.update(chunk)

                    if bytes_left is None:
                        yield chunk
                    elif bytes_left >= len(chunk):
                        yield chunk
                        bytes_left -= len(chunk)
                    else:
                        yield chunk[:bytes_left]
                        bytes_left -= len(chunk)
                        close_if_possible(seg_resp.app_iter)
                        raise SegmentError(
                            'Too many bytes for %(name)s; truncating in '
                            '%(seg)s with %(left)d bytes left' %
                            {'name': self.name, 'seg': seg_req.path,
                             'left': bytes_left})
                close_if_possible(seg_resp.app_iter)

                if seg_hash and seg_hash.hexdigest() != seg_resp.etag:
                    raise SegmentError(
                        "Bad MD5 checksum in %(name)s for %(seg)s: headers had"
                        " %(etag)s, but object MD5 was actually %(actual)s" %
                        {'seg': seg_req.path, 'etag': seg_resp.etag,
                         'name': self.name, 'actual': seg_hash.hexdigest()})

            if bytes_left:
                raise SegmentError(
                    'Not enough bytes for %s; closing connection' % self.name)
        except (ListingIterError, SegmentError) as err:
            self.logger.error(err)
            if not self.validated_first_segment:
                raise
        finally:
            if self.current_resp:
                close_if_possible(self.current_resp.app_iter)

    def app_iter_range(self, *a, **kw):
        """
        swob.Response will only respond with a 206 status in certain cases; one
        of those is if the body iterator responds to .app_iter_range().

        However, this object (or really, its listing iter) is smart enough to
        handle the range stuff internally, so we just no-op this out for swob.
        """
        return self

    def app_iter_ranges(self, ranges, content_type, boundary, content_size):
        """
        This method assumes that iter(self) yields all the data bytes that
        go into the response, but none of the MIME stuff. For example, if
        the response will contain three MIME docs with data "abcd", "efgh",
        and "ijkl", then iter(self) will give out the bytes "abcdefghijkl".

        This method inserts the MIME stuff around the data bytes.
        """
        si = Spliterator(self)
        mri = multi_range_iterator(
            ranges, content_type, boundary, content_size,
            lambda start, end_plus_one: si.take(end_plus_one - start))
        try:
            for x in mri:
                yield x
        finally:
            self.close()

    def validate_first_segment(self):
        """
        Start fetching object data to ensure that the first segment (if any) is
        valid. This is to catch cases like "first segment is missing" or
        "first segment's etag doesn't match manifest".

        Note: this does not validate that you have any segments. A
        zero-segment large object is not erroneous; it is just empty.
        """
        if self.validated_first_segment:
            return

        try:
            self.peeked_chunk = next(self.app_iter)
        except StopIteration:
            pass
        finally:
            self.validated_first_segment = True

    def __iter__(self):
        if self.peeked_chunk is not None:
            pc = self.peeked_chunk
            self.peeked_chunk = None
            return itertools.chain([pc], self.app_iter)
        else:
            return self.app_iter

    def close(self):
        """
        Called when the client disconnect. Ensure that the connection to the
        backend server is closed.
        """
        close_if_possible(self.app_iter)


# Taken from swift/common/utils.py, commit e001c02
def list_from_csv(comma_separated_str):
    """
    Splits the str given and returns a properly stripped list of the comma
    separated values.
    """
    if comma_separated_str:
        return [v.strip() for v in comma_separated_str.split(',') if v.strip()]
    return []


# Taken from swift/common/request_helpers.py, commit e001c02
def resolve_etag_is_at_header(req, metadata):
    """
    Helper function to resolve an alternative etag value that may be stored in
    metadata under an alternate name.

    The value of the request's X-Backend-Etag-Is-At header (if it exists) is a
    comma separated list of alternate names in the metadata at which an
    alternate etag value may be found. This list is processed in order until an
    alternate etag is found.

    The left most value in X-Backend-Etag-Is-At will have been set by the left
    most middleware, or if no middleware, by ECObjectController, if an EC
    policy is in use. The left most middleware is assumed to be the authority
    on what the etag value of the object content is.

    The resolver will work from left to right in the list until it finds a
    value that is a name in the given metadata. So the left most wins, IF it
    exists in the metadata.

    By way of example, assume the encrypter middleware is installed. If an
    object is *not* encrypted then the resolver will not find the encrypter
    middleware's alternate etag sysmeta (X-Object-Sysmeta-Crypto-Etag) but will
    then find the EC alternate etag (if EC policy). But if the object *is*
    encrypted then X-Object-Sysmeta-Crypto-Etag is found and used, which is
    correct because it should be preferred over X-Object-Sysmeta-Crypto-Etag.

    :param req: a swob Request
    :param metadata: a dict containing object metadata
    :return: an alternate etag value if any is found, otherwise None
    """
    alternate_etag = None
    metadata = HeaderKeyDict(metadata)
    if "X-Backend-Etag-Is-At" in req.headers:
        names = list_from_csv(req.headers["X-Backend-Etag-Is-At"])
        for name in names:
            if name in metadata:
                alternate_etag = metadata[name]
                break
    return alternate_etag


# Taken from swift/proxy/controllers/container.py, commit e001c02
#
# Modified to no longer dangle off a controller
def clean_acls(req):
    if 'swift.clean_acl' in req.environ:
        for header in ('x-container-read', 'x-container-write'):
            if header in req.headers:
                try:
                    req.headers[header] = \
                        req.environ['swift.clean_acl'](header,
                                                       req.headers[header])
                except ValueError as err:
                    return HTTPBadRequest(request=req, body=str(err))
    return None
