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

import six
from swift.common.swob import HTTPBadRequest, HTTPNotAcceptable, \
    HTTPNotImplemented, HTTPLengthRequired


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
