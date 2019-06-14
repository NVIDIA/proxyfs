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

import json
import unittest

from swift.common import swob

from . import helpers
import pfs_middleware.s3_compat as s3_compat
import pfs_middleware.utils as utils


class TestS3Compat(unittest.TestCase):
    def setUp(self):
        super(TestS3Compat, self).setUp()
        self.app = helpers.FakeProxy()
        self.s3_compat = s3_compat.S3Compat(self.app, {})

        self.app.register(
            'PUT', '/v1/AUTH_test/con/obj',
            201, {}, '')

    def test_not_bimodal(self):
        req = swob.Request.blank(
            "/v1/AUTH_test/con/obj?multipart-manifest=put",
            environ={'REQUEST_METHOD': 'PUT',
                     'wsgi.input': swob.WsgiBytesIO(b"")},
            headers={'Content-Length': '0'})

        resp = req.get_response(self.s3_compat)
        self.assertEqual(resp.status_int, 201)

    def test_bimodal_but_not_swift3(self):
        req = swob.Request.blank(
            "/v1/AUTH_test/con/obj?multipart-manifest=put",
            environ={'REQUEST_METHOD': 'PUT',
                     'wsgi.input': swob.WsgiBytesIO(b""),
                     utils.ENV_IS_BIMODAL: True},
            headers={'Content-Length': '0'})

        resp = req.get_response(self.s3_compat)
        self.assertEqual(resp.status_int, 201)

    def test_conversion(self):
        self.app.register(
            'COALESCE', '/v1/AUTH_test/con/obj',
            201, {}, '')

        # Note that this is in SLO's internal format since this request has
        # already passed through SLO.
        slo_manifest = [{
            "name": "/con-segments/obj/1506721327.316611/1",
            "hash": "dontcare1",
            "bytes": 12345678901,
        }, {
            "name": "/con-segments/obj/1506721327.316611/2",
            "hash": "dontcare2",
            "bytes": 12345678902,
        }, {
            "name": "/con-segments/obj/1506721327.316611/3",
            "hash": "dontcare3",
            "bytes": 12345678903,
        }]
        serialized_slo_manifest = json.dumps(slo_manifest).encode('utf-8')

        req = swob.Request.blank(
            "/v1/AUTH_test/con/obj?multipart-manifest=put",
            environ={'REQUEST_METHOD': 'PUT',
                     'wsgi.input': swob.WsgiBytesIO(serialized_slo_manifest),
                     utils.ENV_IS_BIMODAL: True,
                     'swift.source': 'S3'},
            headers={'Content-Length': str(len(slo_manifest))})

        resp = req.get_response(self.s3_compat)
        self.assertEqual(resp.status_int, 201)
        self.assertEqual(self.app.calls[0][0], 'COALESCE')

        self.assertEqual(s3_compat.convert_slo_to_coalesce(slo_manifest), {
            "elements": [
                "con-segments/obj/1506721327.316611/1",
                "con-segments/obj/1506721327.316611/2",
                "con-segments/obj/1506721327.316611/3"]})
