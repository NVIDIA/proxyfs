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

import unittest
import pfs_middleware.utils as utils


class TestHelperFunctions(unittest.TestCase):
    def test_extract_errno(self):
        self.assertEqual(2, utils.extract_errno("errno: 2"))
        self.assertEqual(17, utils.extract_errno("errno: 17"))
        self.assertEqual(None, utils.extract_errno("it broke"))

    def test_parse_path(self):
        self.assertEqual(
            utils.parse_path("/v1/a/c/o"),
            ["v1", "a", "c", "o"])
        self.assertEqual(
            utils.parse_path("/v1/a/c/obj/with/slashes"),
            ["v1", "a", "c", "obj/with/slashes"])
        self.assertEqual(
            utils.parse_path("/v1/a/c/obj/trailing/slashes///"),
            ["v1", "a", "c", "obj/trailing/slashes///"])
        self.assertEqual(
            utils.parse_path("/v1/a/c/"),
            ["v1", "a", "c", None])
        self.assertEqual(
            utils.parse_path("/v1/a/c"),
            ["v1", "a", "c", None])
        self.assertEqual(
            utils.parse_path("/v1/a/"),
            ["v1", "a", None, None])
        self.assertEqual(
            utils.parse_path("/v1/a"),
            ["v1", "a", None, None])
        self.assertEqual(
            utils.parse_path("/info"),
            ["info", None, None, None])
        self.assertEqual(
            utils.parse_path("/"),
            [None, None, None, None])
