# Copyright (c) 2016 SwiftStack, Inc.
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

import pfs_middleware.rpc as rpc


class TestAddressParsing(unittest.TestCase):
    def test_ipv4(self):
        resp = {"IsBimodal": True,
                "ActivePeerPrivateIPAddr": "10.2.3.4"}

        _, parsed_ip = rpc.parse_is_account_bimodal_response(resp)
        self.assertEqual(parsed_ip, "10.2.3.4")

    def test_ipv6_no_brackets(self):
        addr = "fc00:df02:c928:4ef2:f085:8af2:cf1b:6b4"
        resp = {"IsBimodal": True,
                "ActivePeerPrivateIPAddr": addr}

        _, parsed_ip = rpc.parse_is_account_bimodal_response(resp)
        self.assertEqual(parsed_ip, addr)

    def test_ipv6_brackets(self):
        addr = "fc00:bbb9:a634:aa7d:8cb1:1c1e:d1cb:519c"
        resp = {"IsBimodal": True,
                "ActivePeerPrivateIPAddr": "[{0}]".format(addr)}

        _, parsed_ip = rpc.parse_is_account_bimodal_response(resp)
        self.assertEqual(parsed_ip, addr)
