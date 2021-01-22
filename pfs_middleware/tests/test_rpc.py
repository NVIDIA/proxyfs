# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0


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


class TestResponseParsing(unittest.TestCase):
    # These maybe aren't great, but they're more than what we had
    def test_get_object(self):
        resp = {
            "ReadEntsOut": 'a',
            "Metadata": 'Yg==',  # 'b'
            "FileSize": 'c',
            "ModificationTime": 'd',
            "AttrChangeTime": 'e',
            "IsDir": 'f',
            "InodeNumber": 'g',
            "NumWrites": 'h',
            "LeaseId": 'i',
        }
        self.assertEqual(rpc.parse_get_object_response(resp), (
            'a', 'b', 'c', 'e', 'f', 'g', 'h', 'i'))

        # older proxyfsd didn't send IsDir, so it will not be present in
        # older responses, but older GET would fail on a directory object
        # so False is correct if IsDir is not present.
        del resp["IsDir"]
        self.assertEqual(rpc.parse_get_object_response(resp), (
            'a', 'b', 'c', 'e', False, 'g', 'h', 'i'))

        # Old proxyfsd didn't send AttrChangeTime, but we've always had
        # ModificationTime available, which is the next-best option
        del resp["AttrChangeTime"]
        self.assertEqual(rpc.parse_get_object_response(resp), (
            'a', 'b', 'c', 'd', False, 'g', 'h', 'i'))

    def test_coalesce_object(self):
        resp = {
            "ModificationTime": 'a',
            "AttrChangeTime": 'b',
            "InodeNumber": 'c',
            "NumWrites": 'd',
        }
        self.assertEqual(rpc.parse_coalesce_object_response(resp), (
            'b', 'c', 'd'))
        # Old proxyfsd didn't send AttrChangeTime, but we've always had
        # ModificationTime available, which is the next-best option
        del resp["AttrChangeTime"]
        self.assertEqual(rpc.parse_coalesce_object_response(resp), (
            'a', 'c', 'd'))

    def test_put_complete(self):
        resp = {
            "ModificationTime": 'a',
            "AttrChangeTime": 'b',
            "InodeNumber": 'c',
            "NumWrites": 'd',
        }
        self.assertEqual(rpc.parse_put_complete_response(resp), (
            'b', 'c', 'd'))
        # Old proxyfsd didn't send AttrChangeTime, but we've always had
        # ModificationTime available, which is the next-best option
        del resp["AttrChangeTime"]
        self.assertEqual(rpc.parse_put_complete_response(resp), (
            'a', 'c', 'd'))

    def test_mkdir(self):
        resp = {
            "ModificationTime": 'a',
            "AttrChangeTime": 'b',
            "InodeNumber": 'c',
            "NumWrites": 'd',
        }
        self.assertEqual(rpc.parse_middleware_mkdir_response(resp), (
            'b', 'c', 'd'))
        # Old proxyfsd didn't send AttrChangeTime, but we've always had
        # ModificationTime available, which is the next-best option
        del resp["AttrChangeTime"]
        self.assertEqual(rpc.parse_middleware_mkdir_response(resp), (
            'a', 'c', 'd'))

    def test_get_account(self):
        resp = {
            "ModificationTime": 'a',
            "AttrChangeTime": 'b',
            "AccountEntries": ['c'],
        }
        self.assertEqual(rpc.parse_get_account_response(resp), ('b', ['c']))
        del resp["AttrChangeTime"]
        self.assertEqual(rpc.parse_get_account_response(resp), ('a', ['c']))

        resp = {
            "ModificationTime": 'a',
            "AttrChangeTime": 'b',
            "AccountEntries": None,
        }
        self.assertEqual(rpc.parse_get_account_response(resp), ('b', []))
        del resp["AttrChangeTime"]
        self.assertEqual(rpc.parse_get_account_response(resp), ('a', []))

    def test_head(self):
        resp = {
            "Metadata": 'YQ==',  # 'a'
            "ModificationTime": 'b',
            "AttrChangeTime": 'c',
            "FileSize": 'd',
            "IsDir": 'e',
            "InodeNumber": 'f',
            "NumWrites": 'g',
        }
        self.assertEqual(rpc.parse_head_response(resp), (
            'a', 'c', 'd', 'e', 'f', 'g'))

        # Old proxyfsd didn't send AttrChangeTime, but we've always had
        # ModificationTime available, which is the next-best option
        del resp["AttrChangeTime"]
        self.assertEqual(rpc.parse_head_response(resp), (
            'a', 'b', 'd', 'e', 'f', 'g'))
