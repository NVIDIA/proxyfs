// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/NVIDIA/proxyfs/conf"
	"github.com/NVIDIA/proxyfs/emswift/emswiftpkg"
)

func TestSwiftAuth(t *testing.T) {
	var (
		authInJSON  string
		authToken   string
		confMap     conf.ConfMap
		confStrings = []string{
			"EMSWIFT.AuthIPAddr=127.0.0.1",
			"EMSWIFT.AuthTCPPort=9997",
			"EMSWIFT.JRPCIPAddr=127.0.0.1",
			"EMSWIFT.JRPCTCPPort=9998",
			"EMSWIFT.NoAuthIPAddr=127.0.0.1",
			"EMSWIFT.NoAuthTCPPort=9999",
			"EMSWIFT.MaxAccountNameLength=256",
			"EMSWIFT.MaxContainerNameLength=256",
			"EMSWIFT.MaxObjectNameLength=1024",
			"EMSWIFT.AccountListingLimit=10000",
			"EMSWIFT.ContainerListingLimit=10000",
		}
		err        error
		storageURL string
	)

	confMap, err = conf.MakeConfMapFromStrings(confStrings)
	if nil != err {
		t.Fatalf("conf.MakeConfMapFromStrings(confStrings) returned unexpected error: %v", err)
	}

	err = emswiftpkg.Start(confMap)
	if nil != err {
		t.Fatalf("emswiftpkg.Start(confMap) returned unexpected error: %v", err)
	}

	authInJSON = "{" +
		"    \"AuthURL\" : \"http://127.0.0.1:9997/auth/v1.0\"," +
		"    \"AuthUser\" : \"test:tester\"," +
		"    \"AuthKey\" : \"testing\"," +
		"    \"Account\" : \"AUTH_test\"" +
		"}"

	authToken, storageURL, err = PerformAuth(authInJSON)
	if nil == err {
		t.Logf("authToken: %s", authToken)
		t.Logf("storageURL: %s", storageURL)
		err = emswiftpkg.Stop()
		if nil != err {
			t.Fatalf("emswiftpkg.Stop() returned unexpected error: %v", err)
		}
	} else {
		_ = emswiftpkg.Stop()
		t.Fatalf("PerformAuth failed: %v", err)
	}
}
