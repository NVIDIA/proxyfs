// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/NVIDIA/proxyfs/conf"
	"github.com/NVIDIA/proxyfs/iswift/iswiftpkg"
)

func TestSwiftAuth(t *testing.T) {
	var (
		authInJSON  string
		authToken   string
		confMap     conf.ConfMap
		confStrings = []string{
			"ISWIFT.SwiftProxyIPAddr=127.0.0.1",
			"ISWIFT.SwiftProxyTCPPort=8443",
			"ISWIFT.MaxAccountNameLength=256",
			"ISWIFT.MaxContainerNameLength=256",
			"ISWIFT.MaxObjectNameLength=1024",
			"ISWIFT.AccountListingLimit=10000",
			"ISWIFT.ContainerListingLimit=10000",
		}
		err        error
		storageURL string
	)

	confMap, err = conf.MakeConfMapFromStrings(confStrings)
	if nil != err {
		t.Fatalf("conf.MakeConfMapFromStrings(confStrings) returned unexpected error: %v", err)
	}

	err = iswiftpkg.Start(confMap)
	if nil != err {
		t.Fatalf("iswiftpkg.Start(confMap) returned unexpected error: %v", err)
	}

	authInJSON = "{" +
		"    \"AuthURL\" : \"http://127.0.0.1:8443/auth/v1.0\"," +
		"    \"AuthUser\" : \"test:tester\"," +
		"    \"AuthKey\" : \"testing\"," +
		"    \"Account\" : \"AUTH_test\"," +
		"    \"Container\" : \"con\"" +
		"}"

	authToken, storageURL, err = PerformAuth(authInJSON)
	if nil == err {
		t.Logf("authToken: %s", authToken)
		t.Logf("storageURL: %s", storageURL)
		err = iswiftpkg.Stop()
		if nil != err {
			t.Fatalf("iswiftpkg.Stop() returned unexpected error: %v", err)
		}
	} else {
		_ = iswiftpkg.Stop()
		t.Fatalf("PerformAuth failed: %v", err)
	}
}
