// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

// Package iswiftpkg implements an emulation of OpenStack Swift by presenting
// a Swift Proxy responding to a minimal set of base OpenStack Swift HTTP
// methods. While there is no support for TLS, a simple Auth functionality
// is provided and its usage is enforced.
//
// To configure an iswiftpkg instance, Start() is called passing, as the sole
// argument, a package conf ConfMap. Here is a sample .conf file:
//
//  [ISWIFT]
//  SwiftProxyIPAddr:       127.0.0.1
//  SwiftProxyTCPPort:      8080
//
//  MaxAccountNameLength:   256
//  MaxContainerNameLength: 256
//  MaxObjectNameLength:    1024
//  AccountListingLimit:    10000
//  ContainerListingLimit:  10000
//
package iswiftpkg

import (
	"github.com/NVIDIA/proxyfs/conf"
)

// Start is called to start serving the NoAuth Swift Proxy Port and,
// optionally, the Auth Swift Proxy Port.
//
func Start(confMap conf.ConfMap) (err error) {
	err = start(confMap)
	return
}

// Stop is called to stop serving.
//
func Stop() (err error) {
	err = stop()
	return
}

// ForceReAuth is called to force a "401 Unauthorized" response to a
// client's subsequent request forcing the client to reauthenticate.
//
func ForceReAuth() {
	forceReAuth()
}
