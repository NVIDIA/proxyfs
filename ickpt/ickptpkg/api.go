// Copyright (c) 2015-2022, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

// Package ickptpkg implements a simple KeyValue store used to provide Strict
// Consistency for storing and retrieving ilayout.CheckPointV*Struct's useful
// when the underlying Object Storage cannot guarantee Strict Consistency.
//
// Not to be confused with the complementary requirement of High Availability,
// ickptpkg only ensures that it will never return a stale ilayout.CheckPointV*Struct
// following successful completion of any update (COMMIT).
//
// To support High Availability, it is actually up to the "client" if ickptpkg
// (i.e. imgrpkg) to utilize N instances of ickptpkg. In addition, the monitonically
// increasing fields in an ilayout.CheckPointV*Struct (including those coming from
// an Eventually Consistent Object Store) may be used to break ties if desired, but
// this is the choice of the "client" (imgrpkg).
//
// This package implaments a simple HTTP-based API. As such, authorization will
// leverage the X-Auth-Token mechanism of the underlying Swift. Each API invocation
// will be authorized by fetching the Object Store's version via HTTP Header-provided
// X-Storage-Url that specifies the full path to the Object containing the
// ilayout.CheckPointV*Struct. The HTTP Header-provided X-Auth-Token will be used
// by the Object Store to verify access.
//
// Note that due to reliance on the X-Storage-Url HTTP Header to specify which
// particular ilayout.CheckPointV*Struct is being referenced, the Path portion
// of an HTTP Request is not used to describe the ProxyFS Volume referenced.
// Instead, the Path portion will be used as a TransactionID for the two-phase
// operations of deleting and setting/updating a ProxyFS Volume's CheckPoint.
// The Path should simply be empty for all other operations.
//
//  DELETE /
//
// Following a successful GET of the X-Storage-Url specifying the X-Auth-Token
// received, the local ilayout.CheckPointV*Struct value retained for that X-Storage-Url
// will be marked for deletion committed via a corresponding POST. The linkage between
// DELETE and POST requests is that they must share a common Path value.
//
//  GET /
//
// Following a successful GET of the X-Storage-Url specifying the X-Auth-Token
// received, the local ilayout.CheckPointV*Struct value retained for that X-Storage-Url
// will be returned. If no such value has been retained, the value returned in the GET
// will be returned instead. An uncommitted prior DELETE or PUT is ignored and discarded.
//
//  HEAD /
//
// A successful 204 No Content response indicates the package instance is in operation.
//
//  POST /
//
// Following a successful GET of the X-Storage-Url specifying the X-Auth-Token
// received, the prior as-yet uncommitted DELETE or PUT will be committed. In
// order to align this POST with the uncommitted DELETE or PUT, their Path values
// must match.
//
//  PUT /
//
// Following a successful GET of the X-Storage-Url specifying the X-Auth-Token
// received, the local ilayout.CheckPointV*Struct value for that X-Storage-Url
// will be set or updated according to the PUT Body. The linkage between PUT
// and POST requests is that they must share a common Path value.
//
// To configure an ickptpkg instance, Start() is called passing, as the sole
// argument, a package conf ConfMap. Here is a sample .conf file:
//
//  [ICKPT]
//  IPAddr:                  127.0.0.1
//  Port:                    33123        # TCP or TLS as determined by:
//  CertFilePath:                         #  TCP: if all of {Cert|Key|CACert}FilePath are missing or empty
//  KeyFilePath:                          #   - or -
//  CACertFilePath:                       #  TLS: if all of {Cert|Key|CACert}FilePath are present
//  DataBasePath:            /tmp/ickptDB # Implicitly created if if non-existent
//  SwiftTimeout:            10m
//  SwiftConnectionPoolSize: 128
//  TransactionTimeout:      10s
//
package ickptpkg

import (
	"github.com/NVIDIA/proxyfs/conf"
)

// Start is called to start serving.
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
