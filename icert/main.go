// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"time"

	"github.com/NVIDIA/proxyfs/icert/icertpkg"
)

func main() {
	var (
		err error
	)

	err = icertpkg.GenCACert("Test Organization CA", icertpkg.GenerateKeyAlgorithmEd25519, time.Duration(time.Hour), "./ed25519_ca_cert.pem", "./ed25519_ca_key.pem")
	if nil != err {
		fmt.Printf("UNDO: icertpkg.GenCACert(,icertpkg.GenerateKeyAlgorithmEd25519,,\"./ed25519_ca_cert.pem\",\"./ed25519_ca_key.pem\") returned err: %v\n", err)
		os.Exit(1)
	}
	err = icertpkg.GenCACert("Test Organization CA", icertpkg.GenerateKeyAlgorithmEd25519, time.Duration(time.Hour), "./ed25519_ca_combined.pem", "./ed25519_ca_combined.pem")
	if nil != err {
		fmt.Printf("UNDO: icertpkg.GenCACert(,icertpkg.GenerateKeyAlgorithmEd25519,,\"./ed25519_ca_combined.pem\",\"./ed25519_ca_combined.pem\") returned err: %v\n", err)
		os.Exit(2)
	}

	err = icertpkg.GenCACert("Test Organization CA", icertpkg.GenerateKeyAlgorithmRSA, time.Duration(time.Hour), "./rsa_ca_cert.pem", "./rsa_ca_key.pem")
	if nil != err {
		fmt.Printf("UNDO: icertpkg.GenCACert(,icertpkg.GenerateKeyAlgorithmRSA,,\"./rsa_ca_cert.pem\",\"./rsa_ca_key.pem\") returned err: %v\n", err)
		os.Exit(3)
	}
	err = icertpkg.GenCACert("Test Organization CA", icertpkg.GenerateKeyAlgorithmRSA, time.Duration(time.Hour), "./rsa_ca_combined.pem", "./rsa_ca_combined.pem")
	if nil != err {
		fmt.Printf("UNDO: icertpkg.GenCACert(,icertpkg.GenerateKeyAlgorithmRSA,,\"./rsa_ca_combined.pem\",\"./rsa_ca_combined.pem\") returned err: %v\n", err)
		os.Exit(4)
	}

	err = icertpkg.GenCert("Test Organization Endpoint", "127.0.0.1", icertpkg.GenerateKeyAlgorithmEd25519, time.Duration(time.Hour), "./ed25519_ca_cert.pem", "./ed25519_ca_key.pem", "./ed25519_cert.pem", "./ed25519_key.pem")
	if nil != err {
		fmt.Printf("UNDO: icertpkg.GenCert(,,icertpkg.GenerateKeyAlgorithmEd25519,,\"./ed25519_ca_cert.pem\",\"./ed25519_ca_key.pem\",\"./ed25519_cert.pem\",\"./ed25519_key.pem\") returned err: %v\n", err)
		os.Exit(5)
	}
	err = icertpkg.GenCert("Test Organization Endpoint", "127.0.0.1", icertpkg.GenerateKeyAlgorithmEd25519, time.Duration(time.Hour), "./ed25519_ca_combined.pem", "./ed25519_ca_combined.pem", "./ed25519_combined.pem", "./ed25519_combined.pem")
	if nil != err {
		fmt.Printf("UNDO: icertpkg.GenCert(,,icertpkg.GenerateKeyAlgorithmEd25519,,\"./ed25519_ca_combined.pem\",\"./ed25519_ca_combined.pem\",\"./ed25519_combined.pem\",\"./ed25519_combined.pem\") returned err: %v\n", err)
		os.Exit(6)
	}

	err = icertpkg.GenCert("Test Organization Endpoint", "127.0.0.1", icertpkg.GenerateKeyAlgorithmRSA, time.Duration(time.Hour), "./rsa_ca_cert.pem", "./rsa_ca_key.pem", "./rsa_cert.pem", "./rsa_key.pem")
	if nil != err {
		fmt.Printf("UNDO: icertpkg.GenCert(,,icertpkg.GenerateKeyAlgorithmRSA,,\"./rsa_ca_cert.pem\",\"./rsa_ca_key.pem\",\"./rsa_cert.pem\",\"./rsa_key.pem\") returned err: %v\n", err)
		os.Exit(7)
	}
	err = icertpkg.GenCert("Test Organization Endpoint", "127.0.0.1", icertpkg.GenerateKeyAlgorithmRSA, time.Duration(time.Hour), "./rsa_ca_combined.pem", "./rsa_ca_combined.pem", "./rsa_combined.pem", "./rsa_combined.pem")
	if nil != err {
		fmt.Printf("UNDO: icertpkg.GenCert(,,icertpkg.GenerateKeyAlgorithmRSA,,\"./rsa_ca_combined.pem\",\"./rsa_ca_combined.pem\",\"./rsa_combined.pem\",\"./rsa_combined.pem\") returned err: %v\n", err)
		os.Exit(8)
	}
}
