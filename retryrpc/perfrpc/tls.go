// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/tls"
	"crypto/x509/pkix"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/NVIDIA/proxyfs/icert/icertpkg"
)

type tlsCertsStruct struct {
	caCertPEMBlock       []byte
	caKeyPEMBlock        []byte
	endpointCertPEMBlock []byte
	endpointKeyPEMBlock  []byte
	endpointTLSCert      tls.Certificate
	caCertFile           string
	caKeyFile            string
	endpointCertFile     string
	endpointKeyFile      string
}

var tlsCerts *tlsCertsStruct

func tlsSetFileNames(tlsCerts *tlsCertsStruct, tlsDir string) {
	tlsCerts.caCertFile = tlsDir + "/caCertFile"
	tlsCerts.caKeyFile = tlsDir + "/caKeyFile"
	tlsCerts.endpointCertFile = tlsDir + "/endpointCertFile"
	tlsCerts.endpointKeyFile = tlsDir + "/endpointKeyFile"
}

// Utility function to initialize tlsCerts
func tlsCertsAllocate(ipAddr string, tlsDir string) (tlsCerts *tlsCertsStruct) {
	var (
		err error
	)

	tlsCerts = &tlsCertsStruct{}
	tlsSetFileNames(tlsCerts, tlsDir)

	tlsCerts.caCertPEMBlock, tlsCerts.caKeyPEMBlock, err = icertpkg.GenCACert(
		icertpkg.GenerateKeyAlgorithmEd25519,
		pkix.Name{
			Organization:  []string{"Test Organization CA"},
			Country:       []string{},
			Province:      []string{},
			Locality:      []string{},
			StreetAddress: []string{},
			PostalCode:    []string{},
		},
		24*time.Hour,
		tlsCerts.caCertFile,
		tlsCerts.caKeyFile)

	if err != nil {
		fmt.Printf("icertpkg.GenCACert() failed: %v", err)
		os.Exit(1)
	}

	tlsCerts.endpointCertPEMBlock, tlsCerts.endpointKeyPEMBlock, err = icertpkg.GenEndpointCert(
		icertpkg.GenerateKeyAlgorithmEd25519,
		pkix.Name{
			Organization:  []string{"Test Organization Endpoint"},
			Country:       []string{},
			Province:      []string{},
			Locality:      []string{},
			StreetAddress: []string{},
			PostalCode:    []string{},
		},
		[]string{},
		[]net.IP{net.ParseIP(ipAddr)},
		time.Hour,
		tlsCerts.caCertPEMBlock,
		tlsCerts.caKeyPEMBlock,
		tlsCerts.endpointCertFile,
		tlsCerts.endpointKeyFile)

	if err != nil {
		fmt.Printf("icertpkg.genEndpointCert() failed: %v", err)
		os.Exit(1)
	}

	tlsCerts.endpointTLSCert, err = tls.X509KeyPair(tlsCerts.endpointCertPEMBlock, tlsCerts.endpointKeyPEMBlock)
	if err != nil {
		fmt.Printf("tls.LoadX509KeyPair() failed: %v", err)
		os.Exit(1)
	}
	return
}
