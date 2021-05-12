// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

// Package icertpkg provides functions to generate certificates compatible
// with standard TLS and HTTPS packages. Functions are provided to create
// RootCA certificates that may then subsequently be used to generate
// endpoint certificates signed by a RootCA. The functions are designed to
// work with either on-disk PEM files and/or in-memory PEM blocks (byte slices).
//
// Inspired by https://shaneutt.com/blog/golang-ca-and-signed-cert-go/
//
package icertpkg

import (
	"crypto/x509/pkix"
	"net"
	"time"
)

const (
	// GenerateKeyAlgorithmEd25519 selects the Ed25519 signature algorithm.
	//
	GenerateKeyAlgorithmEd25519 = "ed25519"

	// GenerateKeyAlgorithmRSA selects the RSA signature algorithm.
	//
	GenerateKeyAlgorithmRSA = "rsa"

	// GenerateKeyAlgorithmRSABits is the number of bits that will be
	// used if GenerateKeyAlgorithmRSA is selected.
	//
	GenerateKeyAlgorithmRSABits = 4096

	// CertificateSerialNumberRandomBits is the number of bits that will
	// be randomly generated for a certificate's SerialNumber.
	//
	CertificateSerialNumberRandomBits = 256

	// GeneratedFilePerm is the permission bits that, after the application
	// of umask, will specify the mode of the created cert|key files.
	//
	GeneratedFilePerm = 0400
)

// GenCACert is called to generate a Certificate Authority using the requested
// generateKeyAlgorithm for the specified subject who's validity last for the
// desired ttl starting from time.Now(). The resultant PEM-encoded certPEMBlock
// and keyPEMBlock are returned. Optionally, certPEMBlock will be written to
// certFile (if not "") and/or keyPEMBlock will be written to keyFile (if not "").
// If certFile and keyFile are the same, both the CA Certificate and its Private
// Key will be written to the common file.
//
func GenCACert(generateKeyAlgorithm string, subject pkix.Name, ttl time.Duration, certFile string, keyFile string) (certPEMBlock []byte, keyPEMBlock []byte, err error) {
	certPEMBlock, keyPEMBlock, err = genCACert(generateKeyAlgorithm, subject, ttl, certFile, keyFile)
	return
}

// GenEndpointCert is called to generate a Certificate using the requested
// generateKeyAlgorithm for the specified subject who's validity lasts for
// the desired ttl starting from time.Now(). The endpoints for which the
// Certificate will apply will be the specified dnsNames and/or ipAddresses.
// The Certificate will be signed by the CA Certificate specified via
// caCert and caKey. Note that caCert and caKey may either be PEMBlock
// byte slices ([]byte) or file path strings. If caCert and caKey are both
// strings, they may refer to a common file. The resulted PEM-encoded
// endpoingCertPEMBlock and endpointKeyPEMBlock are returned. Optionally,
// endpoingCertPEMBlock will be written to endpointCertFile (if not "")
// and/or endpointKeyPEMBlock will be written to endpointKeyFile (if not "").
// If endpointCertFile and endpointKeyFile are the same, both the Endpoint
// Certificate and its Private Key will be written to the common file.
//
func GenEndpointCert(generateKeyAlgorithm string, subject pkix.Name, dnsNames []string, ipAddresses []net.IP, ttl time.Duration, caCert interface{}, caKey interface{}, endpointCertFile string, endpointKeyFile string) (endpointCertPEMBlock []byte, endpointKeyPEMBlock []byte, err error) {
	endpointCertPEMBlock, endpointKeyPEMBlock, err = genEndpointCert(generateKeyAlgorithm, subject, dnsNames, ipAddresses, ttl, caCert, caKey, endpointCertFile, endpointKeyFile)
	return
}
