// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

// Inspired by https://shaneutt.com/blog/golang-ca-and-signed-cert-go/

package icertpkg

import (
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
	GeneratedFilePerm = 0644
)

// GenCACert is called to generate a Certificate Authority for the specified
// organization using the requested generateKeyAlgorithm who's validity lasts
// for the desired ttl starting from time.Now(). The resultant PEM-encoded
// CA Certificate is written to certFile. The PEM-encoded private key for
// the CA Certificate is written to keyFile. If certFile and keyFile are
// the same, both the CA Certificate and its private key will be written to
// the common file.
//
func GenCACert(organization string, generateKeyAlgorithm string, ttl time.Duration, certFile string, keyFile string) (err error) {
	return genCACert(organization, generateKeyAlgorithm, ttl, certFile, keyFile)
}

// GenCert is called to generate a Certificate for the specified organization
// and IP Address using the requested generateKeyAlgorithm who's validity
// lasts for the desired ttl starting from time.Now(). The Certificate will
// be signed by the CA Certificate specified via caCertFile and caKeyFile.
// The caCertFile and caKeyFile values may be identical. The resultant
// PEM-encoded Certificate is written to certFile. The PEM-encoded private
// key for the Certificate is written to keyFile. If certFile and keyFile
// are the same, both the Certificate and its private key will be written to
// the common file.
//
func GenCert(organization string, ipAddress string, generateKeyAlgorithm string, ttl time.Duration, caCertFile string, caKeyFile string, certFile string, keyFile string) (err error) {
	return genCert(organization, ipAddress, generateKeyAlgorithm, ttl, caCertFile, caKeyFile, certFile, keyFile)
}
