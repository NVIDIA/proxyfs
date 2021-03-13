// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package icertpkg

import (
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"math/big"
	"net"
	"time"
)

func genCACert(generateKeyAlgorithm string, subject pkix.Name, ttl time.Duration, certFile string, keyFile string) (err error) {
	var (
		caX509Certificate         []byte
		caX509CertificateTemplate *x509.Certificate
		certPEM                   []byte
		ed25519PrivateKey         ed25519.PrivateKey
		ed25519PublicKey          ed25519.PublicKey
		keyPEM                    []byte
		pkcs8PrivateKey           []byte
		rsaPrivateKey             *rsa.PrivateKey
		rsaPublicKey              crypto.PublicKey
		serialNumber              *big.Int
		serialNumberMax           *big.Int
		timeNow                   time.Time
	)

	serialNumberMax = big.NewInt(0)
	_ = serialNumberMax.Exp(big.NewInt(2), big.NewInt(CertificateSerialNumberRandomBits), nil)

	serialNumber, err = rand.Int(rand.Reader, serialNumberMax)
	if nil != err {
		return
	}

	timeNow = time.Now()

	caX509CertificateTemplate = &x509.Certificate{
		SerialNumber:          serialNumber,
		Subject:               subject,
		NotBefore:             timeNow,
		NotAfter:              timeNow.Add(ttl),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	switch generateKeyAlgorithm {
	case GenerateKeyAlgorithmEd25519:
		ed25519PublicKey, ed25519PrivateKey, err = ed25519.GenerateKey(rand.Reader)
		if nil != err {
			return
		}

		caX509Certificate, err = x509.CreateCertificate(rand.Reader, caX509CertificateTemplate, caX509CertificateTemplate, ed25519PublicKey, ed25519PrivateKey)
		if nil != err {
			return
		}

		pkcs8PrivateKey, err = x509.MarshalPKCS8PrivateKey(ed25519PrivateKey)
		if nil != err {
			return
		}
	case GenerateKeyAlgorithmRSA:
		rsaPrivateKey, err = rsa.GenerateKey(rand.Reader, GenerateKeyAlgorithmRSABits)
		if nil != err {
			return
		}
		rsaPublicKey = rsaPrivateKey.Public()

		caX509Certificate, err = x509.CreateCertificate(rand.Reader, caX509CertificateTemplate, caX509CertificateTemplate, rsaPublicKey, rsaPrivateKey)
		if nil != err {
			return
		}

		pkcs8PrivateKey, err = x509.MarshalPKCS8PrivateKey(rsaPrivateKey)
		if nil != err {
			return
		}
	default:
		err = fmt.Errorf("generateKeyAlgorithm \"%s\" not supported... must be one of \"%s\" or \"%s\"", generateKeyAlgorithm, GenerateKeyAlgorithmEd25519, GenerateKeyAlgorithmRSA)
		return
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caX509Certificate})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8PrivateKey})

	if certFile == keyFile {
		err = ioutil.WriteFile(certFile, append(certPEM, keyPEM...), GeneratedFilePerm)
		if nil != err {
			return
		}
	} else {
		err = ioutil.WriteFile(certFile, certPEM, GeneratedFilePerm)
		if nil != err {
			return
		}
		err = ioutil.WriteFile(keyFile, keyPEM, GeneratedFilePerm)
		if nil != err {
			return
		}
	}

	err = nil
	return
}

func genEndpointCert(generateKeyAlgorithm string, subject pkix.Name, dnsNames []string, ipAddresses []net.IP, ttl time.Duration, caCertFile string, caKeyFile string, endpointCertFile string, endpointKeyFile string) (err error) {
	var (
		caTLSCertificate        tls.Certificate
		caX509Certificate       *x509.Certificate
		certPEM                 []byte
		ed25519PrivateKey       ed25519.PrivateKey
		ed25519PublicKey        ed25519.PublicKey
		keyPEM                  []byte
		pkcs8PrivateKey         []byte
		rsaPrivateKey           *rsa.PrivateKey
		rsaPublicKey            crypto.PublicKey
		serialNumber            *big.Int
		serialNumberMax         *big.Int
		timeNow                 time.Time
		x509Certificate         []byte
		x509CertificateTemplate *x509.Certificate
	)

	serialNumberMax = big.NewInt(0)
	_ = serialNumberMax.Exp(big.NewInt(2), big.NewInt(CertificateSerialNumberRandomBits), nil)

	serialNumber, err = rand.Int(rand.Reader, serialNumberMax)
	if nil != err {
		return
	}

	timeNow = time.Now()

	x509CertificateTemplate = &x509.Certificate{
		SerialNumber:          serialNumber,
		Subject:               subject,
		DNSNames:              dnsNames,
		IPAddresses:           ipAddresses,
		NotBefore:             timeNow,
		NotAfter:              timeNow.Add(ttl),
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}

	caTLSCertificate, err = tls.LoadX509KeyPair(caCertFile, caKeyFile)
	if nil != err {
		return
	}

	caX509Certificate, err = x509.ParseCertificate(caTLSCertificate.Certificate[0])
	if nil != err {
		return
	}

	switch generateKeyAlgorithm {
	case GenerateKeyAlgorithmEd25519:
		ed25519PublicKey, ed25519PrivateKey, err = ed25519.GenerateKey(rand.Reader)
		if nil != err {
			return
		}

		x509Certificate, err = x509.CreateCertificate(rand.Reader, x509CertificateTemplate, caX509Certificate, ed25519PublicKey, caTLSCertificate.PrivateKey)
		if nil != err {
			return
		}

		pkcs8PrivateKey, err = x509.MarshalPKCS8PrivateKey(ed25519PrivateKey)
		if nil != err {
			return
		}
	case GenerateKeyAlgorithmRSA:
		rsaPrivateKey, err = rsa.GenerateKey(rand.Reader, GenerateKeyAlgorithmRSABits)
		if nil != err {
			return
		}
		rsaPublicKey = rsaPrivateKey.Public()

		x509Certificate, err = x509.CreateCertificate(rand.Reader, x509CertificateTemplate, caX509Certificate, rsaPublicKey, caTLSCertificate.PrivateKey)
		if nil != err {
			return
		}

		pkcs8PrivateKey, err = x509.MarshalPKCS8PrivateKey(rsaPrivateKey)
		if nil != err {
			return
		}
	default:
		err = fmt.Errorf("generateKeyAlgorithm \"%s\" not supported... must be one of \"%s\" or \"%s\"", generateKeyAlgorithm, GenerateKeyAlgorithmEd25519, GenerateKeyAlgorithmRSA)
		return
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: x509Certificate})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8PrivateKey})

	if endpointCertFile == endpointKeyFile {
		err = ioutil.WriteFile(endpointCertFile, append(certPEM, keyPEM...), GeneratedFilePerm)
		if nil != err {
			return
		}
	} else {
		err = ioutil.WriteFile(endpointCertFile, certPEM, GeneratedFilePerm)
		if nil != err {
			return
		}
		err = ioutil.WriteFile(endpointKeyFile, keyPEM, GeneratedFilePerm)
		if nil != err {
			return
		}
	}

	err = nil
	return
}
