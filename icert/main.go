// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/x509/pkix"
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/NVIDIA/proxyfs/icert/icertpkg"
)

type stringSlice []string

func (sS *stringSlice) String() (toReturn string) {
	toReturn = fmt.Sprint(*sS)
	return
}

func (sS *stringSlice) Set(s string) (err error) {
	*sS = append(*sS, s)
	err = nil
	return
}

func main() {
	var (
		verboseFlag = flag.Bool("v", false, "verbose mode")

		caFlag = flag.Bool("ca", false, "generated CA Certicate usable for signing Endpoint Certificates")

		generateKeyAlgorithmEd25519Flag = flag.Bool(icertpkg.GenerateKeyAlgorithmEd25519, false, "generate key via Ed25519")
		generateKeyAlgorithmRSAFlag     = flag.Bool(icertpkg.GenerateKeyAlgorithmRSA, false, "generate key via RSA")

		organizationFlag  stringSlice
		countryFlag       stringSlice
		provinceFlag      stringSlice
		localityFlag      stringSlice
		streetAddressFlag stringSlice
		postalCodeFlag    stringSlice

		ttlFlag = flag.Duration("ttl", time.Duration(0), "generated Certificate's time to live")

		dnsNamesFlag    stringSlice
		ipAddressesFlag stringSlice

		caCertPemFilePathFlag = flag.String("caCert", "", "path to CA Certificate")
		caKeyPemFilePathFlag  = flag.String("caKey", "", "path to CA Certificate's PrivateKey")

		endpointCertPemFilePathFlag = flag.String("cert", "", "path to Endpoint Certificate")
		endpointKeyPemFilePathFlag  = flag.String("key", "", "path to Endpoint Certificate's PrivateKey")

		err                  error
		generateKeyAlgorithm string
		ipAddresses          []net.IP
		subject              pkix.Name
	)

	flag.Var(&organizationFlag, "organization", "generated Certificate's Subject.Organization")
	flag.Var(&countryFlag, "country", "generated Certificate's Subject.Country")
	flag.Var(&provinceFlag, "province", "generated Certificate's Subject.Province")
	flag.Var(&localityFlag, "locality", "generated Certificate's Subject.Locality")
	flag.Var(&streetAddressFlag, "streetAddress", "generated Certificate's Subject.StreetAddress")
	flag.Var(&postalCodeFlag, "postalCode", "generated Certificate's Subject.PostalCode")

	flag.Var(&dnsNamesFlag, "dns", "generated Certificate's DNS Name")
	flag.Var(&ipAddressesFlag, "ip", "generated Certificate's IP Address")

	flag.Parse()

	if *verboseFlag {
		fmt.Printf("                         caFlag: %v\n", *caFlag)
		fmt.Println()
		fmt.Printf("generateKeyAlgorithmEd25519Flag: %v\n", *generateKeyAlgorithmEd25519Flag)
		fmt.Printf("    generateKeyAlgorithmRSAFlag: %v\n", *generateKeyAlgorithmRSAFlag)
		fmt.Println()
		fmt.Printf("               organizationFlag: %v\n", organizationFlag)
		fmt.Printf("                    countryFlag: %v\n", countryFlag)
		fmt.Printf("                   provinceFlag: %v\n", provinceFlag)
		fmt.Printf("                   localityFlag: %v\n", localityFlag)
		fmt.Printf("              streetAddressFlag: %v\n", streetAddressFlag)
		fmt.Printf("                 postalCodeFlag: %v\n", postalCodeFlag)
		fmt.Println()
		fmt.Printf("                        ttlFlag: %v\n", *ttlFlag)
		fmt.Println()
		fmt.Printf("                   dnsNamesFlag: %v\n", dnsNamesFlag)
		fmt.Printf("                ipAddressesFlag: %v\n", ipAddressesFlag)
		fmt.Println()
		fmt.Printf("          caCertPemFilePathFlag: \"%v\"\n", *caCertPemFilePathFlag)
		fmt.Printf("           caKeyPemFilePathFlag: \"%v\"\n", *caKeyPemFilePathFlag)
		fmt.Println()
		fmt.Printf("    endpointCertPemFilePathFlag: \"%v\"\n", *endpointCertPemFilePathFlag)
		fmt.Printf("     endpointKeyPemFilePathFlag: \"%v\"\n", *endpointKeyPemFilePathFlag)
	}

	if *generateKeyAlgorithmEd25519Flag {
		if *generateKeyAlgorithmRSAFlag {
			fmt.Printf("Precisely one of -%s or -%s must be specified\n", icertpkg.GenerateKeyAlgorithmEd25519, icertpkg.GenerateKeyAlgorithmRSA)
			os.Exit(1)
		}

		generateKeyAlgorithm = icertpkg.GenerateKeyAlgorithmEd25519
	} else if *generateKeyAlgorithmRSAFlag {
		generateKeyAlgorithm = icertpkg.GenerateKeyAlgorithmRSA
	} else {
		fmt.Printf("Precisely one of -%s or -%s must be specified\n", icertpkg.GenerateKeyAlgorithmEd25519, icertpkg.GenerateKeyAlgorithmRSA)
		os.Exit(1)
	}

	if time.Duration(0) == *ttlFlag {
		fmt.Printf("A non-zero -ttl must be specified\n")
		os.Exit(1)
	}

	if ("" == *caCertPemFilePathFlag) || ("" == *caKeyPemFilePathFlag) {
		fmt.Printf("Both -caCert and -caKey must be specified\n")
		os.Exit(1)
	}

	if *caFlag {
		if ("" != *endpointCertPemFilePathFlag) || ("" != *endpointKeyPemFilePathFlag) {
			fmt.Printf("If -ca is specified, neither -cert nor -key may be specified\n")
			os.Exit(1)
		}
		if (0 != len(dnsNamesFlag)) || (0 != len(ipAddressesFlag)) {
			fmt.Printf("If -ca is specified, neither -dns nor -ip may be specified\n")
			os.Exit(1)
		}
	} else {
		if ("" == *endpointCertPemFilePathFlag) || ("" == *endpointKeyPemFilePathFlag) {
			fmt.Printf("If -ca is not specified, both -cert and -key must be specified\n")
			os.Exit(1)
		}
		if (0 == len(dnsNamesFlag)) && (0 == len(ipAddressesFlag)) {
			fmt.Printf("If -ca is not specified, at least one -dns or -ip must be specified\n")
			os.Exit(1)
		}
	}

	subject = pkix.Name{
		Organization:  organizationFlag,
		Country:       countryFlag,
		Province:      provinceFlag,
		Locality:      localityFlag,
		StreetAddress: streetAddressFlag,
		PostalCode:    postalCodeFlag,
	}

	if *caFlag {
		_, _, err = icertpkg.GenCACert(generateKeyAlgorithm, subject, *ttlFlag, *caCertPemFilePathFlag, *caKeyPemFilePathFlag)
		if nil != err {
			fmt.Printf("icertpkg.GenCACert() failed: %v\n", err)
			os.Exit(1)
		}

		if *verboseFlag {
			fmt.Printf("icertpkg.GenCACert() generated caCert: \"%s\" and caKey: \"%s\"\n", *caCertPemFilePathFlag, *caKeyPemFilePathFlag)
		}
	} else {
		ipAddresses = make([]net.IP, 0, len(ipAddressesFlag))

		for _, ipAddress := range ipAddressesFlag {
			ipAddresses = append(ipAddresses, net.ParseIP(ipAddress))
		}

		_, _, err = icertpkg.GenEndpointCert(generateKeyAlgorithm, subject, dnsNamesFlag, ipAddresses, *ttlFlag, *caCertPemFilePathFlag, *caKeyPemFilePathFlag, *endpointCertPemFilePathFlag, *endpointKeyPemFilePathFlag)
		if nil != err {
			fmt.Printf("icertpkg.GenEndpointCert() failed: %v\n", err)
			os.Exit(1)
		}

		if *verboseFlag {
			fmt.Printf("icertpkg.GenEndpointCert() generated cert: \"%s\" and key: \"%s\"\n", *endpointCertPemFilePathFlag, *endpointKeyPemFilePathFlag)
		}
	}
}
