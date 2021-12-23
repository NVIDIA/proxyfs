// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

// Program idestroy provides a tool to destroy all contents of a ProxyFS container.
//
// The program requires a single argument that is a path to a package config
// formatted configuration to load. Optionally, overrides the the config may
// be passed as additional arguments in the form <section_name>.<option_name>=<value>.
//
// As it is expected to be used (e.g.. during development/testing) to clear out
// a container ultimately used by iclient/imgr, idestroy is designed to simply
// leverage the same iclient .conf file.
//
package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/NVIDIA/proxyfs/conf"
	"github.com/NVIDIA/proxyfs/iauth"
)

func main() {
	const (
		HTTPUserAgent = "idestroy"
	)

	var (
		AuthPlugInEnvName                string
		AuthPlugInEnvValue               string
		AuthPlugInPath                   string
		confMap                          conf.ConfMap
		err                              error
		httpClient                       http.Client
		httpDELETERequest                *http.Request
		httpDELETEResponse               *http.Response
		httpGETRequest                   *http.Request
		httpGETResponse                  *http.Response
		httpGETResponseBodyAsByteSlice   []byte
		httpGETResponseBodyAsString      string
		httpGetResponseBodyAsStringSlice []string
		objectName                       string
		swiftAuthToken                   string
		swiftStorageURL                  string
	)

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "no .conf file specified\n")
		os.Exit(1)
	}

	confMap, err = conf.MakeConfMapFromFile(os.Args[1])
	if nil != err {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	err = confMap.UpdateFromStrings(os.Args[2:])
	if nil != err {
		fmt.Fprintf(os.Stderr, "failed to apply config overrides: %v\n", err)
		os.Exit(1)
	}

	AuthPlugInPath, err = confMap.FetchOptionValueString("ICLIENT", "AuthPlugInPath")
	if nil != err {
		fmt.Printf("confMap.FetchOptionValueString(\"ICLIENT\", \"AuthPlugInPath\") failed: %v\n", err)
		os.Exit(1)
	}
	err = confMap.VerifyOptionIsMissing("ICLIENT", "AuthPlugInEnvName")
	if nil == err {
		AuthPlugInEnvName = ""
		AuthPlugInEnvValue, err = confMap.FetchOptionValueString("ICLIENT", "AuthPlugInEnvValue")
		if nil != err {
			fmt.Printf("confMap.FetchOptionValueString(\"ICLIENT\", \"AuthPlugInEnvValue\") failed: %v\n", err)
			os.Exit(1)
		}
	} else {
		err = confMap.VerifyOptionValueIsEmpty("ICLIENT", "AuthPlugInEnvName")
		if nil == err {
			AuthPlugInEnvName = ""
			AuthPlugInEnvValue, err = confMap.FetchOptionValueString("ICLIENT", "AuthPlugInEnvValue")
			if nil != err {
				fmt.Printf("confMap.FetchOptionValueString(\"ICLIENT\", \"AuthPlugInEnvValue\") failed: %v\n", err)
				os.Exit(1)
			}
		} else {
			AuthPlugInEnvName, err = confMap.FetchOptionValueString("ICLIENT", "AuthPlugInEnvName")
			if nil != err {
				fmt.Printf("confMap.FetchOptionValueString(\"ICLIENT\", \"AuthPlugInEnvName\") failed: %v\n", err)
				os.Exit(1)
			}
			err = confMap.VerifyOptionIsMissing("ICLIENT", "AuthPlugInEnvValue")
			if nil == err {
				AuthPlugInEnvValue = ""
			} else {
				err = confMap.VerifyOptionValueIsEmpty("ICLIENT", "AuthPlugInEnvValue")
				if nil == err {
					AuthPlugInEnvValue = ""
				} else {
					fmt.Printf("If [ICLIENT]AuthPlugInEnvName is present and non-empty, [ICLIENT]AuthPlugInEnvValue must be missing or empty\n")
					os.Exit(1)
				}
			}
		}
	}

	if AuthPlugInEnvName != "" {
		AuthPlugInEnvValue = os.Getenv(AuthPlugInEnvName)
	}

	swiftAuthToken, swiftStorageURL, err = iauth.PerformAuth(AuthPlugInPath, AuthPlugInEnvValue)
	if nil != err {
		fmt.Printf("iauth.PerformAuth(AuthPlugInPath, AuthPlugInEnvValue) failed: %v\n", err)
		os.Exit(1)
	}

	httpClient = http.Client{}

ReFetchObjectNameList:

	httpGETRequest, err = http.NewRequest("GET", swiftStorageURL, nil)
	if nil != err {
		fmt.Printf("http.NewRequest(\"GET\", swiftStorageURL, nil) failed: %v\n", err)
		os.Exit(1)
	}

	httpGETRequest.Header.Add("User-Agent", HTTPUserAgent)
	httpGETRequest.Header.Add("X-Auth-Token", swiftAuthToken)

	httpGETResponse, err = httpClient.Do(httpGETRequest)
	if nil != err {
		fmt.Printf("httpClient.Do(httpGETRequest) failed: %v\n", err)
		os.Exit(1)
	}
	if (httpGETResponse.StatusCode < 200) || (httpGETResponse.StatusCode > 299) {
		fmt.Printf("httpGETResponse.Status unexpected: %v\n", httpGETResponse.Status)
		os.Exit(1)
	}

	httpGETResponseBodyAsByteSlice, err = ioutil.ReadAll(httpGETResponse.Body)
	if nil != err {
		fmt.Printf("ioutil.ReadAll(httpGETResponse.Body) failed: %v\n", err)
		os.Exit(1)
	}
	httpGETResponseBodyAsString = string(httpGETResponseBodyAsByteSlice[:])

	httpGetResponseBodyAsStringSlice = strings.Fields(httpGETResponseBodyAsString)

	if len(httpGetResponseBodyAsStringSlice) == 0 {
		os.Exit(0)
	}

	for _, objectName = range httpGetResponseBodyAsStringSlice {
		httpDELETERequest, err = http.NewRequest("DELETE", swiftStorageURL+"/"+objectName, nil)
		if nil != err {
			fmt.Printf("http.NewRequest(\"DELETE\", swiftStorageURL+\"/\"+objectName, nil) failed: %v\n", err)
			os.Exit(1)
		}

		httpDELETERequest.Header.Add("User-Agent", HTTPUserAgent)
		httpDELETERequest.Header.Add("X-Auth-Token", swiftAuthToken)

		httpDELETEResponse, err = httpClient.Do(httpDELETERequest)
		if nil != err {
			fmt.Printf("httpClient.Do(httpDELETERequest) failed: %v\n", err)
			os.Exit(1)
		}

		_, err = ioutil.ReadAll(httpDELETEResponse.Body)
		if nil != err {
			fmt.Printf("ioutil.ReadAll(httpDELETEResponse.Body) failed: %v\n", err)
			os.Exit(1)
		}

		if (httpDELETEResponse.StatusCode < 200) || (httpDELETEResponse.StatusCode > 299) {
			fmt.Printf("httpDELETEResponse.Status unexpected: %v\n", httpDELETEResponse.Status)
			os.Exit(1)
		}
	}

	goto ReFetchObjectNameList
}
