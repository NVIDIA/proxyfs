// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

type authInStruct struct {
	AuthURL   string
	AuthUser  string
	AuthKey   string
	Account   string
	Container string
}

// PerformAuth accepts a JSON string, performs the OpenStack Swift Authorization
// process and returns the AuthToken and StorageURL that may be used to access
// the OpenStack Swift Account/Container/Objects.
//
// The format of authInJSON is determined by the Go JSON unmarshalling conventions
// of the AuthInStruct declared above.
//
func PerformAuth(authInJSON string) (authToken string, storageURL string, err error) {
	var (
		authIn          authInStruct
		authRequest     *http.Request
		authResponse    *http.Response
		storageURLSplit []string
	)

	err = json.Unmarshal([]byte(authInJSON), &authIn)
	if nil != err {
		err = fmt.Errorf("json.Unmarshal(\"%s\",) failed: %v", authInJSON, err)
		return
	}

	authRequest, err = http.NewRequest("GET", authIn.AuthURL, nil)
	if nil != err {
		err = fmt.Errorf("http.NewRequest(\"GET\", \"%s\", nil) failed: %v", authIn.AuthURL, err)
		return
	}

	authRequest.Header.Add("X-Auth-User", authIn.AuthUser)
	authRequest.Header.Add("X-Auth-Key", authIn.AuthKey)

	authRequest.Header.Add("User-Agent", "iauth-swift")

	authResponse, err = http.DefaultClient.Do(authRequest)
	if nil != err {
		err = fmt.Errorf("http.DefaultClient.Do(authRequest) failed: %v", err)
		return
	}

	if http.StatusOK != authResponse.StatusCode {
		err = fmt.Errorf("authResponse.Status unexpected: %v", authResponse.Status)
		return
	}

	authToken = authResponse.Header.Get("X-Auth-Token")
	storageURL = authResponse.Header.Get("X-Storage-Url")

	if strings.HasPrefix(authIn.AuthURL, "https://") && strings.HasPrefix(storageURL, "http://") {
		// We need to correct for the case where AuthURL starts with "https://""
		// but the Swift Proxy is behind a TLS terminating proxy. In this case,
		// Swift Proxy auth will actually see an AuthURL starting with "http://""
		// and respond with a StorageURL starting with "http://"".

		storageURL = strings.Replace(storageURL, "http://", "https://", 1)
	}

	storageURLSplit = strings.Split(storageURL, "/")

	storageURLSplit[4] = authIn.Account

	storageURLSplit = append(storageURLSplit, authIn.Container)

	storageURL = strings.Join(storageURLSplit, "/")

	err = nil
	return
}
