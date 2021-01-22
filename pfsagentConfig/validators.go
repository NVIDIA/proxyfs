// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package pfsagentConfig

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

// ValidateAuthURL checks that an AUTH URL pointing to a SwiftStack cluster
// func ValidateAuthURL(urlToBeTested string) error {
// 	return nil
// }

// ValidateAccess runs all
func ValidateAccess() (errorType int, err error) {
	mySwiftParams := new(SwiftParams)
	mySwiftParams.User = confMap["Agent"]["SwiftAuthUser"][0]
	mySwiftParams.Key = confMap["Agent"]["SwiftAuthKey"][0]
	mySwiftParams.Account = confMap["Agent"]["SwiftAccountName"][0]
	mySwiftParams.AuthURL = confMap["Agent"]["SwiftAuthURL"][0]

	err = validateURL(mySwiftParams)
	if nil != err {
		errorType = typeAuthURL
		return
	}
	_, err = validateCredentails(mySwiftParams)
	if nil != err {
		errorType = typeCredentails
		return
	}
	err = validateAccount(mySwiftParams)
	if nil != err {
		errorType = typeAccount
		return
	}
	return -1, nil
}

func validateURL(mySwiftParams *SwiftParams) error {
	parts := strings.Split(mySwiftParams.AuthURL, "/")
	if len(parts) < 5 {
		return fmt.Errorf(`
Auth URL should be of the form:
<protocol>://<url>/auth/v<API version>
where
        - protocol is either 'http' or 'https'
        - url is the public address of the cluster (such as 'swiftcluster.com' or '192.168.2.100')
        - API version is usually in the format of 1.0, 2.0 etc.
`)
	}
	protocol := parts[0]
	if strings.Index(protocol, "http") != 0 && strings.Index(protocol, "https") != 0 {
		return fmt.Errorf("Auth URL Should Start With http:// Or https://\n\tYour Protocol is %v", protocol)
	}
	if parts[3] != "auth" {
		return fmt.Errorf("A Valid Auth URL Should Have The Word 'auth' After The Address.\n\tExample: https://swiftcluster.com/auth/v1.0")
	}
	if strings.Index(parts[4], "v") != 0 || len(parts[4]) != 4 {
		return fmt.Errorf("A Valid Auth URL Should Have The Protocol Version After The Term 'auth'.\n\tExample: https://swiftcluster.com/auth/v1.0")
	}
	address := strings.Split(parts[2], "/")[0]
	url := fmt.Sprintf("%v//%v", protocol, address)
	response, generalAccessErr := http.Get(url)
	if nil != generalAccessErr {
		return fmt.Errorf("Error Connecting To The Cluster's URL, Which Probably Means The URL Has Errors.\n\tDetected Cluster URL: %v\n\tError: %v", url, generalAccessErr)
	}
	infoURL := fmt.Sprintf("%v/info", url)
	response, httpErr := http.Get(infoURL)
	if nil != httpErr {
		return fmt.Errorf("Error Connecting To The Cluster's 'info' URL (%v), Though The URL Itself (%v) Is Reponding.\n\tPlease Check Your URL\n\tError: %v", infoURL, url, httpErr)
	}
	defer response.Body.Close()
	infoBody, bodyReadErr := ioutil.ReadAll(response.Body)
	if nil != bodyReadErr {
		return fmt.Errorf("Error Reading Response From %v\n\tError: %v", infoURL, bodyReadErr)
	}
	var infoInterface interface{}
	jsonErr := json.Unmarshal(infoBody, &infoInterface)
	if nil != jsonErr {
		return fmt.Errorf("Error Parsing Info From %v.\nBody Text: %v\n\tError: %v", infoURL, string(infoBody), jsonErr)
	}
	validJSON := infoInterface.(map[string]interface{})
	if nil == validJSON["swift"] {
		return fmt.Errorf("Error Finding The Term 'swift' In Parsed JSON\n\tError: %v", validJSON)
	}

	authClient := &http.Client{}
	authTokenRequest, authTokenErr := http.NewRequest("GET", mySwiftParams.AuthURL, nil)
	if nil != authTokenErr {
		fmt.Printf("Error Creting Auth Request:\n%v\n", authTokenErr)
	}
	authURLResponse, authURLErr := authClient.Do(authTokenRequest)
	if nil != authURLErr {
		return fmt.Errorf("Error Executing Auth URL Request:\n\tURL: %v\n\tError: %v", mySwiftParams.AuthURL, authURLErr)
	}
	if authURLResponse.StatusCode != 401 {
		return fmt.Errorf("Error Response From Auth URL. Get On Auth URL With No User/Key Should Result In Status '401 Unauthorize'\n\tStatus code: %v", authURLResponse.Status)
	}

	return nil
}

func validateCredentails(mySwiftParams *SwiftParams) (authToken string, err error) {
	authToken, err = GetAuthToken(mySwiftParams)
	if nil != err {
		return
	}
	mySwiftParams.AuthToken = authToken
	return
}

func validateAccount(mySwiftParams *SwiftParams) error {
	headers, headErr := GetAccountHeaders(mySwiftParams)
	fmt.Printf("\n\nvalidation succeeded? %v\n\n", nil == headErr)
	if nil != headErr {
		return headErr
	}
	if len(headers) == 0 {
		return fmt.Errorf("Though The Request Succeeded There Were No Headers Returned")
	}
	return nil
}
