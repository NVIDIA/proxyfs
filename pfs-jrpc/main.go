// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/jrpcfs"
)

type jrpcRequestStruct struct {
	JSONrpc string         `json:"jsonrpc"`
	Method  string         `json:"method"`
	ID      uint64         `json:"id"`
	Params  [1]interface{} `json:"params"`
}

type jrpcRequestEmptyParamStruct struct{}

type jrpcResponseIDAndErrorStruct struct {
	ID    uint64 `json:"id"`
	Error string `json:"error"`
}

type jrpcResponseNoErrorStruct struct {
	ID     uint64      `json:"id"`
	Result interface{} `json:"result"`
}

type envStruct struct {
	AuthToken     string
	StorageURL    string
	LastMessageID uint64
	MountID       jrpcfs.MountIDAsString
}

var (
	authKey  string
	authURL  string
	authUser string
	envPath  string
	verbose  bool

	env *envStruct
)

func main() {
	var (
		args         []string
		cmd          string
		confFilePath string
		confMap      conf.ConfMap
		err          error
	)

	// Parse arguments

	args = os.Args[1:]

	if 2 > len(args) {
		doHelp()
		log.Fatalf("Must specify a .conf and a command")
	}

	confFilePath = args[0]

	confMap, err = conf.MakeConfMapFromFile(confFilePath)
	if nil != err {
		log.Fatalf("Failed to load %s: %v", confFilePath, err)
	}

	authURL, err = confMap.FetchOptionValueString("JRPCTool", "AuthURL")
	if nil != err {
		log.Fatalf("Failed to parse %s value for JRPCTool.AuthURL: %v", confFilePath, err)
	}
	if !strings.HasPrefix(strings.ToLower(authURL), "http:") && !strings.HasPrefix(strings.ToLower(authURL), "https:") {
		log.Fatalf("JRPCTool.AuthURL (\"%s\") must start with either \"http:\" or \"https:\"", authURL)
	}
	authUser, err = confMap.FetchOptionValueString("JRPCTool", "AuthUser")
	if nil != err {
		log.Fatalf("Failed to parse %s value for JRPCTool.AuthUser: %v", confFilePath, err)
	}
	authKey, err = confMap.FetchOptionValueString("JRPCTool", "AuthKey")
	if nil != err {
		log.Fatalf("Failed to parse %s value for JRPCTool.AuthKey: %v", confFilePath, err)
	}
	envPath, err = confMap.FetchOptionValueString("JRPCTool", "EnvPath")
	if nil != err {
		log.Fatalf("Failed to parse %s value for JRPCTool.EnvPath: %v", confFilePath, err)
	}
	verbose, err = confMap.FetchOptionValueBool("JRPCTool", "Verbose")
	if nil != err {
		log.Fatalf("Failed to parse %s value for JRPCTool.Verbose: %v", confFilePath, err)
	}

	cmd = args[1]

	switch cmd {
	case "a":
		doAuth()
	case "m":
		doMount()
	case "r":
		doJRPC(args[2:]...)
	case "c":
		doClean()
	default:
		doHelp()
		log.Fatalf("Could not understand command \"%s\"", cmd)
	}
}

func doHelp() {
	log.Printf("pfs-jrpc - commandline ProxyFS JSON RPC tool")
	log.Printf("")
	log.Printf("Usage: pfs-jrpc <.conf> a")
	log.Printf("       pfs-jrpc <.conf> m")
	log.Printf("       pfs-jrpc <.conf> r Server.RpcXXX ['\"Key\":<value>[,\"Key\":<value>]*']")
	log.Printf("       pfs-jrpc <.conf> c")
	log.Printf("")
	log.Printf("Commands:")
	log.Printf("")
	log.Printf(" a                                  pass AuthUser/AuthKey to AuthURL to fetch AuthToken/StorageURL")
	log.Printf(" m                                  perform Server.RpcMountByAccountName of AuthUser's Account")
	log.Printf(" r Server.RpcXXX <JSON RPC params>  perform selected Server.RpcXXX passing JSON RPC params (if any)")
	log.Printf(" c                                  clean up EnvPath")
	log.Printf("")
	log.Printf("Example:")
	log.Printf("")
	log.Printf("$ cat pfs-jrpc.conf")
	log.Printf("")
	log.Printf("[JRPCTool]")
	log.Printf("AuthURL:     http://localhost:8080/auth/v1.0")
	log.Printf("AuthUser:                        test:tester")
	log.Printf("AuthKey:                             testing")
	log.Printf("EnvPath:                      ./pfs-jrpc.env")
	log.Printf("Verbose:                                true")
	log.Printf("")
	log.Printf("$ pfs-jrpc pfs-jrpc.conf a")
	log.Printf("")
	log.Printf("Auth Request:")
	log.Printf("  httpRequest.URL:    http://localhost:8080/auth/v1.0")
	log.Printf("  httpRequest.Header: map[X-Auth-Key:[testing] X-Auth-User:[test:tester]]")
	log.Printf("Auth Response:")
	log.Printf("  env.AuthToken:  AUTH_tk928c2374f62c4de3bfffe4c62bce2e5f")
	log.Printf("  env.StorageURL: http://localhost:8080/v1/AUTH_test")
	log.Printf("")
	log.Printf("$ pfs-jrpc pfs-jrpc.conf m")
	log.Printf("")
	log.Printf("jrpcRequestBuf:")
	log.Printf("{")
	log.Printf("    \"jsonrpc\": \"2.0\",")
	log.Printf("    \"method\": \"Server.RpcMountByAccountName\",")
	log.Printf("    \"id\": 1,")
	log.Printf("    \"params\": [")
	log.Printf("        {")
	log.Printf("            \"AccountName\": \"AUTH_test\",")
	log.Printf("            \"MountOptions\": 0,")
	log.Printf("            \"AuthUserID\": 0,")
	log.Printf("            \"AuthGroupID\": 0")
	log.Printf("        }")
	log.Printf("    ]")
	log.Printf("}")
	log.Printf("httpResponseBody:")
	log.Printf("{")
	log.Printf("    \"error\": null,")
	log.Printf("    \"id\": 1,")
	log.Printf("    \"result\": {")
	log.Printf("        \"RootCAx509CertificatePEM\": \"LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJVVENDQVFPZ0F3SUJBZ0lSQVB3eGNWNGtmOGk5RkxhODViQlg0amt3QlFZREsyVndNQ3d4R0RBV0JnTlYKQkFvVEQwTkJJRTl5WjJGdWFYcGhkR2x2YmpFUU1BNEdBMVVFQXhNSFVtOXZkQ0JEUVRBZ0Z3MHhPVEF4TURFdwpNREF3TURCYUdBOHlNVEU1TURFd01UQXdNREF3TUZvd0xERVlNQllHQTFVRUNoTVBRMEVnVDNKbllXNXBlbUYwCmFXOXVNUkF3RGdZRFZRUURFd2RTYjI5MElFTkJNQ293QlFZREsyVndBeUVBUjlrZ1ZNaFpoaHpDMTJCa0RUMkQKUHE3OTJoRThEVVRLd0ZvbjNKcVhwbTZqT0RBMk1BNEdBMVVkRHdFQi93UUVBd0lDaERBVEJnTlZIU1VFRERBSwpCZ2dyQmdFRkJRY0RBVEFQQmdOVkhSTUJBZjhFQlRBREFRSC9NQVVHQXl0bGNBTkJBT0RQWUl4aVRoZ3RhY3l2Clg0SlhINGNDWGxWU3g4NUk4MjA0YkN2Zy8xeVorL3VNWXFVNGtDUW14ejZxM0h0eDh5aEV2YlpQa1V0QkI5b3cKWkVyaE93TT0KLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQo=\",")
	log.Printf("        \"RetryRPCPublicIPAddr\": \"0.0.0.0\",")
	log.Printf("        \"RootDirInodeNumber\": 1,")
	log.Printf("        \"RetryRPCPort\": 32356,")
	log.Printf("        \"MountID\": \"W3g9ItrLBkdYCoSlKQjSnA==\"")
	log.Printf("    }")
	log.Printf("}")
	log.Printf("")
	log.Printf("$ pfs-jrpc pfs-jrpc.conf r Server.RpcPing '\"Message\":\"Hi\"'")
	log.Printf("")
	log.Printf("jrpcRequestBuf:")
	log.Printf("{")
	log.Printf("    \"jsonrpc\": \"2.0\",")
	log.Printf("    \"method\": \"Server.RpcPing\",")
	log.Printf("    \"id\": 2,")
	log.Printf("    \"params\": [")
	log.Printf("        {")
	log.Printf("            \"MountID\": \"W3g9ItrLBkdYCoSlKQjSnA==\",")
	log.Printf("            \"Message\": \"Hi\"")
	log.Printf("        }")
	log.Printf("    ]")
	log.Printf("}")
	log.Printf("httpResponseBody:")
	log.Printf("{")
	log.Printf("    \"error\": null,")
	log.Printf("    \"id\": 2,")
	log.Printf("    \"result\": {")
	log.Printf("        \"Message\": \"pong 2 bytes\"")
	log.Printf("    }")
	log.Printf("}")
	log.Printf("")
	log.Printf("$ pfs-jrpc pfs-jrpc.conf c")
	log.Printf("")
}

func doAuth() {
	var (
		err          error
		httpClient   *http.Client
		httpRequest  *http.Request
		httpResponse *http.Response
	)

	httpRequest, err = http.NewRequest("GET", authURL, nil)
	if nil != err {
		log.Fatalf("Failed to create GET of authURL==\"%s\": %v", authURL, err)
	}

	httpRequest.Header["X-Auth-User"] = []string{authUser}
	httpRequest.Header["X-Auth-Key"] = []string{authKey}

	if verbose {
		fmt.Printf("Auth Request:\n")
		fmt.Println("  httpRequest.URL:   ", httpRequest.URL)
		fmt.Println("  httpRequest.Header:", httpRequest.Header)
	}

	httpClient = &http.Client{}

	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		log.Fatalf("Failed to issue GET of authURL==\"%s\": %v", authURL, err)
	}
	if http.StatusOK != httpResponse.StatusCode {
		log.Fatalf("Received unexpected HTTP Status for Get of authURL==\"%s\": %s", authURL, httpResponse.Status)
	}

	env = &envStruct{
		AuthToken:     httpResponse.Header.Get("X-Auth-Token"),
		StorageURL:    httpResponse.Header.Get("X-Storage-Url"),
		LastMessageID: 0,
		MountID:       "",
	}

	if verbose {
		fmt.Printf("Auth Response:\n")
		fmt.Printf("  env.AuthToken:  %s\n", env.AuthToken)
		fmt.Printf("  env.StorageURL: %s\n", env.StorageURL)
	}

	if strings.HasPrefix(env.StorageURL, "http:") && strings.HasPrefix(httpRequest.URL.String(), "https:") {
		env.StorageURL = strings.Replace(env.StorageURL, "http:", "https:", 1)

		if verbose {
			fmt.Printf("Auth Response (proxy-corrected):\n")
			fmt.Printf("  env.AuthToken:  %s\n", env.AuthToken)
			fmt.Printf("  env.StorageURL: %s\n", env.StorageURL)
		}
	}

	writeEnv()
}

func doMount() {
	var (
		accountName                 string
		err                         error
		httpClient                  *http.Client
		httpRequest                 *http.Request
		httpResponse                *http.Response
		httpResponseBody            []byte
		httpResponseBodyBytesBuffer bytes.Buffer
		jrpcRequest                 *jrpcRequestStruct
		jrpcRequestBuf              []byte
		jrpcRequestBytesBuffer      bytes.Buffer
		jrpcResponseIDAndError      *jrpcResponseIDAndErrorStruct
		jrpcResponseNoError         *jrpcResponseNoErrorStruct
		mountReply                  *jrpcfs.MountByAccountNameReply
		mountRequest                *jrpcfs.MountByAccountNameRequest
		storageURLSplit             []string
		thisMessageID               uint64
	)

	readEnv()

	thisMessageID = env.LastMessageID + 1
	env.LastMessageID = thisMessageID

	writeEnv()

	storageURLSplit = strings.Split(env.StorageURL, "/")
	if 0 == len(storageURLSplit) {
		log.Fatalf("Attempt to compute accountName from strings.Split(env.StorageURL, \"/\") failed")
	}

	accountName = storageURLSplit[len(storageURLSplit)-1]

	mountRequest = &jrpcfs.MountByAccountNameRequest{
		AccountName:  accountName,
		MountOptions: 0,
		AuthUserID:   0,
		AuthGroupID:  0,
	}

	jrpcRequest = &jrpcRequestStruct{
		JSONrpc: "2.0",
		Method:  "Server.RpcMountByAccountName",
		ID:      thisMessageID,
		Params:  [1]interface{}{mountRequest},
	}

	jrpcRequestBuf, err = json.Marshal(jrpcRequest)
	if nil != err {
		log.Fatalf("Attempt to marshal jrpcRequest(mount) failed: %v", err)
	}

	if verbose {
		_ = json.Indent(&jrpcRequestBytesBuffer, jrpcRequestBuf, "", "    ")
		fmt.Printf("jrpcRequestBuf:\n%s\n", jrpcRequestBytesBuffer.Bytes())
	}

	httpRequest, err = http.NewRequest("PROXYFS", env.StorageURL, bytes.NewReader(jrpcRequestBuf))
	if nil != err {
		log.Fatalf("Failed to create httpRequest for mount: %v", err)
	}

	httpRequest.Header["X-Auth-Token"] = []string{env.AuthToken}
	httpRequest.Header["Content-Type"] = []string{"application/json"}

	httpClient = &http.Client{}

	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		log.Fatalf("Failed to issue PROXYFS Mount: %v", err)
	}
	if http.StatusOK != httpResponse.StatusCode {
		log.Fatalf("Received unexpected HTTP Status for PROXYFS Mount: %s", httpResponse.Status)
	}

	httpResponseBody, err = ioutil.ReadAll(httpResponse.Body)
	if nil != err {
		log.Fatalf("Failed to read httpResponse.Body: %v", err)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		log.Fatalf("Failed to close httpResponse.Body: %v", err)
	}

	if verbose {
		_ = json.Indent(&httpResponseBodyBytesBuffer, httpResponseBody, "", "    ")
		fmt.Printf("httpResponseBody:\n%s\n", httpResponseBodyBytesBuffer.Bytes())
	}

	jrpcResponseIDAndError = &jrpcResponseIDAndErrorStruct{}

	err = json.Unmarshal(httpResponseBody, jrpcResponseIDAndError)
	if nil != err {
		log.Fatalf("Failed to json.Unmarshal(httpResponseBody) [Case 1]: %v", err)
	}
	if thisMessageID != jrpcResponseIDAndError.ID {
		log.Fatalf("Got unexpected MessageID in httpResponseBody [Case 1]")
	}
	if "" != jrpcResponseIDAndError.Error {
		log.Fatalf("Got JRPC Failure on PROXYFS Mount: %s", jrpcResponseIDAndError.Error)
	}

	mountReply = &jrpcfs.MountByAccountNameReply{}
	jrpcResponseNoError = &jrpcResponseNoErrorStruct{Result: mountReply}

	err = json.Unmarshal(httpResponseBody, jrpcResponseNoError)
	if nil != err {
		log.Fatalf("Failed to json.Unmarshal(httpResponseBody) [Case 2]: %v", err)
	}
	if thisMessageID != jrpcResponseIDAndError.ID {
		log.Fatalf("Got unexpected MessageID in httpResponseBody [Case 2]")
	}

	env.MountID = mountReply.MountID

	writeEnv()
}

func doJRPC(s ...string) {
	var (
		arbitraryRequestMethod      string
		arbitraryRequestParam       string
		err                         error
		httpClient                  *http.Client
		httpRequest                 *http.Request
		httpResponse                *http.Response
		httpResponseBody            []byte
		httpResponseBodyBytesBuffer bytes.Buffer
		jrpcRequest                 *jrpcRequestStruct
		jrpcRequestBuf              []byte
		jrpcRequestBytesBuffer      bytes.Buffer
		jrpcResponseIDAndError      *jrpcResponseIDAndErrorStruct
		thisMessageID               uint64
	)

	readEnv()

	if "" == env.MountID {
		log.Fatalf("Attempt to issue JSON RPC to unmounted volume")
	}

	thisMessageID = env.LastMessageID + 1
	env.LastMessageID = thisMessageID

	writeEnv()

	arbitraryRequestMethod = s[0]

	switch len(s) {
	case 1:
		arbitraryRequestParam = "\"MountID\":\"" + string(env.MountID) + "\""
	case 2:
		arbitraryRequestParam = "\"MountID\":\"" + string(env.MountID) + "\"," + s[1]
	default:
		log.Fatalf("JSON RPC must be either [Method] or [Method Param]... not %v", s)
	}

	jrpcRequest = &jrpcRequestStruct{
		JSONrpc: "2.0",
		Method:  arbitraryRequestMethod,
		ID:      thisMessageID,
		Params:  [1]interface{}{&jrpcRequestEmptyParamStruct{}},
	}

	jrpcRequestBuf, err = json.Marshal(jrpcRequest)
	if nil != err {
		log.Fatalf("Attempt to marshal jrpcRequest failed: %v", err)
	}

	jrpcRequestBuf = append(jrpcRequestBuf[:len(jrpcRequestBuf)-3], arbitraryRequestParam...)
	jrpcRequestBuf = append(jrpcRequestBuf, "}]}"...)

	if verbose {
		_ = json.Indent(&jrpcRequestBytesBuffer, jrpcRequestBuf, "", "    ")
		fmt.Printf("jrpcRequestBuf:\n%s\n", jrpcRequestBytesBuffer.Bytes())
	}

	httpRequest, err = http.NewRequest("PROXYFS", env.StorageURL, bytes.NewReader(jrpcRequestBuf))
	if nil != err {
		log.Fatalf("Failed to create httpRequest: %v", err)
	}

	httpRequest.Header["X-Auth-Token"] = []string{env.AuthToken}
	httpRequest.Header["Content-Type"] = []string{"application/json"}

	httpClient = &http.Client{}

	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		log.Fatalf("Failed to issue PROXYFS: %v", err)
	}
	if http.StatusOK != httpResponse.StatusCode {
		log.Fatalf("Received unexpected HTTP Status: %s", httpResponse.Status)
	}

	httpResponseBody, err = ioutil.ReadAll(httpResponse.Body)
	if nil != err {
		log.Fatalf("Failed to read httpResponse.Body: %v", err)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		log.Fatalf("Failed to close httpResponse.Body: %v", err)
	}

	if verbose {
		_ = json.Indent(&httpResponseBodyBytesBuffer, httpResponseBody, "", "    ")
		fmt.Printf("httpResponseBody:\n%s\n", httpResponseBodyBytesBuffer.Bytes())
	}

	jrpcResponseIDAndError = &jrpcResponseIDAndErrorStruct{}

	err = json.Unmarshal(httpResponseBody, jrpcResponseIDAndError)
	if nil != err {
		log.Fatalf("Failed to json.Unmarshal(httpResponseBody) [Case 2]: %v", err)
	}
	if thisMessageID != jrpcResponseIDAndError.ID {
		log.Fatalf("Got unexpected MessageID in httpResponseBody [Case 2]")
	}
	if "" != jrpcResponseIDAndError.Error {
		log.Fatalf("Got JRPC Failure: %s", jrpcResponseIDAndError.Error)
	}
}

func doClean() {
	_ = os.RemoveAll(envPath)
}

func writeEnv() {
	var (
		envBuf []byte
		err    error
	)

	envBuf, err = json.Marshal(env)
	if nil != err {
		log.Fatalf("Failed to json.Marshal(env): %v", err)
	}

	err = ioutil.WriteFile(envPath, envBuf, 0644)
	if nil != err {
		log.Fatalf("Failed to persist ENV to envPath==\"%s\": %v", envPath, err)
	}
}

func readEnv() {
	var (
		envBuf []byte
		err    error
	)

	env = &envStruct{}
	envBuf, err = ioutil.ReadFile(envPath)
	if nil != err {
		log.Fatalf("Failed to recover ENV from envPath==\"%s\": %v", envPath, err)
	}

	err = json.Unmarshal(envBuf, env)
	if nil != err {
		log.Fatalf("Failed to json.Unmarshal(envBuf): %v", err)
	}
}
