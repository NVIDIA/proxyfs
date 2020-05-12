package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"

	"github.com/swiftstack/ProxyFS/version"
	"golang.org/x/sys/unix"
)

type authInStruct struct {
	AuthURL  string
	AuthUser string
	AuthKey  string
	Account  string
}

type authOutStruct struct {
	AuthToken  string
	StorageURL string
}

func main() {
	var (
		authIn         authInStruct
		err            error
		plugInEnvName  string
		plugInEnvValue string
		signalReceived os.Signal
		signalChan     chan os.Signal
	)

	switch len(os.Args) {
	case 0:
		fmt.Fprintf(os.Stderr, "Logic error... len(os.Args) cannot be zero\n")
		os.Exit(1)
	case 1:
		fmt.Fprintf(os.Stderr, "Missing PlugInEnvName\n")
		os.Exit(1)
	case 2:
		plugInEnvName = os.Args[1]
	default:
		fmt.Fprintf(os.Stderr, "Superfluous arguments (beyond PlugInEnvName) supplied: %v\n", os.Args[2:])
		os.Exit(1)
	}

	plugInEnvValue = os.Getenv(plugInEnvName)

	err = json.Unmarshal([]byte(plugInEnvValue), &authIn)
	if nil != err {
		fmt.Fprintf(os.Stderr, "json.Unmarshal(\"%s\",) failed: %v\n", plugInEnvValue, err)
		os.Exit(1)
	}

	performAuth(&authIn)

	signalChan = make(chan os.Signal, 1)
	signal.Notify(signalChan, unix.SIGHUP, unix.SIGINT, unix.SIGTERM)

	for {
		signalReceived = <-signalChan

		switch signalReceived {
		case unix.SIGHUP:
			performAuth(&authIn)
		case unix.SIGINT:
			os.Exit(0)
		case unix.SIGTERM:
			os.Exit(0)
		}
	}
}

func performAuth(authIn *authInStruct) {
	var (
		authOut         *authOutStruct
		authOutJSON     []byte
		authRequest     *http.Request
		authResponse    *http.Response
		err             error
		storageURLSplit []string
	)

	authRequest, err = http.NewRequest("GET", authIn.AuthURL, nil)
	if nil != err {
		fmt.Fprintf(os.Stderr, "http.NewRequest(\"GET\", \"%s\", nil) failed: %v\n", authIn.AuthURL, err)
		os.Exit(1)
	}

	authRequest.Header.Add("X-Auth-User", authIn.AuthUser)
	authRequest.Header.Add("X-Auth-Key", authIn.AuthKey)

	authRequest.Header.Add("User-Agent", "PFSAgent-Auth "+version.ProxyFSVersion)

	authResponse, err = http.DefaultClient.Do(authRequest)
	if nil != err {
		fmt.Fprintf(os.Stderr, "http.DefaultClient.Do(authRequest) failed: %v\n", err)
		os.Exit(1)
	}

	if http.StatusOK != authResponse.StatusCode {
		fmt.Fprintf(os.Stderr, "authResponse.Status unexpecte: %v\n", authResponse.Status)
		os.Exit(1)
	}

	authOut = &authOutStruct{
		AuthToken:  authResponse.Header.Get("X-Auth-Token"),
		StorageURL: authResponse.Header.Get("X-Storage-Url"),
	}

	if strings.HasPrefix(authIn.AuthURL, "https://") && strings.HasPrefix(authOut.StorageURL, "http://") {
		// We need to correct for the case where AuthURL starts with "https://""
		// but the Swift Proxy is behind a TLS terminating proxy. In this case,
		// Swift Proxy auth will actually see an AuthURL starting with "http://""
		// and respond with a StorageURL starting with "http://"".

		authOut.StorageURL = strings.Replace(authOut.StorageURL, "http://", "https://", 1)
	}

	storageURLSplit = strings.Split(authOut.StorageURL, "/")

	storageURLSplit[3] = "proxyfs"
	storageURLSplit[4] = authIn.Account

	authOut.StorageURL = strings.Join(storageURLSplit, "/")

	authOutJSON, err = json.Marshal(authOut)
	if nil != err {
		fmt.Fprintf(os.Stderr, "json.Marshal(authOut) failed: %v\n", err)
		os.Exit(1)
	}

	_, err = os.Stdout.Write(authOutJSON)
	if nil != err {
		fmt.Fprintf(os.Stderr, "os.Stdout.Write(authOutJSON) failed: %v\n", err)
		os.Exit(1)
	}
}
