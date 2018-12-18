package ramswift

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/ProxyFS/utils"
)

func TestViaNoAuthClient(t *testing.T) {
	const (
		noAuthTCPPort = "9999"
	)
	var (
		confStrings = []string{
			"SwiftClient.NoAuthTCPPort=" + noAuthTCPPort,
			"SwiftClient.NoAuthIPAddr=127.0.0.1",
			"Cluster.WhoAmI=Peer0",
			"FSGlobals.VolumeGroupList=",
			"Peer:Peer0.ReadCacheQuotaFraction=0.20",
			"RamSwiftInfo.MaxAccountNameLength=256",
			"RamSwiftInfo.MaxContainerNameLength=256",
			"RamSwiftInfo.MaxObjectNameLength=1024",
			"RamSwiftInfo.AccountListingLimit=10000",
			"RamSwiftInfo.ContainerListingLimit=10000",
		}
		contentType                  string
		contentTypeMultiPartBoundary string
		doneChan                     chan bool
		err                          error
		errChan                      chan error
		expectedBuf                  []byte
		expectedInfo                 string
		httpClient                   *http.Client
		httpRequest                  *http.Request
		httpResponse                 *http.Response
		mouseHeaderPresent           bool
		pipeReader                   *io.PipeReader
		pipeWriter                   *io.PipeWriter
		readBuf                      []byte
		signalHandlerIsArmedWG       sync.WaitGroup
		urlForInfo                   string
		urlPrefix                    string
	)

	signalHandlerIsArmedWG.Add(1)
	doneChan = make(chan bool, 1) // Must be buffered to avoid race

	go Daemon("/dev/null", confStrings, &signalHandlerIsArmedWG, doneChan, unix.SIGTERM)

	signalHandlerIsArmedWG.Wait()

	// Setup urlPrefix to be "http://127.0.0.1:<SwiftClient.NoAuthTCPPort>/v1/"

	urlForInfo = "http://127.0.0.1:" + noAuthTCPPort + "/info"
	urlPrefix = "http://127.0.0.1:" + noAuthTCPPort + "/v1/"

	// Setup http.Client that we will use for all HTTP requests

	httpClient = &http.Client{}

	// Send a GET for "/info" expecting [RamSwiftInfo] data in compact JSON form

	httpRequest, err = http.NewRequest("GET", urlForInfo, nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusOK != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	expectedInfo = "{\"swift\": {\"max_account_name_length\": 256,\"max_container_name_length\": 256,\"max_object_name_length\": 1024,\"account_listing_limit\": 10000,\"container_listing_limit\": 10000}}"
	if int64(len(expectedInfo)) != httpResponse.ContentLength {
		t.Fatalf("GET of /info httpResponse.ContentLength unexpected")
	}
	readBuf, err = ioutil.ReadAll(httpResponse.Body)
	if nil != err {
		t.Fatalf("ioutil.ReadAll() returned unexpected error: %v", err)
	}
	if expectedInfo != utils.ByteSliceToString(readBuf) {
		t.Fatalf("GET of /info httpResponse.Body contents unexpected")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a PUT for account "TestAccount" and header Cat: Dog

	httpRequest, err = http.NewRequest("PUT", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Cat", "Dog")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusCreated != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for account "TestAccount" expecting header Cat: Dog

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a GET for account "TestAccount" expecting Content-Length: 0 and header Cat: Dog

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	if 0 != httpResponse.ContentLength {
		t.Fatalf("TestAccount should contain no elements at this point")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a POST for account "TestAccount" and header Mouse: Bird

	httpRequest, err = http.NewRequest("POST", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Mouse", "Bird")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for account "TestAccount" expecting header Cat: Dog & Mouse: Bird

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	if httpResponse.Header.Get("Mouse") != "Bird" {
		t.Fatalf("TestAccount should have header Mouse: Bird")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a POST for account "TestAccount" deleting header Mouse

	httpRequest, err = http.NewRequest("POST", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Mouse", "")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for account "TestAccount" expecting header Cat: Dog & no Mouse header

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	_, mouseHeaderPresent = httpResponse.Header["Mouse"]
	if mouseHeaderPresent {
		t.Fatalf("TestAccount should not have header Mouse")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a PUT for account "TestAccount" and header Mouse: Bird

	httpRequest, err = http.NewRequest("PUT", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Mouse", "Bird")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusAccepted != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for account "TestAccount" expecting header Cat: Dog & Mouse: Bird

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	if httpResponse.Header.Get("Mouse") != "Bird" {
		t.Fatalf("TestAccount should have header Mouse: Bird")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a PUT for account "TestAccount" deleting header Mouse

	httpRequest, err = http.NewRequest("PUT", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Mouse", "")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusAccepted != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for account "TestAccount" expecting header Cat: Dog & no Mouse header

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	_, mouseHeaderPresent = httpResponse.Header["Mouse"]
	if mouseHeaderPresent {
		t.Fatalf("TestAccount should not have header Mouse")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a PUT for container "TestContainer" and header Cat: Dog

	httpRequest, err = http.NewRequest("PUT", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Cat", "Dog")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusCreated != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a GET for account "TestAccount" expecting "TestContainer\n" and header Cat: Dog

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusOK != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	if int64(len("TestContainer\n")) != httpResponse.ContentLength {
		t.Fatalf("TestAccount should contain only \"TestContainer\\n\" at this point")
	}
	readBuf, err = ioutil.ReadAll(httpResponse.Body)
	if nil != err {
		t.Fatalf("ioutil.ReadAll() returned unexpected error: %v", err)
	}
	if "TestContainer\n" != utils.ByteSliceToString(readBuf) {
		t.Fatalf("TestAccount should contain only \"TestContainer\\n\" at this point")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a GET for account "TestAccount" with marker "AAA" expecting "TestContainer\n" and header Cat: Dog

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount?marker=AAA", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusOK != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	if int64(len("TestContainer\n")) != httpResponse.ContentLength {
		t.Fatalf("TestAccount should contain only \"TestContainer\\n\" at this point")
	}
	readBuf, err = ioutil.ReadAll(httpResponse.Body)
	if nil != err {
		t.Fatalf("ioutil.ReadAll() returned unexpected error: %v", err)
	}
	if "TestContainer\n" != utils.ByteSliceToString(readBuf) {
		t.Fatalf("TestAccount should contain only \"TestContainer\\n\" at this point")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a GET for account "TestAccount" with marker "ZZZ" expecting Content-Length: 0 and header Cat: Dog

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount?marker=ZZZ", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	if 0 != httpResponse.ContentLength {
		t.Fatalf("TestAccount should contain no elements at this point")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for container "TestContainer" expecting header Cat: Dog

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestContainer should have header Cat: Dog")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a GET for container "TestContainer" expecting Content-Length: 0 and header Cat: Dog

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestContainer should have header Cat: Dog")
	}
	if 0 != httpResponse.ContentLength {
		t.Fatalf("TestContainer should contain no elements at this point")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a POST for container "TestContainer" and header Mouse: Bird

	httpRequest, err = http.NewRequest("POST", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Mouse", "Bird")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for container "TestContainer" expecting header Cat: Dog & Mouse: Bird

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestContainer should have header Cat: Dog")
	}
	if httpResponse.Header.Get("Mouse") != "Bird" {
		t.Fatalf("TestContainer should have header Mouse: Bird")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a POST for container "TestContainer" deleting header Mouse

	httpRequest, err = http.NewRequest("POST", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Mouse", "")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for container "TestContainer" expecting header Cat: Dog & no Mouse header

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestContainer should have header Cat: Dog")
	}
	_, mouseHeaderPresent = httpResponse.Header["Mouse"]
	if mouseHeaderPresent {
		t.Fatalf("TestContainer should not have header Mouse")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a PUT for container "TestContainer" and header Mouse: Bird

	httpRequest, err = http.NewRequest("PUT", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Mouse", "Bird")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusAccepted != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for container "TestContainer" expecting header Cat: Dog & Mouse: Bird

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestContainer should have header Cat: Dog")
	}
	if httpResponse.Header.Get("Mouse") != "Bird" {
		t.Fatalf("TestContainer should have header Mouse: Bird")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a PUT for container "TestContainer" deleting header Mouse

	httpRequest, err = http.NewRequest("PUT", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Mouse", "")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusAccepted != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for container "TestContainer" expecting header Cat: Dog & no Mouse header

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestContainer should have header Cat: Dog")
	}
	_, mouseHeaderPresent = httpResponse.Header["Mouse"]
	if mouseHeaderPresent {
		t.Fatalf("TestContainer should not have header Mouse")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a non-chunked PUT for object "Foo" to contain []byte{0x00, 0x01, 0x02}

	httpRequest, err = http.NewRequest("PUT", urlPrefix+"TestAccount/TestContainer/Foo", bytes.NewReader([]byte{0x00, 0x01, 0x02}))
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusCreated != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a chunked PUT for object "Bar"" with 1st chunk being []byte{0xAA, 0xBB} & 2nd chunk being []byte{0xCC, 0xDD, 0xEE}

	pipeReader, pipeWriter = io.Pipe()
	httpRequest, err = http.NewRequest("PUT", urlPrefix+"TestAccount/TestContainer/Bar", pipeReader)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.ContentLength = -1
	httpRequest.Header.Del("Content-Length")
	errChan = make(chan error, 1)
	go func() {
		var (
			nonShadowingErr          error
			nonShadowingHTTPResponse *http.Response
		)
		nonShadowingHTTPResponse, nonShadowingErr = httpClient.Do(httpRequest)
		if nil == nonShadowingErr {
			httpResponse = nonShadowingHTTPResponse
		}
		errChan <- nonShadowingErr
	}()
	_, err = pipeWriter.Write([]byte{0xAA, 0xBB})
	if nil != err {
		t.Fatalf("pipeWriter.Write() returned unexpected error: %v", err)
	}
	_, err = pipeWriter.Write([]byte{0xCC, 0xDD, 0xEE})
	if nil != err {
		t.Fatalf("pipeWriter.Write() returned unexpected error: %v", err)
	}
	err = pipeWriter.Close()
	if nil != err {
		t.Fatalf("pipeWriter.Close() returned unexpected error: %v", err)
	}
	err = <-errChan
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusCreated != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a GET for container "TestContainer" expecting "Bar\nFoo\n" and header Cat: Dog

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusOK != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	if int64(len("Bar\nFoo\n")) != httpResponse.ContentLength {
		t.Fatalf("TestContainer should contain only \"Bar\\nFoo\\n\" at this point")
	}
	readBuf, err = ioutil.ReadAll(httpResponse.Body)
	if nil != err {
		t.Fatalf("ioutil.ReadAll() returned unexpected error: %v", err)
	}
	if "Bar\nFoo\n" != utils.ByteSliceToString(readBuf) {
		t.Fatalf("TestContainer should contain only \"Bar\\nFoo\\n\" at this point")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a GET for container "TestContainer" with marker "AAA" expecting "Bar\nFoo\n" and header Cat: Dog

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount/TestContainer?marker=AAA", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusOK != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	if int64(len("Bar\nFoo\n")) != httpResponse.ContentLength {
		t.Fatalf("TestContainer should contain only \"Bar\\nFoo\\n\" at this point")
	}
	readBuf, err = ioutil.ReadAll(httpResponse.Body)
	if nil != err {
		t.Fatalf("ioutil.ReadAll() returned unexpected error: %v", err)
	}
	if "Bar\nFoo\n" != utils.ByteSliceToString(readBuf) {
		t.Fatalf("TestContainer should contain only \"Bar\\nFoo\\n\" at this point")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a GET for container "TestContainer" with marker "ZZZ" expecting Content-Length: 0 and header Cat: Dog

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount/TestContainer?marker=ZZZ", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestContainer should have header Cat: Dog")
	}
	if 0 != httpResponse.ContentLength {
		t.Fatalf("TestContainer should contain no elements at this point")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for object "Foo" expecting Content-Length: 3

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount/TestContainer/Foo", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusOK != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if 3 != httpResponse.ContentLength {
		t.Fatalf("httpResponse.ContentLength contained unexpected value: %v", httpResponse.ContentLength)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a full object GET for object "Foo" expecting []byte{0x00, 0x01, 0x02}

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount/TestContainer/Foo", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusOK != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if int64(len([]byte{0x00, 0x01, 0x02})) != httpResponse.ContentLength {
		t.Fatalf("Foo should contain precisely []byte{0x00, 0x01, 0x02}")
	}
	readBuf, err = ioutil.ReadAll(httpResponse.Body)
	if nil != err {
		t.Fatalf("ioutil.ReadAll() returned unexpected error: %v", err)
	}
	if 0 != bytes.Compare([]byte{0x00, 0x01, 0x02}, readBuf) {
		t.Fatalf("Foo should contain precisely []byte{0x00, 0x01, 0x02}")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a range GET of bytes at offset 1 for length 3 for object "Bar" expecting []byte{0xBB, 0xCC, 0xDD}

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount/TestContainer/Bar", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Range", "bytes=1-3")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusPartialContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if int64(len([]byte{0xBB, 0xCC, 0xDD})) != httpResponse.ContentLength {
		t.Fatalf("Bar's bytes 1-3 should contain precisely []byte{0xBB, 0xCC, 0xDD}")
	}
	readBuf, err = ioutil.ReadAll(httpResponse.Body)
	if nil != err {
		t.Fatalf("ioutil.ReadAll() returned unexpected error: %v", err)
	}
	if 0 != bytes.Compare([]byte{0xBB, 0xCC, 0xDD}, readBuf) {
		t.Fatalf("Bar's bytes 1-3 should contain precisely []byte{0xBB, 0xCC, 0xDD}")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a range GET of bytes at offset 0 for length 2
	//                          and offset 3 for length of 1 for object "Bar"
	// expecting two MIME parts: []byte{0xAA, 0xBB} and  []byte{0xDD}

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount/TestContainer/Bar", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Range", "bytes=0-1,3-3")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusPartialContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	contentType = httpResponse.Header.Get("Content-Type")
	contentTypeMultiPartBoundary = strings.TrimPrefix(contentType, "multipart/byteranges; boundary=")
	if (len(contentType) == len(contentTypeMultiPartBoundary)) || (0 == len(contentTypeMultiPartBoundary)) {
		t.Fatalf("httpReponse.Header[\"Content-Type\"] contained unexpected value: \"%v\"", contentType)
	}
	expectedBuf = make([]byte, 0, httpResponse.ContentLength)
	expectedBuf = append(expectedBuf, []byte("--"+contentTypeMultiPartBoundary+"\r\n")...)
	expectedBuf = append(expectedBuf, []byte("Content-Type: application/octet-stream\r\n")...)
	expectedBuf = append(expectedBuf, []byte("Content-Range: bytes 0-1/5\r\n")...)
	expectedBuf = append(expectedBuf, []byte("\r\n")...)
	expectedBuf = append(expectedBuf, []byte{0xAA, 0xBB}...)
	expectedBuf = append(expectedBuf, []byte("\r\n")...)
	expectedBuf = append(expectedBuf, []byte("--"+contentTypeMultiPartBoundary+"\r\n")...)
	expectedBuf = append(expectedBuf, []byte("Content-Type: application/octet-stream\r\n")...)
	expectedBuf = append(expectedBuf, []byte("Content-Range: bytes 3-3/5\r\n")...)
	expectedBuf = append(expectedBuf, []byte("\r\n")...)
	expectedBuf = append(expectedBuf, []byte{0xDD}...)
	expectedBuf = append(expectedBuf, []byte("\r\n")...)
	expectedBuf = append(expectedBuf, []byte("--"+contentTypeMultiPartBoundary+"--")...)
	if int64(len(expectedBuf)) != httpResponse.ContentLength {
		t.Fatalf("Unexpected multi-part GET response Content-Length")
	}
	readBuf, err = ioutil.ReadAll(httpResponse.Body)
	if nil != err {
		t.Fatalf("ioutil.ReadAll() returned unexpected error: %v", err)
	}
	if 0 != bytes.Compare(expectedBuf, readBuf) {
		t.Fatalf("Unexpected payload of multi-part GET response")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a tail GET of the last 2 bytes for object "Bar" expecting []byte{0xDD, 0xEE}

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount/TestContainer/Bar", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Range", "bytes=-2")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusPartialContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if int64(len([]byte{0xDD, 0xEE})) != httpResponse.ContentLength {
		t.Fatalf("Bar's last 2 bytes should contain precisely []byte{0xDD, 0xEE}")
	}
	readBuf, err = ioutil.ReadAll(httpResponse.Body)
	if nil != err {
		t.Fatalf("ioutil.ReadAll() returned unexpected error: %v", err)
	}
	if 0 != bytes.Compare([]byte{0xDD, 0xEE}, readBuf) {
		t.Fatalf("Bar's last 2 bytes should contain precisely []byte{0xDD, 0xEE}")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a DELETE for object "Foo"

	httpRequest, err = http.NewRequest("DELETE", urlPrefix+"TestAccount/TestContainer/Foo", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a DELETE for object "Bar"

	httpRequest, err = http.NewRequest("DELETE", urlPrefix+"TestAccount/TestContainer/Bar", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a GET for container "TestContainer" expecting Content-Length: 0 and header Cat: Dog

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestContainer should have header Cat: Dog")
	}
	if 0 != httpResponse.ContentLength {
		t.Fatalf("TestContainer should contain no elements at this point")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a DELETE for container "TestContainer"

	httpRequest, err = http.NewRequest("DELETE", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a GET for account "TestAccount" expecting Content-Length: 0 and header Cat: Dog

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	if 0 != httpResponse.ContentLength {
		t.Fatalf("TestAccount should contain no elements at this point")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a DELETE for account "TestAccount"

	httpRequest, err = http.NewRequest("DELETE", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send ourself a SIGTERM to signal normal termination of mainWithArgs()

	unix.Kill(unix.Getpid(), unix.SIGTERM)

	_ = <-doneChan
}
