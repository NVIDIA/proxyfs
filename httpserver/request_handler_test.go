package httpserver

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/swiftstack/ProxyFS/conf"
)

func testSetup() (err error) {
	testConfMapStrings := []string{
		"Stats.IPAddr=localhost",
		"Stats.UDPPort=52184",
		"Stats.BufferLength=100",
	}

	testConfMap, err := conf.MakeConfMapFromStrings(testConfMapStrings)
	if nil != err {
		return
	}

	globals.confMap = testConfMap

	return nil
}

func testTeardown() (err error) {
	return
}

func TestMain(m *testing.M) {
	err := testSetup()
	if nil != err {
		fmt.Fprintf(os.Stderr, "test setup failed: %v\n", err)
		os.Exit(1)
	}

	testResults := m.Run()

	err = testTeardown()
	if nil != err {
		fmt.Fprintf(os.Stderr, "test teardown failed: %v\n", err)
		os.Exit(1)
	}

	os.Exit(testResults)
}

func TestConfigExpansion(t *testing.T) {

	testConfigExpansion := func(url string, shouldBeExpanded bool) {
		req := httptest.NewRequest("GET", "http://pfs.com"+url, nil)
		w := httptest.NewRecorder()

		doGet(w, req)
		resp := w.Result()
		body, _ := ioutil.ReadAll(resp.Body)

		if resp.StatusCode != 200 {
			t.Errorf("[%s]: Config response was %d; expected 200", url, resp.StatusCode)
			return
		}

		crs := bytes.Count(body, []byte("\n"))
		if shouldBeExpanded {
			if crs <= 2 {
				t.Errorf("[%s]: Only found %d <CR>s, but config should be expanded.", url, crs)
			}
		} else {
			if crs > 2 {
				t.Errorf("[%s]: Found %d <CR>s, but config should be compact.", url, crs)
			}
		}
	}

	// NOTE:  These are really routing tests.  If and when we isolate the routing code, we can
	// move tests like this there.  If and when we use an off-the-shelf router, we can delete
	// these altogether and just call the internal function for a few canonical urls.
	expandedUrls := []string{
		"/config",
		"/config/?",
		"/config/?foo=bar",
		"/config/?compact=false",
	}

	compactUrls := []string{
		"/config?compact=true",
		"/config?compact=1",
		"/config/?foo=bar&compact=1&baz=quux",
	}

	for _, url := range expandedUrls {
		testConfigExpansion(url, true)
	}

	for _, url := range compactUrls {
		testConfigExpansion(url, false)
	}

	return
}
