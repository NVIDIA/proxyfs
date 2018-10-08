package httpserver

import (
	"bytes"
	"io/ioutil"
	"net/http/httptest"
	"testing"
)

func TestConfigExpansion(t *testing.T) {
	testSetup(t)
	defer testTeardown(t)

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
