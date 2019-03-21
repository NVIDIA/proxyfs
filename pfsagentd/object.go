package main

import (
	"fmt"
	"io"
	"net/http"
)

type rcS struct {
	open bool
	fp   uint64
}

// rcTest demonstrates both a Ranged GET and a Chunked PUT
//
// Setup:
//   Launch saio
//     cdpfs
//     saio/home/swift/bin/resetswift
//     start_proxyfs_and_swift
//     mkdir /CommonMountPoint/Container
//     cat > /CommonMountPoint/Container/ObjectGET < EOF
//     Hello
//     There
//     Ed
//     EOF
//   Modify main.go::main() to call rcTest() right before waiting on a Signal
//     cdpfs
//     cd pfsagentd
//     make install
//
// Run the result:
//   pfsagentd pfsagentsaio.conf Agent.LogToConsole=false
//   ^C to cleanly terminate it
//
// Results expected:
//   resBody: There (those 5 chars only) should be logged
//   sudo cat /CommonMountPoint/Container/ObjectPUT should display
//     Hi
//     Bye
//
func rcTest() {
	var (
		req     *http.Request
		err     error
		res     *http.Response
		resBody []byte
		ok      bool
		rc      *rcS
	)

	req, err = http.NewRequest(http.MethodGet, globals.swiftAccountURL+"/TestContainer/ObjectGET", nil)
	fmt.Printf("req: %+v\n", req)
	fmt.Printf("err: %v\n", err)
	req.Header.Add("Range", "bytes=6-10")
	req.Header.Add("X-Bypass-Proxyfs", "true")
	res, resBody, ok = doHTTPRequest(req, http.StatusOK, http.StatusPartialContent)
	fmt.Printf("res: %+v\n", res)
	fmt.Printf("resBody: %v\n", string(resBody[:]))
	fmt.Printf("ok: %v\n", ok)

	rc = &rcS{open: false, fp: 0}

	req, err = http.NewRequest(http.MethodPut, globals.swiftAccountURL+"/TestContainer/ObjectPUT", rc)
	fmt.Printf("req: %+v\n", req)
	fmt.Printf("err: %v\n", err)
	req.Header.Add("X-Bypass-Proxyfs", "true")
	res, _, ok = doHTTPRequest(req, http.StatusOK, http.StatusCreated)
	fmt.Printf("res: %+v\n", res)
	fmt.Printf("ok: %v\n", ok)
}

func (rc *rcS) Read(p []byte) (n int, err error) {
	fmt.Printf("rCS.Read() called with len(p) == %d\n", len(p))
	defer func() { fmt.Printf("rCS.Read() returning n == %d err == %v\n", n, err) }()
	if !rc.open {
		rc.open = true
		rc.fp = 0
	}
	if 0 == rc.fp {
		p[0] = 'H'
		p[1] = 'i'
		p[2] = '\n'
		n = 3
		rc.fp = 3
		err = nil
	} else if 3 == rc.fp {
		p[0] = 'B'
		p[1] = 'y'
		p[2] = 'e'
		p[3] = '\n'
		n = 4
		rc.fp = 7
		err = io.EOF
	} else {
		n = 0
		err = io.EOF
	}
	return
}

func (rc *rcS) Close() (err error) {
	rc.open = false
	rc.fp = 0
	return
}
