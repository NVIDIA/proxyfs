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

// rcTest demonstrates a Chunked PUT
//
// Setup:
//   Launch saio
//     cdpfs
//     saio/home/swift/bin/resetswift
//     start_proxyfs_and_swift
//     mkdir /CommonMountPoint/Container
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
//   sudo cat /CommonMountPoint/Container/ObjectPUT should display
//     Hi
//     Bye
//
func rcTest() {
	var (
		req *http.Request
		err error
		res *http.Response
		ok  bool
		rc  *rcS
		sC  int
	)

	rc = &rcS{open: false, fp: 0}

	req, err = http.NewRequest(http.MethodPut, globals.swiftAccountURL+"/TestContainer/ObjectPUT", rc)
	fmt.Printf("req: %+v\n", req)
	fmt.Printf("err: %v\n", err)
	req.Header.Add("X-Bypass-Proxyfs", "true")
	res, _, ok, sC = doHTTPRequest(req, http.StatusOK, http.StatusCreated)
	fmt.Printf("res: %+v\n", res)
	fmt.Printf("ok: %v\n", ok)
	fmt.Printf("sC: %v\n", sC)
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
