package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/swiftstack/ProxyFS/version"
)

func serveHTTP() {
	var (
		ipAddrTCPPort string
	)

	ipAddrTCPPort = net.JoinHostPort(globals.config.HTTPServerIPAddr, strconv.Itoa(int(globals.config.HTTPServerTCPPort)))

	globals.httpServer = &http.Server{
		Addr:    ipAddrTCPPort,
		Handler: &globals,
	}

	globals.httpServerWG.Add(1)

	go func() {
		var (
			err error
		)

		err = globals.httpServer.ListenAndServe()
		if http.ErrServerClosed != err {
			log.Fatalf("httpServer.ListenAndServe() exited unexpectedly: %v", err)
		}

		globals.httpServerWG.Done()
	}()
}

func unserveHTTP() {
	var (
		err error
	)

	err = globals.httpServer.Shutdown(context.TODO())
	if nil != err {
		log.Fatalf("httpServer.Shutdown() returned with an error: %v", err)
	}

	globals.httpServerWG.Wait()
}

func (dummy *globalsStruct) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case http.MethodGet:
		serveGet(responseWriter, request)
	default:
		responseWriter.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func serveGet(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		path string
	)

	path = strings.TrimRight(request.URL.Path, "/")

	switch path {
	case "/version":
		serveGetOfVersion(responseWriter, request)
	default:
		responseWriter.WriteHeader(http.StatusNotFound)
	}
}

func serveGetOfVersion(responseWriter http.ResponseWriter, request *http.Request) {
	responseWriter.Header().Set("Content-Type", "text/plain")
	responseWriter.WriteHeader(http.StatusOK)
	_, _ = responseWriter.Write([]byte(version.ProxyFSVersion))
}
