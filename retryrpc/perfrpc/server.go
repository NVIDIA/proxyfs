// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/NVIDIA/proxyfs/retryrpc"
)

func configNewServer(ipAddr string, dnsName string, port int, lt time.Duration, tlsDir string, dontStartTrimmers bool, useTLS bool) (rrSvr *retryrpc.Server, err error) {
	var (
		config      *retryrpc.ServerConfig
		ipAddrOrDNS string
	)

	if dnsName != "" {
		ipAddrOrDNS = dnsName
	} else {
		ipAddrOrDNS = ipAddr
	}

	if useTLS {
		_ = os.Mkdir(tlsDir, os.ModePerm)

		// Create certificate/CA that lasts for 30 days
		tlsCert := tlsCertsAllocate(ipAddr, dnsName, 30*24*time.Hour, tlsDir)

		config = &retryrpc.ServerConfig{
			LongTrim:        lt,
			ShortTrim:       100 * time.Millisecond,
			DNSOrIPAddr:     ipAddrOrDNS,
			Port:            port,
			DeadlineIO:      60 * time.Second,
			KeepAlivePeriod: 60 * time.Second,
			TLSCertificate:  tlsCert.endpointTLSCert,
		}
	} else {
		config = &retryrpc.ServerConfig{
			LongTrim:        lt,
			ShortTrim:       100 * time.Millisecond,
			DNSOrIPAddr:     ipAddrOrDNS,
			Port:            port,
			DeadlineIO:      60 * time.Second,
			KeepAlivePeriod: 60 * time.Second,
			TLSCertificate:  tls.Certificate{},
		}
	}

	// Create a new RetryRPC Server.  Completed request will live on
	// completedRequests for 10 seconds.
	rrSvr = retryrpc.NewServer(config)

	return
}

// TODO - should this be a goroutine with WG?????
// want way to shutdown with curl/WS? how dump stats?
func becomeAServer(ipAddr string, dnsName string, port int, tlsDir string, useTLS bool) error {

	// Create new TestPingServer - needed for calling RPCs
	myJrpcfs := &PerfPingServer{}

	rrSvr, err := configNewServer(ipAddr, dnsName, port, 10*time.Minute, tlsDir, false, useTLS)
	if err != nil {
		return err
	}

	// Register the Server - sets up the methods supported by the
	// server
	err = rrSvr.Register(myJrpcfs)
	if err != nil {
		return err
	}

	// Start listening for requests on the ipaddr/port
	err = rrSvr.Start()
	if err != nil {
		return err
	}

	// Tell server to start accepting and processing requests
	rrSvr.Run()

	// Block indefinitely
	for {
		time.Sleep(1 * time.Hour)
	}

	/*
		// TODO - decide if we want a way to shutdown server
		// Stop the server before exiting
		rrSvr.Close()
		return nil
	*/
}

type ServerSubcommand struct {
	fs      *flag.FlagSet
	ipAddr  string // IP Address server will use
	dnsName string // DNS Name of server
	port    int    // Port on which to listen
	tlsDir  string //  Directory to read for TLS info
	dbgport string // Debug port for pprof webserver
}

func NewServerCommand() *ServerSubcommand {
	s := &ServerSubcommand{
		fs: flag.NewFlagSet("server", flag.ContinueOnError),
	}
	s.fs.StringVar(&s.ipAddr, "ipaddr", "", "IP Address server will use (optional)")
	s.fs.StringVar(&s.dnsName, "dnsname", "", "DNS Name server will use (optional)")
	s.fs.IntVar(&s.port, "port", 0, "Port on which to listen")
	s.fs.StringVar(&s.tlsDir, "tlsdir", "", "Directory containing TLS info")
	s.fs.StringVar(&s.dbgport, "dbgport", "", "Debug port for pprof webserver (optional)")

	return s
}

func (s *ServerSubcommand) Init(args []string) error {
	return s.fs.Parse(args)
}

func (s *ServerSubcommand) Name() string {
	return s.fs.Name()
}

// TODO - how dump bucketstats, etc???? do from webserver?
func (s *ServerSubcommand) Run() (err error) {
	if s.ipAddr == "" && s.dnsName == "" {
		err = fmt.Errorf("Must pass either IP Address or DNS name")
		s.fs.PrintDefaults()
		return
	}
	if s.ipAddr != "" && s.dnsName != "" {
		err = fmt.Errorf("Must pass only IP Address or DNS name")
		s.fs.PrintDefaults()
		return
	}
	if s.port == 0 {
		err = fmt.Errorf("Port cannot be 0")
		s.fs.PrintDefaults()
		return
	}
	if s.tlsDir == "" {
		err = fmt.Errorf("tlsdir cannot be empty")
		s.fs.PrintDefaults()
		return
	}

	// Start debug webserver if we have a debug port
	if s.dbgport != "" {
		hostPort := net.JoinHostPort("localhost", s.dbgport)
		go http.ListenAndServe(hostPort, nil)
	}

	err = becomeAServer(s.ipAddr, s.dnsName, s.port, s.tlsDir, true)
	return err
}
