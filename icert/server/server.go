package main

import (
	"bufio"
	"crypto/tls"
	"log"
	"net"
)

func main() {
	log.SetFlags(log.Lshortfile)

	// cer, err := tls.LoadX509KeyPair("../ed25519_cert.pem", "../ed25519_key.pem")
	// cer, err := tls.LoadX509KeyPair("../ed25519_combined.pem", "../ed25519_combined.pem")
	// cer, err := tls.LoadX509KeyPair("../rsa_cert.pem", "../rsa_key.pem")
	cer, err := tls.LoadX509KeyPair("../rsa_combined.pem", "../rsa_combined.pem")
	if err != nil {
		log.Println(err)
		return
	}

	config := &tls.Config{Certificates: []tls.Certificate{cer}}
	ln, err := tls.Listen("tcp", "127.0.0.1:8443", config)
	if err != nil {
		log.Println(err)
		return
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		msg, err := r.ReadString('\n')
		if err != nil {
			log.Println(err)
			return
		}

		println(msg)

		n, err := conn.Write([]byte("world\n"))
		if err != nil {
			log.Println(n, err)
			return
		}
	}
}
