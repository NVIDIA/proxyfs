package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
)

func main() {
	log.SetFlags(log.Lshortfile)

	// caCertPEM, err := ioutil.ReadFile("../ed25519_ca_cert.pem")
	// caCertPEM, err := ioutil.ReadFile("../ed25519_ca_combined.pem")
	// caCertPEM, err := ioutil.ReadFile("../rsa_ca_cert.pem")
	caCertPEM, err := ioutil.ReadFile("../rsa_ca_combined.pem")
	if err != nil {
		log.Println(err)
		return
	}

	rootCAs := x509.NewCertPool()
	ok := rootCAs.AppendCertsFromPEM(caCertPEM)
	if !ok {
		err := fmt.Errorf("rootCAs.AppendCertsFromPEM(caCertPEM) returned !ok")
		log.Println(err)
		return
	}

	tlsConfig := &tls.Config{
		RootCAs: rootCAs,
	}

	conn, err := tls.Dial("tcp", "127.0.0.1:8443", tlsConfig)
	if err != nil {
		log.Println(err)
		return
	}
	defer conn.Close()

	n, err := conn.Write([]byte("hello\n"))
	if err != nil {
		log.Println(n, err)
		return
	}

	buf := make([]byte, 100)
	n, err = conn.Read(buf)
	if err != nil {
		log.Println(n, err)
		return
	}

	println(string(buf[:n]))
}
