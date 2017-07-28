package main

import (
	"log"
	"os"

	"github.com/swiftstack/ProxyFS/ramswift"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("no .conf file specified")
	}

	doneChan := make(chan bool, 1) // Must be buffered to avoid race

	go ramswift.Daemon(os.Args[1], os.Args[2:], nil, doneChan)

	_ = <-doneChan
}
