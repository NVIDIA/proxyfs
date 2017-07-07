// The proxyfsd program is the main ProxyFS dæmon and is named accordingly.
package main

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/pkg/profile"
	"github.com/swiftstack/conf"

	// NOTE: DO NOT use logger in this code, since it is not up when this code runs.
	//"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/proxyfsd"
)

const (
	PfsDebugCfgSection = "ProxyfsDebug"
	ProfileTypeKey     = "ProfileType"
)

// NOTE: Don't use logger.Fatal* to error out from this function; it prevents us
//       from handling returned errors and gracefully unwinding.
func main() {
	args := os.Args[1:]

	// Read in the program's os.Arg[1]-specified (and required) .conf file
	if len(args) == 0 {
		log.Printf("proxyfsd/main(): ERROR no .conf file specified")
		// Exit with non-success status that can be checked from scripts
		os.Exit(1)
	}

	confMap, confErr := conf.MakeConfMapFromFile(args[0])
	if nil != confErr {
		log.Printf("proxyfsd/main(): failed to load config, err %v", confErr)
		// Exit with non-success status that can be checked from scripts
		os.Exit(1)
	}

	// Update confMap with any extra os.Args supplied
	for i := 1; i < len(args); i++ {
		confErr = confMap.UpdateFromString(args[i])
		if nil != confErr {
			log.Printf("proxyfsd/main(): failed to load config overrides, err %v", confErr)
			// Exit with non-success status that can be checked from scripts
			os.Exit(1)
		}
	}

	errChan := make(chan error, 1) // Must be buffered to avoid race
	var wg sync.WaitGroup

	// Start profiling using pkg/profile, if requested. Profiling stops when proxyfsd exits.
	//
	// With the settings used below, output goes to a generated directory in /tmp.
	//  (example: /tmp/profile083387279/cpu.pprof)
	//
	// go tool pprof can then be used to analyze the file.
	//
	profileType, confErr := confMap.FetchOptionValueString(PfsDebugCfgSection, ProfileTypeKey)
	if confErr == nil {
		switch profileType {
		case "Memory":
			defer profile.Start(profile.MemProfile).Stop()
		case "Block":
			defer profile.Start(profile.BlockProfile).Stop()
		case "CPU":
			defer profile.Start(profile.CPUProfile).Stop()
		}
	}
	// If not specified in conf map or type doesn't match one of the above, don't do profiling.

	go proxyfsd.Daemon(confMap, nil, errChan, &wg)

	err := <-errChan

	wg.Wait() // wait for services to go Down()

	if nil != err {
		fmt.Fprintf(os.Stderr, "proxyfsd/Daemon(): exited with error: %v\n", err) // Can't use logger.*() as it's not currently "up"
		os.Exit(1)                                                                // Exit with non-success status that can be checked from scripts
	}
}
