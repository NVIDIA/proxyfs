package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/evtlog"
	"github.com/swiftstack/ProxyFS/transitions"
)

func usage() {
	fmt.Println("pfsevtlogd -?")
	fmt.Println("   Prints this help text")
	fmt.Println("pfsevtlogd [-D] ConfFile [ConfFileOverrides]*")
	fmt.Println("   -D requests that just the Shared Memory Object be deleted")
	fmt.Println("  ConfFile specifies the .conf file as also passed to proxyfsd et. al.")
	fmt.Println("  ConfFileOverrides is an optional list of modifications to ConfFile to apply")
}

func main() {
	var (
		args                         []string
		confMap                      conf.ConfMap
		err                          error
		formattedRecord              string
		justDeleteSharedMemoryObject bool
		numDroppedRecords            uint64
		outputFile                   *os.File
		outputPath                   string
		pollDelay                    time.Duration
		signalChan                   chan os.Signal
	)

	if (2 == len(os.Args)) && ("-?" == os.Args[1]) {
		usage()
		os.Exit(0)
	}

	if (1 < len(os.Args)) && ("-D" == os.Args[1]) {
		justDeleteSharedMemoryObject = true
		args = os.Args[2:]
	} else {
		justDeleteSharedMemoryObject = false
		args = os.Args[1:]
	}

	if len(args) < 1 {
		log.Fatalf("no .conf file specified")
	}

	confMap, err = conf.MakeConfMapFromFile(args[0])
	if nil != err {
		log.Fatalf("failed to load config: %v", err)
	}

	err = confMap.UpdateFromStrings(args[1:])
	if nil != err {
		log.Fatalf("failed to apply config overrides: %v", err)
	}

	err = transitions.Up(confMap)
	if nil != err {
		log.Fatalf("transitions.Up() failed: %v", err)
	}

	if justDeleteSharedMemoryObject {
		evtlog.MarkForDeletion()
		err = transitions.Down(confMap)
		if nil != err {
			log.Fatalf("transitions.Down() failed: %v", err)
		}
		os.Exit(0)
	}

	pollDelay, err = confMap.FetchOptionValueDuration("EventLog", "DaemonPollDelay")
	if nil != err {
		log.Fatalf("confMap.FetchOptionValueDuration(\"EventLog\", \"DaemonPollDelay\") failed: %v", err)
	}

	outputPath, err = confMap.FetchOptionValueString("EventLog", "DaemonOutputPath")
	if nil == err {
		outputFile, err = os.OpenFile(outputPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, syscall.S_IRUSR|syscall.S_IWUSR)
	} else {
		outputFile = os.Stdout
	}

	signalChan = make(chan os.Signal, 1)

	signal.Notify(signalChan, unix.SIGINT, unix.SIGTERM)

	for {
		formattedRecord, numDroppedRecords = evtlog.Retrieve()

		if 0 != numDroppedRecords {
			fmt.Fprintf(outputFile, "*** numDroppedRecords == 0x%016X\n", numDroppedRecords)
		}

		if "" == formattedRecord {
			select {
			case _ = <-signalChan:
				err = outputFile.Close()
				if nil != err {
					log.Fatalf("outputFile.Close() failed: %v", err)
				}

				err = transitions.Down(confMap)
				if nil != err {
					log.Fatalf("transitions.Down() failed: %v", err)
				}

				os.Exit(0)
			case _ = <-time.After(pollDelay):
				// Just loop back to attempt evtlog.Retrieve()
			}
		} else {
			fmt.Fprintf(outputFile, "%s\n", formattedRecord)
		}
	}
}
