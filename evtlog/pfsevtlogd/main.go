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
)

func main() {
	var (
		confMap         conf.ConfMap
		err             error
		formattedRecord string
		outputFile      *os.File
		outputPath      string
		pollDelay       time.Duration
		signalChan      chan os.Signal
	)

	if len(os.Args) < 2 {
		log.Fatalf("no .conf file specified")
	}

	confMap, err = conf.MakeConfMapFromFile(os.Args[1])
	if nil != err {
		log.Fatalf("failed to load config: %v", err)
	}

	err = confMap.UpdateFromStrings(os.Args[2:])
	if nil != err {
		log.Fatalf("failed to apply config overrides: %v", err)
	}

	err = evtlog.Up(confMap)
	if nil != err {
		log.Fatalf("evtlog.Up() failed: %v", err)
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
		formattedRecord = evtlog.Retrieve()

		if "" == formattedRecord {
			select {
			case _ = <-signalChan:
				err = outputFile.Close()
				if nil != err {
					log.Fatalf("outputFile.Close() failed: %v", err)
				}

				err = evtlog.Down()
				if nil != err {
					log.Fatalf("evtlog.Down() failed: %v", err)
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
