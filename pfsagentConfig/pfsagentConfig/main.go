package main

import (
	"fmt"
	"os"

	pfsagentConfig "github.com/swiftstack/ProxyFS/pfsagentConfig"
)

const (
	defaultConfigPath string = "/etc/pfsagent/"
)

func main() {
	if len(os.Args) == 1 {
		loadError := pfsagentConfig.LoadConfig("")
		if nil != loadError {
			fmt.Println("Failed loading config. Error:", loadError)
			os.Exit(1)
		}
		pfsagentConfig.RunStateMachine()
		os.Exit(0)
	}

	switch os.Args[1] {
	case "firstRun":
		err := firstTimeRun()
		if nil != err {
			fmt.Println(err)
			os.Exit(2)
		}
		os.Exit(0)
	case "configPath":
		if len(os.Args) > 1 {
			pfsagentConfig.ConfigPath = os.Args[2]
			loadError := pfsagentConfig.LoadConfig("")
			if nil != loadError {
				fmt.Println("Failed loading config. Error:", loadError)
				os.Exit(1)
			}
			pfsagentConfig.RunStateMachine()
			os.Exit(0)
		}
		fmt.Print(usageText)
		os.Exit(2)
	case "version":
		fmt.Print(version)
		os.Exit(0)
	case "usage":
		fmt.Print(usageText)
		os.Exit(0)
	default:
		fmt.Print(usageText)
		os.Exit(2)
	}
}

func firstTimeRun() error {
	return pfsagentConfig.FirstTimeRun()
}
