package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/swiftstack/ProxyFS/conf"
)

func main() {
	args := os.Args[1:]

	// Read in the program's os.Arg[1]-specified (and required) .conf file
	if len(args) == 0 {
		log.Fatalf("no .conf file specified")
	}

	confMap, confErr := conf.MakeConfMapFromFile(args[0])
	if nil != confErr {
		log.Fatalf("failed to load config: %v", confErr)
	}

	// Update confMap with any extra os.Args supplied
	confErr = confMap.UpdateFromStrings(args[1:])
	if nil != confErr {
		log.Fatalf("failed to load config overrides: %v", confErr)
	}

	confMapJSONPacked, _ := json.Marshal(confMap)

	fmt.Printf("%v", string(confMapJSONPacked[:]))
}
