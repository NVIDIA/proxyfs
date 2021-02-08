// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/NVIDIA/proxyfs/conf"
	"github.com/NVIDIA/proxyfs/transitions"
)

func main() {
	var (
		args              []string
		confErr           error
		confMap           conf.ConfMap
		confMapJSON       bytes.Buffer
		confMapJSONPacked []byte
	)

	args = os.Args[1:]

	// Read in the program's os.Arg[1]-specified (and required) .conf file
	if len(args) == 0 {
		log.Fatalf("no .conf file specified")
	}

	confMap, confErr = conf.MakeConfMapFromFile(args[0])
	if nil != confErr {
		log.Fatalf("failed to load config: %v", confErr)
	}

	// Update confMap with any extra os.Args supplied
	confErr = confMap.UpdateFromStrings(args[1:])
	if nil != confErr {
		log.Fatalf("failed to load config overrides: %v", confErr)
	}

	confErr = transitions.UpgradeConfMapIfNeeded(confMap)
	if nil != confErr {
		log.Fatalf("failed to upgrade config: %v", confErr)
	}

	confMapJSONPacked, _ = json.Marshal(confMap)
	json.Indent(&confMapJSON, confMapJSONPacked, "", "\t")

	fmt.Printf("%v\n", confMapJSON.String())
}
