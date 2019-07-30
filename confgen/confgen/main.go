// This is a command-line wrapper around package confgen APIs.
//
// Note that the ConfFileOverrides` arguments are specified at the end of the command
// in order to allow for a list of zero or more items. As such, the `ConfFilePath`
// argument immediately preceeds `ConfFileOverrides`. This is the reverse of the
// argument order in the package confgen APIs that conversely follow a from->to
// convention.
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/swiftstack/ProxyFS/confgen"
)

func usage() {
	fmt.Printf("%s -?\n", os.Args[0])
	fmt.Printf("  Prints this help text\n")
	fmt.Printf("%s -I InitialDirPath ConfFilePath [ConfOverrides]*\n", os.Args[0])
	fmt.Printf("  Computes the initial set of conf files storing them in created dir <InitialDirPath>\n")
	fmt.Printf("%s -P InitialDirPath PhaseOneDirPath PhaseTwoDirPath ConfFilePath [ConfOverrides]*\n", os.Args[0])
	fmt.Printf("  Computes the two-phase migration from the conf files in <InitialDirPath>\n")
	fmt.Printf("  to reach the conf specified in <ConfFilePath>+<ConfOverrides> by first\n")
	fmt.Printf("  evacuating Volumes and VolumeGroups that are moving away or being deleted\n")
	fmt.Printf("  in Phase One and then populating Volumes and VolumeGroups that are arriving\n")
	fmt.Printf("  (via creation of moving towards) in Phase Two.\n")
}

func main() {
	var (
		confFilePath    string
		confOverrides   []string
		err             error
		initialDirPath  string
		phaseOneDirPath string
		phaseTwoDirPath string
	)

	if 1 == len(os.Args) {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "-?":
		usage()
		os.Exit(0)
	case "-I":
		if 4 > len(os.Args) {
			usage()
			os.Exit(1)
		}

		initialDirPath = os.Args[2]
		confFilePath = os.Args[3]
		confOverrides = os.Args[4:]

		err = confgen.ComputeInitial(confFilePath, confOverrides, initialDirPath)
	case "-P":
		if 6 > len(os.Args) {
			usage()
			os.Exit(1)
		}

		initialDirPath = os.Args[2]
		phaseOneDirPath = os.Args[3]
		phaseTwoDirPath = os.Args[4]
		confFilePath = os.Args[5]
		confOverrides = os.Args[6:]

		err = confgen.ComputePhases(initialDirPath, confFilePath, confOverrides, phaseOneDirPath, phaseTwoDirPath)
	default:
		usage()
		os.Exit(1)
	}

	if nil != err {
		log.Fatal(err)
	}
}
