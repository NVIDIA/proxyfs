// The mkproxyfs program is the command line form invoking the mkproxyfs package's Format() function.

package main

import (
	"fmt"
	"os"

	"github.com/swiftstack/ProxyFS/mkproxyfs"
)

func usage() {
	fmt.Println("mkproxyfs -?")
	fmt.Println("   Prints this help text")
	fmt.Println("mkproxyfs -N|-I|-F VolumeNameToFormat ConfFile [ConfFileOverrides]*")
	fmt.Println("   -N indicates that VolumeNameToFormat must be empty")
	fmt.Println("   -I indicates that VolumeNameToFormat should only be formatted if necessary")
	fmt.Println("   -F indicates that VolumeNameToFormat should be formatted regardless")
	fmt.Println("      Note: This may take awhile to clear out existing objects/containers")
	fmt.Println("  VolumeNameToFormat indicates which Volume in ConfFile is to be formatted")
	fmt.Println("      Note: VolumeNameToFormat need not be marked as active on any peer")
	fmt.Println("  ConfFile specifies the .conf file as also passed to proxyfsd et. al.")
	fmt.Println("  ConfFileOverrides is an optional list of modifications to ConfFile to apply")
}

func main() {
	var (
		err  error
		mode mkproxyfs.Mode
	)

	if (2 == len(os.Args)) && ("-?" == os.Args[1]) {
		usage()
		os.Exit(0)
	}

	if 4 > len(os.Args) {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "-N":
		mode = mkproxyfs.ModeNew
	case "-I":
		mode = mkproxyfs.ModeOnlyIfNeeded
	case "-F":
		mode = mkproxyfs.ModeReformat
	default:
		usage()
		os.Exit(1)
	}

	err = mkproxyfs.Format(mode, os.Args[2], os.Args[3], os.Args[4:], os.Args)
	if nil == err {
		os.Exit(0)
	} else {
		fmt.Fprintf(os.Stderr, "mkproxyfs: Format() returned error: %v\n", err)
		os.Exit(1)
	}
}
