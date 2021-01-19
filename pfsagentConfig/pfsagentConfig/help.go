// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
)

var (
	usageText string = fmt.Sprintf(`
%v is a config utility for the SwiftStack ProxyFS Agent (pfsagentd).

It can be used interactively by just calling it with no parameters, as such:
%[1]v

[FUTURE] Or it can be used non-interactively by stating the parameter(s) you wish to modify, as such:
%[1]v swiftAuthURL <auth url> swiftAuthUser <username>....

For both interactive and non-interactive, the utility expects to find the the PFSagent config file at %[2]v. If you wish to use a different path, type
%[1]v configFile /path/to/config/file

For a longer explanation, you can type
%[1]v help

Which will also list all paraemters available for change.
For explanation about a specific parameter, type
%[1]v help <parameter>

To print this short usage message, type
%[1]v usage

`, os.Args[0], defaultConfigPath)
)

var version = fmt.Sprintf(`
	pfsagentConfig
		Client version: %v
		API version: %v
`, "0.0.1", "0.0.1")

// `, globalConf.Version, globalConf.APIVersion)
