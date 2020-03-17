/*
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/swiftstack/ProxyFS/pfsagentConfig"
)

// firstRunCmd represents the firstRun command
var firstRunCmd = &cobra.Command{
	Use:   "firstRun",
	Short: "A short wizard to only set parameters to connect to a SwiftStack cluster (Auth URL, user, key, account)",
	RunE:  firstRunRunE,
}

func firstRunRunE(cmd *cobra.Command, args []string) (err error) {
	cfg := new(pfsagentConfig.PFSagentConfig)
	cfg.SetConfigPath(cfgPath)
	err = cfg.LoadConfig(cfgName)
	if nil != err {
		return
	}
	err = pfsagentConfig.FirstTimeRun(*cfg)
	if nil != err {
		fmt.Println(err)
		return
		// os.Exit(2)
	}
	// os.Exit(0)
	return
}

func init() {
	rootCmd.AddCommand(firstRunCmd)
}
