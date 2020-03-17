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

var (
// cfgName string
)

// guidedCmd represents the guided command
var guidedCmd = &cobra.Command{
	Use:   "guided",
	Short: "A menu-driven TUI",
	RunE:  guidedRunE,
}

func guidedRunE(cmd *cobra.Command, args []string) (err error) {
	cfg := new(pfsagentConfig.PFSagentConfig)
	cfg.SetConfigPath(cfgPath)
	err = cfg.LoadConfig("")
	if nil != err {
		fmt.Println("Failed loading config. Error:", err)
		return
		// os.Exit(1)
	}
	err = pfsagentConfig.RunStateMachine(*cfg)
	if nil != err {
		fmt.Println(err)
		return
		// os.Exit(2)
	}
	return
	// os.Exit(0)
}

func init() {
	rootCmd.AddCommand(guidedCmd)
	guidedCmd.Flags().StringVarP(&cfgName, "config", "c", "", `The name of an existing or new config.
	If config is not found a new config will be created with the given name.
	If config is not provided a new config will be created and named as the SwiftStack account name`)
}
