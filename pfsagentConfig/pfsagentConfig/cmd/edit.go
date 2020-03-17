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
	"github.com/spf13/cobra"
	"github.com/swiftstack/ProxyFS/pfsagentConfig"
)

var (
	// configName          string
	varSwiftAuthURL     string
	varSwiftAuthUser    string
	varSwiftAuthKey     string
	varSwiftAccountName string
)

// editCmd represents the edit command
var editCmd = &cobra.Command{
	Use:   "edit",
	Short: "Edit an existing config",
	RunE:  editRunE,
}

func editRunE(cmd *cobra.Command, args []string) (err error) {
	cfg := new(pfsagentConfig.PFSagentConfig)
	cfg.SetConfigPath(cfgPath)
	var undoMap map[string]string
	err = cfg.LoadConfig(cfgName)
	if nil != err {
		return
	}
	if len(varSwiftAuthURL) > 0 {
		undoMap["SwiftAuthURL"], err = cfg.GetValue("SwiftAuthURL")
		if nil != err {
			return
		}
		cfg.UpdateFromString("SwiftAuthURL", varSwiftAuthURL)
	}

	// var errorType int
	_, err = cfg.ValidateAccess()
	if nil != err {

		cfg.UpdateFromString()
		return
	}
	return
}

func init() {
	rootCmd.AddCommand(editCmd)
	editCmd.Flags().StringVarP(&cfgName, "config", "c", "", "The config name to validate")
	editCmd.MarkFlagRequired("config")
	editCmd.Flags().StringVar(&varSwiftAuthURL, "SwiftAuthURL", "", "Auth URL for the SwiftStack cluster")
	editCmd.Flags().StringVar(&varSwiftAuthUser, "SwiftAuthUser", "", "The user name for the SwiftStack cluster")
	editCmd.Flags().StringVar(&varSwiftAuthKey, "SwiftAuthKey", "", "The user key for the SwiftStack cluster")
	editCmd.Flags().StringVar(&varSwiftAccountName, "SwiftAccountName", "", "The SwiftStack account to access")

}
