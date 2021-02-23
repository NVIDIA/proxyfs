// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iauth

import (
	"fmt"
	"plugin"
)

// PerformAuth accepts a path to an Auth PlugIn and a string to pass to a func
// also named PerformAuth requesting it to perform the necessary authorization.
//
// The return from the Auth PlugIn's PerformAuth func is simply returned to the
// caller of this func.
//
func PerformAuth(authPlugInPath string, authInString string) (authToken string, storageURL string, err error) {
	var (
		ok                  bool
		performAuthAsFunc   func(authInString string) (authToken string, storageURL string, err error)
		performAuthAsSymbol plugin.Symbol
		plugIn              *plugin.Plugin
	)

	plugIn, err = plugin.Open(authPlugInPath)
	if nil != err {
		err = fmt.Errorf("plugin.Open(\"%s\") failed: %v", authPlugInPath, err)
		return
	}

	performAuthAsSymbol, err = plugIn.Lookup("PerformAuth")
	if nil != err {
		err = fmt.Errorf("plugIn[\"%s\"].Lookup(\"PerformAuth\") failed: %v", authPlugInPath, err)
		return
	}

	performAuthAsFunc, ok = performAuthAsSymbol.(func(authInString string) (authToken string, storageURL string, err error))
	if !ok {
		err = fmt.Errorf("performAuthAsSymbol.(func(authInString string) (authToken string, storageURL string, err error)) returned !ok")
		return
	}

	authToken, storageURL, err = performAuthAsFunc(authInString)

	return
}
