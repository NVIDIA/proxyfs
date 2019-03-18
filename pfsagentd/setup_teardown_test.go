package main

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/swiftstack/ProxyFS/conf"
)

func testSetup(t *testing.T) {
	var (
		err             error
		testConfMap     conf.ConfMap
		testConfStrings []string
		testDir         string
	)

	testDir, err = ioutil.TempDir(os.TempDir(), "pfsagentd_test_")
	if nil != err {
		t.Fatalf("ioutil.TempDir() failed: %v", err)
	}

	err = os.Chdir(testDir)
	if nil != err {
		t.Fatalf("os.Chdir() failed: %v", err)
	}

	err = os.Mkdir("TestMountPointPath", 0777)
	if nil != err {
		t.Fatalf("os.Mkdir() failed: %v", err)
	}

	testConfStrings = []string{
		"Agent.FUSEVolumeName=TestVolumeName",
		"Agent.FUSEMountPointPath=TestMountPointPath",
		"Agent.FUSEUnMountRetryDelay=100ms",
		"Agent.FUSEUnMountRetryCap=100",
		"Agent.SwiftAuthURL=http://" + testSwiftProxyAddr + "/auth/v1.0",
		"Agent.SwiftAuthUser=TestAuthUser",
		"Agent.SwiftAuthKey=TestAuthKey",
		"Agent.SwiftAccountName=AUTH_TestAccount",
		"Agent.SwiftTimeout=10s",
		"Agent.SwiftRetryLimit=10",
		"Agent.SwiftRetryDelay=1s",
		"Agent.SwiftRetryExpBackoff=1.4",
		"Agent.SwiftConnectionPoolSize=100",
		"Agent.ReadCacheLineSize=1048576",
		"Agent.ReadCacheLineCount=1000",
		"Agent.ReadPlanLineSize=1048576",
		"Agent.ReadPlanLineCount=1000",
		"Agent.LogFilePath=",
		"Agent.LogToConsole=true",
		"Agent.TraceEnabled=false",
	}

	testConfMap, err = conf.MakeConfMapFromStrings(testConfStrings)
	if nil != err {
		t.Fatalf("conf.MakeConfMapFromStrings() failed: %v", err)
	}

	globals.logFile = nil
	globals.config.LogFilePath = ""
	globals.config.LogToConsole = true

	startSwiftProxyEmulator(t)

	initializeGlobals(testConfMap)

	performMount()
}

func testTeardown(t *testing.T) {
	var (
		err     error
		testDir string
	)

	performUnmount()

	testDir, err = os.Getwd()
	if nil != err {
		t.Fatalf("os.Getwd() failed: %v", err)
	}

	err = os.Chdir("..")
	if nil != err {
		t.Fatalf("os.Chdir() failed: %v", err)
	}

	err = os.RemoveAll(testDir)
	if nil != err {
		t.Fatalf("os.RemoveAll() failed: %v", err)
	}

	stopSwiftProxyEmulator()
}
