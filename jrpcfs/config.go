package jrpcfs

import (
	"os"
	"sync"

	"github.com/swiftstack/conf"

	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/logger"
)

type globalsStruct struct {
	sync.Mutex
	dataPathLogging bool

	// Map used to store volumes already mounted for bimodal support
	// TODO: These never get purged !!!
	mountIDMap  map[uint64]fs.MountHandle
	lastMountID uint64

	// Map used to store volumes already mounted for bimodal support
	bimodalMountMap map[string]fs.MountHandle
}

var globals globalsStruct

// NOTE: Don't use logger.Fatal* to error out from this function; it prevents us
//       from handling returned errors and gracefully unwinding.
func Up(confMap conf.ConfMap) (err error) {
	globals.mountIDMap = make(map[uint64]fs.MountHandle)
	globals.lastMountID = uint64(0) // The only invalid MountID

	globals.bimodalMountMap = make(map[string]fs.MountHandle)

	// Fetch IPAddr from config file
	peerName, err := confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		logger.ErrorfWithError(err, "failed to get Cluster.WhoAmI from config file")
		return
	}
	ipAddr, err := confMap.FetchOptionValueString(peerName, "PrivateIPAddr")
	if nil != err {
		logger.ErrorfWithError(err, "failed to get %s.PrivateIPAddr from config file", peerName)
		return
	}

	// Fetch port number from config file
	portString, err := confMap.FetchOptionValueString("JSONRPCServer", "TCPPort")
	if nil != err {
		logger.ErrorfWithError(err, "failed to get JSONRPCServer.TCPPort from config file")
		return
	}

	// Fetch fastPort number from config file
	fastPortString, err := confMap.FetchOptionValueString("JSONRPCServer", "FastTCPPort")
	if nil != err {
		logger.ErrorfWithError(err, "failed to get JSONRPCServer.TCPFastPort from config file")
		return
	}

	// Set data path logging level to true, so that all trace logging is controlled by settings
	// in the logger package. To enable jrpcfs trace logging, set Logging.TraceLevelLogging to jrpcfs.
	// This will enable all jrpcfs trace logs, including those formerly controled by globals.dataPathLogging.
	// To disable read/write/flush trace logs separately, change this setting here to false.
	globals.dataPathLogging, err = confMap.FetchOptionValueBool("JSONRPCServer", "DataPathLogging")
	if nil != err {
		logger.ErrorfWithError(err, "failed to get JSONRPCServer.DataPathLogging from config file")
		return
	}

	// Optionally (and typically for now)...
	//   copy the contents of rpc_server.conf to somewhere the samba json rpc client can see it
	rpcDontWriteConf, err := confMap.FetchOptionValueBool("JSONRPCServer", "DontWriteConf")
	if (nil != err) || !rpcDontWriteConf {
		// NOTE: We used to only write to the file if it didn't already exist.
		//       But now we write it every time. The side effect of this logic
		//       is that one cannot manually change this file, since any changes
		//       will be overwritten the next time this code runs.
		destFilename := "/tmp/rpc_server.conf"
		out, nonShadowingErr := os.OpenFile(destFilename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if nil != nonShadowingErr {
			logger.ErrorfWithError(nonShadowingErr, "failed to open /tmp/rpc_server.conf")
			err = nonShadowingErr
			return
		}

		_, err = out.WriteString("[JSONRPCServer]\n")
		_, err = out.WriteString("IPAddr: " + ipAddr + "\n")
		_, err = out.WriteString("TCPPort: " + portString + "\n")
		_, err = out.WriteString("FastTCPPort: " + fastPortString + "\n")

		err = out.Close()
		if nil != err {
			logger.ErrorfWithError(err, "failed to close /tmp/rpc_server.conf")
			return
		}
	}

	// Init JSON RPC server stuff
	jsonRpcServerUp(ipAddr, portString)

	// Now kick off our other, faster RPC server
	ioServerUp(ipAddr, fastPortString)

	return
}

func Down() (err error) {
	err = nil
	jsonRpcServerDown()
	ioServerDown()
	return
}
