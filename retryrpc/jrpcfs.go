package retryrpc

import (
	"encoding/json"
	"fmt"

	"github.com/swiftstack/ProxyFS/jrpcfs"
)

// TODO - how prove have correct Rpcs methods???

// methodToReply returns the appropriate reply structure for this method
func methodToReply(method string, buf []byte) (reply interface{}, err error) {

	switch method {
	case "Server.RpcChmod":
		reply = jrpcfs.Reply{}
		return

	case "Server.RpcChown":
		reply = jrpcfs.Reply{}
		return

	case "Server.RpcCreate":
		reply = jrpcfs.InodeReply{}
		return

	case "Server.RpcFetchExtentMapChunk":
		reply = jrpcfs.FetchExtentMapChunkReply{}
		return

	case "Server.RpcGetStat":
		reply = jrpcfs.StatStruct{}
		return

	case "Server.RpcGetXAttr":
		reply = jrpcfs.GetXAttrReply{}
		return

	case "Server.RpcLink":
		reply = jrpcfs.Reply{}
		return

	case "Server.RpcListXAttr":
		reply = jrpcfs.ListXAttrReply{}
		return

	case "Server.RpcLookup":
		reply = jrpcfs.InodeReply{}
		return

	case "Server.RpcMkdir":
		reply = jrpcfs.InodeReply{}
		return

		// TODO - do we need RpcMkdirPath here?

	case "Server.RpcMountByAccountName":
		// TODO - how do this?  Need this?

	case "Server.RpcPing":
		p := pingJSONReply{}
		err = json.Unmarshal(buf, &p)
		if err != nil {
			fmt.Printf("CLIENT: Unmarshal of buf failed with err: %v\n", err)
			// TODO - error handling???
			return
		}
		return p.Result[0], nil

	case "Server.RpcProvisionObject":
		reply = jrpcfs.ProvisionObjectReply{}
		return

	case "Server.RpcReadSymlink":
		reply = jrpcfs.ReadSymlinkReply{}
		return

	case "Server.RpcReaddirByLoc":
		reply = jrpcfs.ReaddirReply{}
		return

	case "Server.RpcRemoveXAttr":
		reply = jrpcfs.Reply{}
		return

	case "Server.RpcRename":
		reply = jrpcfs.Reply{}
		return

	case "Server.RpcResize":
		reply = jrpcfs.Reply{}
		return

	case "Server.RpcRmdir":
		reply = jrpcfs.Reply{}
		return

	case "Server.RpcSetTime":
		reply = jrpcfs.Reply{}
		return

	case "Server.RpcSetXAttr":
		reply = jrpcfs.Reply{}
		return

	case "Server.RpcStatVFS":
		reply = jrpcfs.StatVFS{}
		return

	case "Server.RpcSymlink":
		reply = jrpcfs.Reply{}
		return

	case "Server.RpcUnlink":
		reply = jrpcfs.Reply{}
		return

	case "Server.RpcWrote":
		reply = jrpcfs.WroteReply{}
		return

	default:
		err = fmt.Errorf("Invalid RPC Method - unable to return Reply structure for method: %v", method)
	}

	return
}

// These data structures in effect "redefine" jsonRequest and jsonReply.
//
// This is the only way we can unmarshal Params and Result into the
// appropriate concrete type
type pingJSONReq struct {
	Params [1]jrpcfs.PingReq `json:"params"`
}
type pingJSONReply struct {
	Result [1]jrpcfs.PingReply `json:"result"`
}
