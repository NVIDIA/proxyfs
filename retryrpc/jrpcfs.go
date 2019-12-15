package retryrpc

import "github.com/swiftstack/ProxyFS/jrpcfs"

// TODO - get rid of these

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

type svrRequest struct {
	Params [1]interface{} `json:"params"`
}

type svrResponse struct {
	Result interface{} `json:"result"`
}
