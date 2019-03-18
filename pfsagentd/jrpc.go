package main

import (
	"github.com/swiftstack/ProxyFS/jrpcfs"
)

type JrpcGenericRequestStruct struct {
	JSONrpc string `json:"jsonrpc"`
	Method  string `json:"method"`
	ID      uint64 `json:"id"`
}

type JrpcGenericReplyStruct struct {
	ID    uint64 `json:"id"`
	Error string `json:"error"`
}

type JrpcPingReqStruct struct {
	JrpcGenericRequestStruct
	Params [1]jrpcfs.PingReq `json:"params"`
}

type JrpcPingReplyStruct struct {
	ID     uint64           `json:"id"`
	Error  string           `json:"error"`
	Result jrpcfs.PingReply `json:"result"`
}
