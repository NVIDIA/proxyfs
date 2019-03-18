package main

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/swiftstack/ProxyFS/jrpcfs"
)

// func TestFunctional(t *testing.T) {
// 	testSetup(t)
// 	time.Sleep(10 * time.Second) // TODO
// 	testTeardown(t)
// }

type testPingReqStruct struct {
	JSONrpc string            `json:"jsonrpc"`
	Method  string            `json:"method"`
	Params  [1]jrpcfs.PingReq `json:"params"`
	ID      uint64            `json:"id"`
}

func TestMe(t *testing.T) {
	tPR := testPingReqStruct{
		JSONrpc: "2.0",
		Method:  "Server.RpcPing",
		ID:      42,
	}
	tPR.Params[0].Message = "Hi"
	buf, _ := json.Marshal(tPR)
	fmt.Printf("\n%s\n\n", string(buf[:]))

	var jrpcGenericRequest JrpcGenericRequestStruct
	err := json.Unmarshal(buf, &jrpcGenericRequest)
	if (nil != err) || ("2.0" != jrpcGenericRequest.JSONrpc) {
		t.Fatalf("json.Unmarshal(G) had issues... err: %v", err)
	}

	jrpcPingReq := &JrpcPingReqStruct{}
	err = json.Unmarshal(buf, jrpcPingReq)
	if nil != err {
		t.Fatalf("json.Unmarshal(P) had issues... err: %v", err)
	}

	fmt.Printf("jrpcPingReq: %#v\n\n", jrpcPingReq)

	jrpcPingReply := &JrpcPingReplyStruct{
		ID:     jrpcPingReq.ID,
		Error:  "",
		Result: jrpcfs.PingReply{Message: jrpcPingReq.Params[0].Message},
	}

	buf, err = json.Marshal(jrpcPingReply)
	if nil != err {
		t.Fatalf("json.Marshal(P) had issues... err: %v", err)
	}
	fmt.Printf("%s\n", string(buf[:]))
}
