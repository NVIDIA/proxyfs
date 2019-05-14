package pfsagent

import (
	"encoding/json"
	"fmt"
)

type jrpcRequestMethodAndIDStruct struct {
	Method string `json:"method"`
	ID     uint64 `json:"id"`
}

type jrpcRequestStruct struct {
	JSONrpc string         `json:"jsonrpc"`
	Method  string         `json:"method"`
	ID      uint64         `json:"id"`
	Params  [1]interface{} `json:"params"`
}

type jrpcResponseIDStruct struct {
	ID uint64 `json:"id"`
}

type jrpcResponseIDAndErrorStruct struct {
	ID    uint64 `json:"id"`
	Error string `json:"error"`
}

type jrpcResponseNoErrorStruct struct {
	ID     uint64      `json:"id"`
	Result interface{} `json:"result"`
}

type jrpcResponseWithErrorStruct struct {
	ID     uint64      `json:"id"`
	Error  string      `json:"error"`
	Result interface{} `json:"result"`
}

func jrpcMarshalRequest(requestMethod string, request interface{}) (requestID uint64, requestBuf []byte, marshalErr error) {
	var (
		jrpcRequest *jrpcRequestStruct
	)

	globals.Lock()
	requestID = globals.jrpcLastID + 1
	globals.jrpcLastID = requestID
	globals.Unlock()

	jrpcRequest = &jrpcRequestStruct{
		JSONrpc: "2.0",
		Method:  requestMethod,
		ID:      requestID,
		Params:  [1]interface{}{request},
	}

	requestBuf, marshalErr = json.Marshal(jrpcRequest)

	return
}

func jrpcMarshalResponse(requestID uint64, responseError error, response interface{}) (responseBuf []byte, marshalErr error) {
	var (
		jrpcResponse interface{}
	)

	if nil == responseError {
		if nil == response {
			jrpcResponse = &jrpcResponseIDStruct{
				ID: requestID,
			}
		} else {
			jrpcResponse = &jrpcResponseNoErrorStruct{
				ID:     requestID,
				Result: response,
			}
		}
	} else {
		if nil == response {
			jrpcResponse = &jrpcResponseIDAndErrorStruct{
				ID:    requestID,
				Error: responseError.Error(),
			}
		} else {
			jrpcResponse = &jrpcResponseWithErrorStruct{
				ID:     requestID,
				Error:  responseError.Error(),
				Result: response,
			}
		}
	}

	responseBuf, marshalErr = json.Marshal(jrpcResponse)

	return
}

func jrpcUnmarshalRequestForMethodAndID(requestBuf []byte) (requestMethod string, requestID uint64, unmarshalErr error) {
	var (
		jrpcRequest *jrpcRequestMethodAndIDStruct
	)

	jrpcRequest = &jrpcRequestMethodAndIDStruct{}

	unmarshalErr = json.Unmarshal(requestBuf, jrpcRequest)

	if nil == unmarshalErr {
		requestMethod = jrpcRequest.Method
		requestID = jrpcRequest.ID
	}

	return
}

func jrpcUnmarshalRequest(requestID uint64, requestBuf []byte, request interface{}) (unmarshalErr error) {
	var (
		jrpcRequest *jrpcRequestStruct
	)

	jrpcRequest = &jrpcRequestStruct{
		Params: [1]interface{}{request},
	}

	unmarshalErr = json.Unmarshal(requestBuf, jrpcRequest)

	if (nil == unmarshalErr) && (requestID != jrpcRequest.ID) {
		unmarshalErr = fmt.Errorf("requestID mismatch")
	}

	return
}

func jrpcUnmarshalResponseForIDAndError(responseBuf []byte) (requestID uint64, responseErr error, unmarshalErr error) {
	var (
		jrpcResponse *jrpcResponseIDAndErrorStruct
	)

	jrpcResponse = &jrpcResponseIDAndErrorStruct{}

	unmarshalErr = json.Unmarshal(responseBuf, jrpcResponse)

	if nil == unmarshalErr {
		requestID = jrpcResponse.ID
		if "" == jrpcResponse.Error {
			responseErr = nil
		} else {
			responseErr = fmt.Errorf("%s", jrpcResponse.Error)
		}
	}

	return
}

func jrpcUnmarshalResponse(requestID uint64, responseBuf []byte, response interface{}) (unmarshalErr error) {
	var (
		jrpcResponse *jrpcResponseWithErrorStruct
	)

	jrpcResponse = &jrpcResponseWithErrorStruct{
		Result: response,
	}

	unmarshalErr = json.Unmarshal(responseBuf, jrpcResponse)

	if (nil == unmarshalErr) && (requestID != jrpcResponse.ID) {
		unmarshalErr = fmt.Errorf("requestID mismatch")
	}

	return
}
