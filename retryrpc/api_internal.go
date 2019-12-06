// Package retryrpc provides a client and server RPC model which survives
// lost connections on either the client or the server.
package retryrpc

// jsonRequest is used to marshal an RPC request in/out of JSON
type jsonRequest struct {
	MyUniqueID string         `json:"myuniqueid"`
	RequestID  uint64         `json:"requestid"`
	Method     string         `json:"method"`
	Params     [1]interface{} `json:"params"`
}

// jsonResponse is used to marshal an RPC response in/out of JSON
type jsonResponse struct {
	Result interface{} `json:"result"`
}
