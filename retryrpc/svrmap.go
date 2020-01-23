package retryrpc

import (
	"errors"
	"reflect"
	"unicode"
	"unicode/utf8"
)

var typeOfError = reflect.TypeOf((*error)(nil)).Elem()

// Find all methods for the type which can be exported.
// Build svrMap listing methods available as well as their
// request and reply types.
func (server *Server) buildSvrMap(typ reflect.Type) {
	for m := 0; m < typ.NumMethod(); m++ {
		method := typ.Method(m)
		mtype := method.Type
		mname := method.Name

		// Just like net/rpc, we have these requirements on methods:
		// - must be exported
		// - needs three ins: receiver, *args, *reply
		// - reply has to be a pointer and must be exported
		// - method can only return one value of type error
		if method.PkgPath != "" {
			continue
		}

		if mtype.NumIn() != 3 {
			continue
		}
		argType := mtype.In(1)
		if !isExportedOrBuiltinType(argType) {
			continue
		}

		replyType := mtype.In(2)
		if replyType.Kind() != reflect.Ptr {
			continue
		}

		if !isExportedOrBuiltinType(replyType) {
			continue
		}

		if mtype.NumOut() != 1 {
			continue
		}

		returnType := mtype.Out(0)
		if returnType != typeOfError {
			continue
		}

		// We save off the request type so we know how to unmarshal the request.
		// We use the reply type to allocate the reply struct and marshal the response.
		ma := methodArgs{methodPtr: &method, request: argType, reply: replyType}
		server.svrMap[mname] = &ma
	}
}

func isExportedOrBuiltinType(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return isMethodExported(t.Name()) || t.PkgPath() == ""
}

func isMethodExported(name string) bool {
	ch, _ := utf8.DecodeRuneInString(name)
	return unicode.IsUpper(ch)
}

// Figure out what methods we can have as RPCs and build the
// service map
func (server *Server) register(retrySvr interface{}) (err error) {

	// Find all the methods associated with retrySvr and put into serviceMap
	typ := reflect.TypeOf(retrySvr)
	rcvr := reflect.ValueOf(retrySvr)
	sname := reflect.Indirect(rcvr).Type().Name()

	if !isMethodExported(sname) {
		s := "retryrpc.Register: type " + sname + " is not exported"
		return errors.New(s)
	}

	server.buildSvrMap(typ)
	return
}
