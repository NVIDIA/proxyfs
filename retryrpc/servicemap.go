package retryrpc

import (
	"errors"
	"fmt"
	"reflect"
	"unicode"
	"unicode/utf8"
)

// NOTE: The basic code in this file is taken from Golang
// "net/rpc" server.go:register()

// Precompute the reflect type for error. Can't use error directly
// because Typeof takes an empty interface value. This is annoying.
var typeOfError = reflect.TypeOf((*error)(nil)).Elem()

// Find all methods which can be exported and build
// serviceMap
func (server *Server) buildServiceMap(typ reflect.Type) {
	for m := 0; m < typ.NumMethod(); m++ {
		method := typ.Method(m)
		mtype := method.Type
		mname := method.Name
		fmt.Printf("mname: %v method: %+v\n", mname, method)
		// Method must be exported.
		if method.PkgPath != "" {
			continue
		}
		// Method needs three ins: receiver, *args, *reply.
		if mtype.NumIn() != 3 {
			continue
		}
		// First arg need not be a pointer.
		argType := mtype.In(1)
		if !isExportedOrBuiltinType(argType) {
			continue
		}
		// Second arg must be a pointer.
		replyType := mtype.In(2)
		if replyType.Kind() != reflect.Ptr {
			continue
		}
		// Reply type must be exported.
		if !isExportedOrBuiltinType(replyType) {
			continue
		}
		// Method needs one out.
		if mtype.NumOut() != 1 {
			continue
		}
		// The return type of the method must be error.
		if returnType := mtype.Out(0); returnType != typeOfError {
			continue
		}

		server.serviceMap[mname] = &method
	}
}

// Is this type exported or a builtin?
//
// Taken from Golang "net/rpc" server.go
func isExportedOrBuiltinType(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	// PkgPath will be non-empty even for an exported type,
	// so we need to check the type name as well.
	return IsExported(t.Name()) || t.PkgPath() == ""
}

// IsExported reports whether name starts with an upper-case letter.
//
// NOTE: Go 1.13.1 has this function in the "go/token" package.
// Remove this function once we upgrade to 1.13.1.
func IsExported(name string) bool {
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

	if !IsExported(sname) {
		s := "retryrpc.Register: type " + sname + " is not exported"
		return errors.New(s)
	}
	fmt.Printf("typ: %v rcvr: %v sname: %v\n", typ, rcvr, sname)

	server.buildServiceMap(typ)
	fmt.Printf("ServiceMap: %+v\n", server.serviceMap)

	return
}
