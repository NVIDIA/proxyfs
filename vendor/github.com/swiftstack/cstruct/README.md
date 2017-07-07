# cstruct

Go package implementing cstruct-style pack/unpack

## API Reference

```
// LittleEndian byte order (i.e. least signficant byte first) for {|u}int{16|32|64} types
var LittleEndian = binary.LittleEndian

// BigEndian byte order (i.e. most signficant byte first) for {|u}int{16|32|64} types
var BigEndian = binary.BigEndian

// Examine may be used to determine the size of a []byte needed by Pack()
func Examine(objIF interface{}) (bytesNeeded uint64, trailingByteSlice bool, err error)

// Pack is used to serialize the supplied struct (passed by value or reference) in the desired byte order
func Pack(srcObjIF interface{}, byteOrder binary.ByteOrder) (dst []byte, err error)

// Unpack is used to deserialize into the supplied struct (passed by reference) in the desired byte order
//
// Note that if the supplied struct contains a trailing byte slice, bytesConsumed will equal len(src)
func Unpack(src []byte, dstObjIF interface{}, byteOrder binary.ByteOrder) (bytesConsumed uint64, err error)
```

## Contributors

 * ed@swiftstack.com

## License

TBD
