# cityhash

http://godoc.org/github.com/creachadair/cityhash

A transliteration of the CityHash implementation from C++ to Go.

This is a straightforward implementation in Go of the CityHash
non-cryptographic hashing algorithm, done by transliterating the C++
implementation.  The 32-bit, 64-bit, and 128-bit functions are implemented.
The CRC functions (e.g., `CityHashCrc128`) are not implemented.  This
implementation is up-to-date with version 1.1.1 of the C++ library.

The unit tests were constructed by extracting the test vectors from the C++
unit test file.  The `convert_tests` script does this extraction.

The original CityHash code can be found at: http://github.com/google/cityhash.
