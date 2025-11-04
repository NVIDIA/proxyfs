# swiftclient

A Go package providing a thread-safe Keyed Cache for inodes, buffers, DLM locks, and anything else that might need it.

## Synopsis

Provides a thread-safe cache for storing objects which are identified by a key.
The maximum size of the cache is bounded and old objects are discarded to make
space for new ones.  The client must provide read() and write() callbacks to
support instantiation of new objects and flushing of dirty objects.  The cache
interlocks object instatiation and initialization to insure only one instance of
a particular key is present and that it is initialized (read) before any thread
can see its content.  Reference counting is used to insure referenced objects
are kept around as long as they're needed, so the client must be careful to
release each hold that it acquires.

## Motivation

We need a thread-safe map with a bounded size to 1) cache recently accessed
objects and 2) cache dirty objects such tha multiple modifications can be made
to the object before it is flushed.

## Installation

TBD

## API Reference

TBD

## Tests

TBD

## Contributors

 * charmer@swiftstack.com

## License

TBD
