# ProxyFS
Integrated File and Object Access for Swift Object Storage

## Synopsis

ProxyFS provides integrated file and object API access for data stored with Swift object storage for the same data. 

ProxyFS supports SMB and NFS along with AWS S3 and Swift object protocols. 

ProxyFS is a hierarchical file system that supports POSIX requirements.

ProxyFS enables filesystem clients to mount and read/write data that is simultaneously accessible to object API clients.

## Development Environment

The officially supported development environment for ProxyFS is
[Runway](https://github.com/swiftstack/runway). Please read Runway's
documentation and make sure to use the ProxyFS manifest instead of the default
one.

## How to get the code (if not using Runway)

* Define your GOPATH as desired (where your bin/, /pkg, and /src/ directory trees will appear)
* cd $GOPATH
* mkdir -p src/github.com/swiftstack
* cd src/github.com/swiftstack
* git clone git@github.com:swiftstack/ProxyFS.git
* cd ProxyFS
* git submodule update --init --recursive

## How to run unit tests (in your Development Environment)

* Install/update to at least Go 1.8.3 (if not using Runway)
* cd $GOPATH/src/github.com/swiftstack/ProxyFS
* ./regression_test.py

## Commercial Deployment

See: http://swiftstack.com

## License

See LICENSE file
