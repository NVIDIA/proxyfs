# ProxyFS
Integrated File and Object Access for Swift Object Storage

## Synopsis

ProxyFS is a hierarchical file system that provides integrated file
and object API access for data stored with
[Swift object storage](http://swift.openstack.org) for the same
data. It supports SMB and NFS along with AWS S3 and Swift object
protocols. We call this "bi-modal access", and it means that filesystem
clients and mount and read/write data that is simultaneously accessible
to object API clients.

## How to Contribute

All code contributions for ProxyFS go through GitHub.

https://github.com/swiftstack/ProxyFS

Please feel free to contribute by opening a pull request to the
`development` branch. If you see an open pull request, feel free to
review it and leave comments on it. ProxyFS follows a
[git flow](https://datasift.github.io/gitflow/IntroducingGitFlow.html)
development model.

If you'd like to ask questions, discuss ideas, or otherwise communicate
with other ProxyFS contributors, please register for and participate
in the [ProxyFS dev mailing list](https://lists.proxyfs.org/mailman/listinfo).

## Development Environment

The officially supported development environment for ProxyFS is
[Runway](https://github.com/swiftstack/runway). Please read Runway's
documentation and make sure to use the ProxyFS manifest instead of the default
one.

## How to get the code (if not using Runway)

* Define your GOPATH as desired (where your bin/, /pkg, and /src/
  directory trees will appear)
* cd $GOPATH
* mkdir -p src/github.com/swiftstack
* cd src/github.com/swiftstack
* git clone git@github.com:swiftstack/ProxyFS.git
* cd ProxyFS
* git submodule update --init --recursive

## How to run unit tests (in your Development Environment)

* Install/update to at least Go 1.8.3 (if not using Runway)
* Ensure $GOPATH/bin is in your $PATH
* cd $GOPATH/src/github.com/swiftstack/ProxyFS
* ./regression_test.py

## Commercial Deployment

See: http://swiftstack.com

## License

See LICENSE file
