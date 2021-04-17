[![Build Status](https://github.com/NVIDIA/proxyfs/actions/workflows/run-unit-tests.yml/badge.svg)](https://github.com/NVIDIA/proxyfs/actions/workflows/run-unit-tests.yml?query=branch%3Adevelopment)
[![Coverage Status](https://coveralls.io/repos/github/swiftstack/ProxyFS/badge.svg?branch=development)](https://coveralls.io/github/swiftstack/ProxyFS?branch=development)

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

https://github.com/NVIDIA/proxyfs

Please feel free to contribute by opening a pull request to the
`development` branch. If you see an open pull request, feel free to
review it and leave comments on it. ProxyFS follows a
[git flow](https://datasift.github.io/gitflow/IntroducingGitFlow.html)
development model.

If you'd like to ask questions, discuss ideas, or otherwise communicate
with other ProxyFS contributors, please register for and participate
in the [ProxyFS dev mailing list](https://lists.proxyfs.org/mailman/listinfo)
or the [ProxyFS Slack group](https://proxyfs.slack.com), which you can join through
[this inivitation](https://join.slack.com/t/proxyfs/shared_invite/enQtMzA2NTQwMDU4NTkyLWM4ZjhkYmE0NWEzMTYzZGZkNThkNzcxMzg0NWIzMmQ4MTU5MGQyMDRlY2UzMDU0YjBlNGZkMzk4N2NkNTRjNjY).

## How to get the code

* Define your GOPATH as desired (where your bin/, /pkg, and /src/
  directory trees will appear)
* cd $GOPATH
* mkdir -p src/github.com/NVIDIA
* cd src/github.com/NVIDIA
* git clone git@github.com:NVIDIA/proxyfs.git
* cd ProxyFS

## How to run unit tests (in your Development Environment)

* Install/update to at least Go 1.8.3
* Ensure $GOPATH/bin is in your $PATH
* cd $GOPATH/src/github.com/NVIDIA/proxyfs
* make

## License

See LICENSE file
