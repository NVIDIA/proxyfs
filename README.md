[![Build Status](https://github.com/NVIDIA/proxyfs/actions/workflows/run-unit-tests.yml/badge.svg)](https://github.com/NVIDIA/proxyfs/actions/workflows/run-unit-tests.yml?query=branch%3Adevelopment)
[![Coverage Status](https://coveralls.io/repos/github/swiftstack/ProxyFS/badge.svg?branch=development)](https://coveralls.io/github/swiftstack/ProxyFS?branch=development)

# ProxyFS
Integrated File and Object Access for Swift Object Storage

## Synopsis

ProxyFS is a disributed read/write hierarchical POSIX file system layered on top of 
[Swift object storage](http://swift.openstack.org).

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

* git clone git@github.com:NVIDIA/proxyfs.git
* cd proxyfs

## How to run unit tests (in your Development Environment)

* [Host shell] docker compose build
* [Host shell] docker compose up -d dev
* [dev  shell] make
* [dev  shell] make test

## License

See LICENSE file
