# ProxyFS
File Server Proxy for Swift Clusters

## Synopsis

ProxyFS provides SMB and (eventually) NFS access to a Swift Cluster. ProxyFS additionally provides a hierarchical file system similar to POSIX requirements.

## Motivation

Swift is accessed via a RESTful API supporting CREATE (via HTTP PUT & HTTP POST), READ (via HTTP GET) and DELETE (via HTTP DELETE). Swift does not, however, provide UPDATE (e.g. via HTTP PATCH). This missing UPDATE capability provides a challenge in presenting Swift-managed data to users and applications expecting traditional file system semantics (e.g. via WebDAV). Further, many users and applications continue to require local file system behavior despite the data residing elsewhere via such protocols as SMB and NFS.

Swift presents a hierarchy consisting of Accounts and Containers. Within each Container, is what could be viewed as a non-hierarchical file system. This, too, is often inconvenient for many users and applications expecting to be able to create sub-directories and organise their data within a Container in a hierarchical way. Additionally, features like hard and soft links are often expected.

ProxyFS was created to extend the applicability of Swift Clusters to address such file system-oriented use cases.

## Installation

### Development

Make sure you've [installed and set up Go](https://golang.org/doc/install#install).

Choose a go base directory and set it to $GOPATH in your environment. Then:

    # after export GOPATH=/path/to/root
    cd $GOPATH
    mkdir -p src/github.com/swiftstack
    cd !$
    git clone git@github.com:swiftstack/ProxyFS
    cd ProxyFS
    ./regression_test.py -g

$ go test github.com/swiftstack/ProxyFS/fs

## API Reference

TBD

## Tests

TBD

## Contributors

 * balaji@swiftstack.com
 * ed@swiftstack.com
 * zdavis@swiftstack.com
 * kmalone@swiftstack.com
 * bschatz@swiftstack.com
 * orion@swiftstack.com

## License

TBD
