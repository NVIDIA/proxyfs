# ProxyFS
File Server Proxy for Swift Clusters

## Synopsis

ProxyFS provides SMB and (eventually) NFS access to a Swift Cluster. ProxyFS additionally provides a hierarchical file system similar to POSIX requirements.

## Motivation

Swift is accessed via a RESTful API supporting CREATE (via HTTP PUT & HTTP POST), READ (via HTTP GET) and DELETE (via HTTP DELETE). Swift does not, however, provide UPDATE (e.g. via HTTP PATCH). This missing UPDATE capability provides a challenge in presenting Swift-managed data to users and applications expecting traditional file system semantics (e.g. via WebDAV). Further, many users and applications continue to require local file system behavior despite the data residing elsewhere via such protocols as SMB and NFS.

Swift presents a hierarchy consisting of Accounts and Containers. Within each Container, is what could be viewed as a non-hierarchical file system. This, too, is often inconvenient for many users and applications expecting to be able to create sub-directories and organise their data within a Container in a hierarchical way. Additionally, features like hard and soft links are often expected.

ProxyFS was created to extend the applicability of Swift Clusters to address such file system-oriented use cases.

## Development

Make sure you've [installed and set up Go](https://golang.org/doc/install#install).

Choose a go base directory and set it to $GOPATH in your environment. Then:

    # after export GOPATH=/path/to/root
    cd $GOPATH
    mkdir -p src/github.com/swiftstack
    cd !$
    git clone git@github.com:swiftstack/ProxyFS
    cd ProxyFS
    git submodule update --init --recursive

If updating from a pre-submodule ProxyFS repo:

    cd $GOPATH/src/github.com/swiftstack/ProxyFS
    git pull
    git checkout development
    rm -rf jrpcclient
    rm -rf vfs
    git submodule update --init --recursive

To work on files in the ProxyFS repo:

    cd $GOPATH/src/github.com/swiftstack/ProxyFS
    git pull
    git checkout development
    ...

To work on files in the vfs (similar steps for jrpcclient) submodule repo:

    cd $GOPATH/src/github.com/swiftstack/ProxyFS/vfs
    git pull
    git checkout master
    ...

To check-in changes in the vfs (similar steps for jrpcclient) submodule repo:

    cd $GOPATH/src/github.com/swiftstack/ProxyFS/vfs
    git commit <your changes>
    cd ..
    git commit <updated vfs dir/submodule tag>

To push your changes to the vfs (similar steps for jrpcclient) submodule to origin:

    cd $GOPATH/src/github.com/swiftstack/ProxyFS/vfs
    git push
    cd ..
    git push

To restore your submodules to their "(HEAD detached at ...)" branch:

    cd $GOPATH/src/github.com/swiftstack/ProxyFS
    git pull
    git checkout development
    git submodule update --recursive

To build/test/install ProxyFS:

    cd $GOPATH/src/github.com/swiftstack/ProxyFS
    ./regression_test.py

## Contributors

 * balaji@swiftstack.com
 * bschatz@swiftstack.com
 * charmer@swiftstack.com
 * ed@swiftstack.com
 * kmalone@swiftstack.com
 * orion@swiftstack.com
 * zdavis@swiftstack.com

## License

See LICENSE file
