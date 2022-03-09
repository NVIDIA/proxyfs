[![Build Status](https://github.com/NVIDIA/proxyfs/actions/workflows/run-unit-tests.yml/badge.svg)](https://github.com/NVIDIA/proxyfs/actions/workflows/run-unit-tests.yml?query=branch%3Adevelopment)
[![Coverage Status](https://coveralls.io/repos/github/swiftstack/ProxyFS/badge.svg?branch=development)](https://coveralls.io/github/swiftstack/ProxyFS?branch=development)

# ProxyFS
Provides a POSIX compliant file system mount of an OpenStack Swift Object Storage Container

## Synopsis

ProxyFS is a disributed read/write hierarchical POSIX file system layered on top of 
[Swift object storage](http://swift.openstack.org). File system accesses map to Objects
inside a Swift Container.

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

## Docker-based Development Environment

ProxyFS develoment/building leverages Docker Containers and Docker Compose.

A Multi-Stage `Dockerfile` is provided that builds the following three images:

* `dev` - used to build, debug, and unit test a simple ProxyFS deployment
* `build` - used to provide a clean build environment for constructing `deploy`
* `deploy` - a space/dependency efficient deployable Docker Container holding:
  * `ickpt` - an optional, replicable, strictly consistent service to store ProxyFS checkpoints that are, otherwise, the only element of a ProxyFS file system subject to the Eventual Consistency risk of using OpenStack Swift as a storage back-end
  * `imgr` - a service capable of managing the metadata for a collection of ProxyFS file systems
  * `iclient` - the FUSE service deployed to support the mounting of a ProxyFS file system

To build the images:

* docker-compose build

To instantiate either a development or test environment, a `docker-compose.yml` is provided that instantiates the following Docker Containers:

* `swift` - a deployment of a Swift "All-in-One" Container instance
* `dev` - an instance of the `dev` image with your ProxyFS repo dir mapped to `/src`
* `ickpt` - an instance of the `deploy` image that launches the `ickpt` service
* `imgr` - an instance of the `deploy` image that launches the `imgr` service
* `iclient` - an instance of the `deploy` image that launches the `iclient` service setup to present its FUSE mount point at `/mnt`

To kick off development activities:

* [Host shell] docker-compose up -d dev
* [Host shell] docker-compose exec dev sh

To build all the images:
* [`dev` /src#] make

To clear out prior launches and run the processes in the background:
* [`dev` /src#] rm -rf /tmp/ickptDB
* [`dev` /src#] ickpt/ickpt ickpt/dev.conf &
* [`dev` /src#] imgr/imgr imgr/dev.conf &
* [`dev` /src#] idestroy/idestroy iclient/dev.conf &
* [`dev` /src#] imgr/mkmount.sh -fs
* [`dev` /src#] iclient/iclient iclient/dev.conf &

Notes:
* To relaunch the daemons without reformatting, skip the `rm -rf /tmp/ickptDB` and `idestroy...` steps
* `idestroy` step will fail with `httpGETResponse.Status unexpected: 404 Not Found` if this is the fist iteration through 
* `imgr` and `iclient` will be logging to $StdOut in this example launching
* Each of the above can be terminated by delivering a SIGINT or SIGTERM to their processes

For a more appropriate environment in which to perform functional testing, the `docker-compose.yml` file my also be used to launch the suite of Docker Containers:

* [Host shell] docker-compose up -d iclient
* [Host shell] docker-compose exec iclient sh

At this point, you have a separate Docker Container running each of the ProxyFS services. Inside the `iclient` Docker Container, the `/mnt` directory will be the FUSE mount point to your ProxyFS file system.

For either the up cases (i.e. `dev` or `iclient`), both the `imgr` and `iclient` services provide an HTTP endpoint that, due to the port mapping specified in `docker-compose.yml`, should be reachable from a browser in the host:

* `imgr` - http://localhost:15346/
* `iclient` - http://localhost:15347/

## License

See LICENSE file
