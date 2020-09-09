# PFSAgent Init Daemon For Use In Containers

Docker-style containers are designed to run a single program. If that program
has a dependency on running services, such as `pfsagentd`, there is no opportunity
to ensure these are running with a subsystem like `systemd`. While there are a
number of `systemd` substitutes in wide use in containers that have this service
requirement (e.g. `supervisord` and `s6`), these tools generally don't have a
mechanism to order such services nor ensure that each is up and running sufficient
for the subsequent service or the application to immediately consume.

The program in this directory, `pfsagentd-init`, provides the capability to launch
a number of instances of `pfsagentd`, ensures that they are able to receive traffic
via their `FUSEMountPointPath`, and finally launching the desired application.

As may be common, the application to be dependent upon one or more `pfsagentd`
instances has already been containerized in some existing container image.
A `Dockerfile` in `../container/build` is provided to build the components that
need to be added to a derived container image based off the application's original
container image. Then, a tool in `../container/insert` will create a `Dockerfile`
derived from the application's original container image that imports these
to-be-inserted components.

As part of the created `Dockerfile` is a replacement for any ENTRYPOINT and/or
CMD that will launch `pfsagentd-init` that accomplishes this conversion.
