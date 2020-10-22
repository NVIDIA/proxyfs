FROM centos

ARG GolangVersion=1.13.6
ARG ProxyFS_Version=stable

ENV GolangBasename "go${GolangVersion}.linux-amd64.tar.gz"
ENV GolangURL      "https://golang.org/dl/${GolangBasename}"

RUN yum install -y fuse gcc git make python2 tar wget
RUN ln -s /usr/bin/python2 /usr/bin/python

WORKDIR /opt/PFSAgent

RUN wget -nv $GolangURL
RUN tar -C /usr/local -xzf $GolangBasename

ENV GOPATH /opt/PFSAgent/GOPATH
ENV PATH   $PATH:/usr/local/go/bin:$GOPATH/bin

RUN mkdir -p $GOPATH/src/github.com/swiftstack
WORKDIR $GOPATH/src/github.com/swiftstack
RUN git clone https://github.com/swiftstack/ProxyFS.git
WORKDIR $GOPATH/src/github.com/swiftstack/ProxyFS
RUN git checkout $ProxyFS_Version

RUN make version pfsagent

WORKDIR /opt/PFSAgent

# To build this image:
#
#   docker build \
#          [--build-arg GolangVersion=<major#.minor#[.patch#]>]      \
#          [--build-arg ProxyFS_Version=<branch-name-or-tag-or-SHA>] \
#          [-t <repository>:<tag>]                                   .
