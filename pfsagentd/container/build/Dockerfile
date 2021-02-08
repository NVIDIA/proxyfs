# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

FROM centos

ARG GolangVersion=1.15.5
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

RUN mkdir -p $GOPATH/src/github.com/NVIDIA
WORKDIR $GOPATH/src/github.com/NVIDIA
RUN git clone https://github.com/NVIDIA/proxyfs.git
WORKDIR $GOPATH/src/github.com/NVIDIA/proxyfs
RUN git checkout $ProxyFS_Version

RUN make version pfsagent

WORKDIR /opt/PFSAgent

# To build this image:
#
#   docker build \
#          [--build-arg GolangVersion=<major#.minor#[.patch#]>]      \
#          [--build-arg ProxyFS_Version=<branch-name-or-tag-or-SHA>] \
#          [-t <repository>:<tag>]                                   .
