# Copyright (c) 2015-2022, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# To build this image:
#
#   docker build                                      \
#          --target {base|buildable|dev|build|deploy} \
#          [--build-arg GolangVersion=<X.YY.Z>]       \
#          [--build-arg MakeTarget={|all|ci|minimal}] \
#          [-t <repository>[:<tag>]]                  .
#
#   Notes:
#     --target base:
#       1) provides a clean/small base image for the rest to leverage
#       2) only addition is libc6-compat to enable Golang runtime compatibility
#     --target dev:
#       1) builds an image capable of building all elements
#       2) /src is shared with context dir of host
#     --target build:
#       1) clones the context dir at /clone
#       2) performs a make clean to clear out non-source-controlled artifacts
#       3) performs a make $MakeTarget
#     --target deploy:
#       1) provides a minimal image containing ickpt, imgr, and iclient
#       2) also includes the icert tool useful for the following steps
#       3) creates TLS cert/key files for RootCA
#       4) creates TLS cert/key files for ickpt
#       5) creates TLS cert/key files for imgr
#     --build-arg GolangVersion:
#       1) identifies Golang version
#       2) default specified in ARG GolangVersion line in --target base
#     --build-arg MakeTarget:
#       1) identifies Makefile target(s) to build (following make clean)
#       2) defaults to blank (equivalent to "minimal")
#       3) only used in --target build
#     -t:
#       1) provides a name REPOSITORY:TAG for the built image
#       2) if no tag is specified, TAG will be "latest"
#       3) if no repository is specified, only the IMAGE ID will identify the built image
#
# To run the resultant image:
#
#   docker run                                                        \
#          [-d|--detach]                                              \
#          [-it]                                                      \
#          [--rm]                                                     \
#          [--cap-add SYS_ADMIN]                                      \
#          [--device /dev/fuse]                                       \
#          [--mount src="$(pwd)",target="/src",type=bind]             \
#          <image id>|<repository>[:<tag>]
#
#   Notes:
#     -d|--detach:   tells Docker to detach from running container 
#     -it:           tells Docker to run container interactively
#     --rm:          tells Docker to destroy container upon exit
#     --cap-add:
#       1) tells Docker to enable FUSE mounts
#       2) only useful for --target dev and --target iclient
#     --device:
#       1) tells Docker to grant access to /dev/fuse
#       2) only useful for --target dev and --target iclient
#     --mount:
#       1) bind mounts the context into /src in the container
#       2) /src will be a read-write'able equivalent to the context dir
#       3) only useful for --target dev

FROM ubuntu:22.04 as base

RUN    apt-get update \
    && apt-get upgrade -y

ARG TimeZone=America/Los_Angeles
RUN ln -snf /usr/share/zoneinfo/${TimeZone} /etc/localtime
RUN echo ${TimeZone} > /etc/timezone

RUN DEBIAN_FRONTEND="noninteractive" apt-get install -y tzdata

FROM base as buildable

RUN    apt-get update \
    && apt-get install -y \
                        build-essential \
                        curl \
                        dnsutils \
                        fuse \
                        git \
                        iputils-ping \
                        jq \
                        make \
                        protobuf-compiler \
                        s3cmd \
                        tree \
                        vim \
                        wget

ARG GolangVersion=1.22.0
ENV GolangBasename="go${GolangVersion}.linux-amd64.tar.gz"
ENV GolangURL="https://golang.org/dl/${GolangBasename}"
WORKDIR /tmp
RUN wget -nv ${GolangURL}
RUN tar -C /usr/local -xzf $GolangBasename
ENV PATH $PATH:/usr/local/go/bin
RUN git clone https://github.com/go-delve/delve
WORKDIR /tmp/delve
RUN go build github.com/go-delve/delve/cmd/dlv
RUN cp dlv /usr/local/go/bin/.
WORKDIR /

RUN echo '#!/bin/bash'                  >  /root/.bashrc_additions
RUN echo 'export PS1="\w$ "'            >> /root/.bashrc_additions
RUN echo 'export GOPATH=${HOME}/go'     >> /root/.bashrc_additions
RUN echo 'export GOBIN=${GOPATH}/bin'   >> /root/.bashrc_additions
RUN echo 'export PATH=${GOBIN}:${PATH}' >> /root/.bashrc_additions
RUN echo 'go env -w CGO_ENABLED=1'      >> /root/.bashrc_additions

RUN echo ""                      >> /root/.bashrc
RUN echo ". ~/.bashrc_additions" >> /root/.bashrc

FROM buildable as dev

VOLUME /src
WORKDIR /src
RUN git config --global --add safe.directory /src

FROM buildable as build

ARG MakeTarget
COPY . /clone
WORKDIR /clone
RUN git config --global --add safe.directory /clone
RUN make clean
RUN make $MakeTarget

FROM buildable as deploy

COPY --from=build /clone/iauth/iauth-swift/iauth-swift.so ./
COPY --from=build /clone/ickpt/ickpt                      ./
COPY --from=build /clone/ickpt/ickpt.conf                 ./
COPY --from=build /clone/icert/icert                      ./
COPY --from=build /clone/iclient/iclient                  ./
COPY --from=build /clone/iclient/iclient.conf             ./
COPY --from=build /clone/iclient/iclient.sh               ./
COPY --from=build /clone/imgr/imgr                        ./
COPY --from=build /clone/imgr/imgr.conf                   ./
RUN ./icert -ca -ed25519 -caCert ca_cert.pem -caKey ca_key.pem -commonName RootCA -ttl 3560
RUN ./icert     -ed25519 -caCert ca_cert.pem -caKey ca_key.pem -commonName ickpt  -ttl 3560 -cert ickpt_cert.pem -key ickpt_key.pem -dns ickpt
RUN ./icert     -ed25519 -caCert ca_cert.pem -caKey ca_key.pem -commonName imgr   -ttl 3560 -cert imgr_cert.pem  -key imgr_key.pem  -dns imgr
