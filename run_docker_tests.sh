#!/bin/bash
#
# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

IMAGE=$1

if [ -z "$IMAGE" ]
then
  echo "usage: $0 build|pull"
  exit 1
fi

if [ "$IMAGE" == "build" ]; then
    set -x
    # That's how you run it from scratch
    docker build -t proxyfs_unit_tests test/container
    docker run --cap-add SYS_ADMIN --device /dev/fuse -it -v `pwd`:/gopathroot/src/github.com/NVIDIA/proxyfs proxyfs_unit_tests
elif [ "$IMAGE" == "pull" ]; then
    set -x
    # That's how you run it using the image on Docker Hub
    docker run --cap-add SYS_ADMIN --device /dev/fuse -it -v `pwd`:/gopathroot/src/github.com/NVIDIA/proxyfs swiftstack/proxyfs_unit_tests
else
    echo "Bad argument: $IMAGE"
    exit 1
fi
