#!/bin/bash

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
    docker run -it -v `pwd`:/gopathroot/src/github.com/swiftstack/ProxyFS proxyfs_unit_tests
elif [ "$IMAGE" == "pull" ]; then
    set -x
    # That's how you run it using the image on Docker Hub
    docker run -it -v `pwd`:/gopathroot/src/github.com/swiftstack/ProxyFS swiftstack/proxyfs_unit_tests
else
    echo "Bad argument: $IMAGE"
    exit 1
fi
