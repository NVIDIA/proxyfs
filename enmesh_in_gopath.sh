#!/bin/bash

# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

#  This script takes a clean checkout of the ProxyFS repo, turns it into a
#  working directory (e.g., enmeshes it into src/github.com/swiftstack/Proxyfs)
#  It is intended for use by CI/CD systems.

set -e
shopt -s extglob

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SWIFTSTACK_DIR=src/github.com/swiftstack
REPO_CLONE_PARENT_DIR=$ROOT_DIR/$SWIFTSTACK_DIR
PROXYFS_DIR=$REPO_CLONE_PARENT_DIR/ProxyFS

if [ ! -d $PROXYFS_DIR ]; then
    cd $ROOT_DIR
    mkdir -p $PROXYFS_DIR
    mv !(ci.sh|src) $PROXYFS_DIR

    # Ensure that the enmeshed dir is still usable as a git repo
    mv .git* $PROXYFS_DIR
    cp ci.sh $PROXYFS_DIR
fi
