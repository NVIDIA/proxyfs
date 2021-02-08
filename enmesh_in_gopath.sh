#!/bin/bash

# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

#  This script takes a clean checkout of the ProxyFS repo, turns it into a
#  working directory (e.g., enmeshes it into src/github.com/NVIDIA/proxyfs)
#  It is intended for use by CI/CD systems.

set -e
shopt -s extglob

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
NVIDIA_DIR=src/github.com/NVIDIA
REPO_CLONE_PARENT_DIR=$ROOT_DIR/$NVIDIA_DIR
PROXYFS_DIR=$REPO_CLONE_PARENT_DIR/proxyfs

if [ ! -d $PROXYFS_DIR ]; then
    cd $ROOT_DIR
    mkdir -p $PROXYFS_DIR
    mv !(ci.sh|src) $PROXYFS_DIR

    # Ensure that the enmeshed dir is still usable as a git repo
    mv .git* $PROXYFS_DIR
    cp ci.sh $PROXYFS_DIR
fi
