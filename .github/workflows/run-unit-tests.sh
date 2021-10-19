#!/bin/bash

# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e
set -x

cd /gopathroot/src/github.com/NVIDIA/proxyfs

go get github.com/mattn/goveralls

make ci

# $COVERALLS_TOKEN must be configured in the CI/CD environment
if [ -n "$COVERALLS_TOKEN" ] && [ -n "$GIT_BRANCH" ]; then
    goveralls -coverprofile coverage.coverprofile -service travis-ci -repotoken $COVERALLS_TOKEN || true
fi
