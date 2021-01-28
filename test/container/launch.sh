#!/bin/bash

# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e
set -x

# Build ProxyFS and run tests
cd $GOPATH/src/github.com/swiftstack/ProxyFS
make ci
# $COVERALLS_TOKEN must be configured in TravisCI
if [ -n "$COVERALLS_TOKEN" ] && [ -n "$TRAVIS_BRANCH" ]; then
    GIT_BRANCH=$TRAVIS_BRANCH goveralls -coverprofile coverage.coverprofile -service travis-ci -repotoken $COVERALLS_TOKEN
fi
