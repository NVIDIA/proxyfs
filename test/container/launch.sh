#!/bin/bash

set -e
set -x

# Setup Samba
# Ideally, these 3 vars should be populated with info from the system.
# For now, it's just hardcoded.
OS_DISTRO=centos
OS_DISTRO_VERSION=7.4
SAMBA_VERSION=4.6.12

SAMBA_DIR=build-samba-`echo $SAMBA_VERSION | tr . -`-${OS_DISTRO}-`echo $OS_DISTRO_VERSION | tr . -`

cd $GOPATH/src/github.com/swiftstack/ProxyFS/vfs

if [[ -L samba ]]; then
    rm samba
else
    if [[ -e samba ]]; then
        echo "non-symlink \$GOPATH/src/github.com/swiftstack/ProxyFS/vfs/samba cannot pre-exist"
        exit 1
    fi
fi

if [[ -d "${SAMBA_DIR}" ]]; then
    ln -s $SAMBA_DIR samba
else
    git clone -b samba-$SAMBA_VERSION --single-branch --depth 1 https://github.com/samba-team/samba.git $SAMBA_DIR
    ln -s $SAMBA_DIR samba
    cd samba
    ./configure
    make clean
    make GEN_NDR_TABLES
fi
export SAMBA_SOURCE=$GOPATH/src/github.com/swiftstack/ProxyFS/vfs/samba

# Build ProxyFS and run tests
cd $GOPATH/src/github.com/swiftstack/ProxyFS
make ci
# $COVERALLS_TOKEN must be configured in TravisCI
if [ -n "$COVERALLS_TOKEN" ] && [ -n "$TRAVIS_BRANCH" ]; then
    GIT_BRANCH=$TRAVIS_BRANCH goveralls -coverprofile coverage.coverprofile -service travis-ci -repotoken $COVERALLS_TOKEN
fi
