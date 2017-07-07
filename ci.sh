#!/bin/bash

#  This script takes a clean checkout of the ProxyFS repo, turns it into a
#  working directory (e.g., enmeshes it into src/github.com/swiftstack/Proxyfs)
#  and runs the unit tests on it.  It is intended for use by CI/CD systems.

shopt -s extglob

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SWIFTSTACK_DIR=src/github.com/swiftstack
PROXYFS_REPO=git@github.com:swiftstack/ProxyFS.git
CSTRUCT_REPO=git@github-cstruct:swiftstack/cstruct.git
SORTEDMAP_REPO=git@github-sortedmap:swiftstack/sortedmap.git
SAMBA_REPO=https://github.com/samba-team/samba.git
REPO_CLONE_PARENT_DIR=$ROOT_DIR/$SWIFTSTACK_DIR
PROXYFS_DIR=$REPO_CLONE_PARENT_DIR/ProxyFS
CSTRUCT_DIR=$REPO_CLONE_PARENT_DIR/cstruct
SORTEDMAP_DIR=$REPO_CLONE_PARENT_DIR/sortedmap
SAMBA_DIR=$REPO_CLONE_PARENT_DIR/samba
MIDDLEWARE_DIR=$PROXYFS_DIR/pfs_middleware

if [ ! -d $PROXYFS_DIR ]; then
    cd $ROOT_DIR
    mkdir -p $PROXYFS_DIR
    mv !(src) $PROXYFS_DIR
    mv .agignore $PROXYFS_DIR
    mv .git* $PROXYFS_DIR
fi

export GOPATH=$ROOT_DIR
export PATH=$GOPATH/bin:$PATH

cp $SWIFTSTACK_DIR/ProxyFS/ci/ansible/install_proxyfs_runway.sh $ROOT_DIR/install.sh

if [ ! -z "$DO_NOT_INSTALL" ]; then
    exit 0
fi

cd $REPO_CLONE_PARENT_DIR
git clone -b v4-3-stable --single-branch --depth 1 $SAMBA_REPO
cd $SAMBA_DIR
make clean
./configure
make GEN_NDR_TABLES

cd $REPO_CLONE_PARENT_DIR
git clone -b master --single-branch --depth 1 $CSTRUCT_REPO
cd $CSTRUCT_DIR
./regression_test.py

cd $REPO_CLONE_PARENT_DIR
git clone -b master --single-branch --depth 1 $SORTEDMAP_REPO
cd $SORTEDMAP_DIR
./regression_test.py

cd $PROXYFS_DIR
./regression_test.py --no-install

cd $MIDDLEWARE_DIR
echo "========================================"
# If this fails, it's almost certainly due to missing dependencies. If
# dependencies are missing, then "python setup.py test" will give you
# some opaque, unhelpful error like:
#
# AttributeError: 'module' object has no attribute 'test_pfs_middleware'.
#
# What it means is "importing test_pfs_middleware failed, but I'm not
# going to tell you why".
#
# When that happens, this import command will give you a
# probably-helpful stack trace so you can see what's missing.
echo "python -c \"import swift.common.utils\""
python -c "import swift.common.utils"
echo "========================================"

python setup.py test

exit $?
