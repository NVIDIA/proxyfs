#!/bin/bash

# WARNING:
# This script should be run after `ci.sh` has been run, with the proper file
# structure already in place. `ci.sh` should have placed this script outside
# the `src/github.com/swiftstack/ProxyFS` structure, and most probably it will
# have renamed this script as simply `install.sh`. Running this script from any
# other place will result in an error.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $SCRIPT_DIR/src/github.com/swiftstack/ProxyFS/ci/ansible
./install_proxyfs.sh runway
