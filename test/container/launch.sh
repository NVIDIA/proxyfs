#!/bin/bash

set -e
set -x

# Yum install. Trying to do as much as possible at once.
# Here's an explanation of why we need to install each package:
## Install Golang
# -  gcc
# -  wget
## Install Python pip
# -  epel-release
# -  python-pip
## Setup ProxyFS build environment
# -  sudo
# -  json-c-devel
# -  fuse
## Install liberasurecode
# -  http://www.rpmfind.net/linux/fedora/linux/releases/25/Everything/x86_64/os/Packages/l/liberasurecode-1.1.1-1.fc25.x86_64.rpm
# -  http://www.rpmfind.net/linux/fedora/linux/releases/25/Everything/x86_64/os/Packages/l/liberasurecode-devel-1.1.1-1.fc25.x86_64.rpm
## Setup Samba
# -  git \
# -  gcc-c++-4.8.5-16.el7_4.1 \
# -  python-devel-2.7.5-58.el7 \
# -  gnutls-devel-3.3.26-9.el7 \
# -  libacl-devel-2.2.51-12.el7 \
# -  openldap-devel-2.4.44-5.el7 \
# -  samba-4.6.2-12.el7_4 \
# -  samba-client-4.6.2-12.el7_4 \
# -  cifs-utils-6.2-10.el7 \
yum -y --disableexcludes=all install gcc
yum -y install \
    wget \
    epel-release \
    sudo \
    json-c-devel \
    fuse \
    http://www.rpmfind.net/linux/fedora/linux/releases/25/Everything/x86_64/os/Packages/l/liberasurecode-1.1.1-1.fc25.x86_64.rpm \
    http://www.rpmfind.net/linux/fedora/linux/releases/25/Everything/x86_64/os/Packages/l/liberasurecode-devel-1.1.1-1.fc25.x86_64.rpm \
    git \
    gcc-c++-4.8.5-16.el7_4.1 \
    python-devel-2.7.5-58.el7 \
    gnutls-devel-3.3.26-9.el7 \
    libacl-devel-2.2.51-12.el7 \
    openldap-devel-2.4.44-5.el7 \
    samba-4.6.2-12.el7_4 \
    samba-client-4.6.2-12.el7_4 \
    cifs-utils-6.2-10.el7
yum -y install python-pip

# Pip install. Trying to do as much as possible at once.
# Here's an explanation of why we need to install each package:
## Setup ProxyFS build environment
# - requests
## Install Python tox
# - tox
pip install --upgrade pip
pip install \
    requests \
    tox

# Install Golang
cd /tmp
wget -q https://storage.googleapis.com/golang/go1.8.3.linux-amd64.tar.gz
tar -C /usr/local -xf go1.8.3.linux-amd64.tar.gz
rm -rf go1.8.3.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin

# Setup ProxyFS build environment
export GOPATH=/gopathroot
export PATH=$PATH:$GOPATH/bin
echo "user_allow_other" >> /etc/fuse.conf

# Setup Samba
cd $GOPATH/src/github.com/swiftstack/ProxyFS/vfs
git clone -b samba-4.6.12 --single-branch --depth 1 https://github.com/samba-team/samba.git samba4-6-12-centos
ln -s samba4-6-12-centos samba
cd samba
./configure
make clean
make GEN_NDR_TABLES
export SAMBA_SOURCE=$GOPATH/src/github.com/swiftstack/ProxyFS/vfs/samba

# Build ProxyFS and run tests
cd $GOPATH/src/github.com/swiftstack/ProxyFS
make
