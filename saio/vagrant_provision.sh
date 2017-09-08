#!/bin/bash

set -e
set -x

# Install Golang

sudo yum -y install gcc
cd /tmp
wget -q https://storage.googleapis.com/golang/go1.9.linux-amd64.tar.gz
sudo tar -C /usr/local -xf go1.9.linux-amd64.tar.gz
rm go1.9.linux-amd64.tar.gz
sudo echo "export PATH=\$PATH:/usr/local/go/bin" >> /etc/profile

# Install Python modules

sudo yum -y install epel-release
sudo yum -y install python-pip
sudo pip install --upgrade pip
sudo pip install requests

# Setup Samba - TODO (./configure)

# Setup ProxyFS build environment

sudo echo "export GOPATH=/vagrant" >> /etc/profile
sudo echo "export PATH=\$PATH:\$GOPATH/bin" >> /etc/profile
sudo echo "alias cdpfs=\"cd \$GOPATH/src/github.com/swiftstack/ProxyFS\"" >> /etc/profile
sudo yum -y install json-c-devel

# Setup Swift - TODO (2 replica-1 policies... in one drive)
