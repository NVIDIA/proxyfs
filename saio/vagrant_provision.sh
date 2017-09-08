#!/bin/bash
#
# Note: This script assumes it is being run as root

set -e
set -x

# Install Golang

yum -y install gcc
cd /tmp
wget -q https://storage.googleapis.com/golang/go1.9.linux-amd64.tar.gz
tar -C /usr/local -xf go1.9.linux-amd64.tar.gz
rm go1.9.linux-amd64.tar.gz
echo "export PATH=\$PATH:/usr/local/go/bin" >> /etc/profile

# Install Git

yum -y install git

# Install Python pip

yum -y install epel-release
yum -y install python-pip
pip install --upgrade pip

# Setup ProxyFS build environment

pip install requests
yum -y install json-c-devel
yum -y install fuse
echo "export GOPATH=/vagrant" >> /etc/profile
echo "export PATH=\$PATH:\$GOPATH/bin" >> /etc/profile
echo "alias cdpfs=\"cd \$GOPATH/src/github.com/swiftstack/ProxyFS\"" >> /etc/profile
echo "user_allow_other" >> /etc/fuse.conf

# Setup Samba

yum -y install gcc
yum -y install gcc-c++
yum -y install python-devel
yum -y install gnutls-devel
yum -y install libacl-devel
yum -y install openldap-devel
yum -y install samba
yum -y install samba-client
yum -y install cifs-utils
cd /vagrant/src/github.com/swiftstack/ProxyFS/saio
if [[ -d samba4-4-centos ]]
then
    if [[ -L samba4-4-centos ]]
    then
        echo "non-directory symlink \$GOPATH/src/github.com/swiftstack/ProxyFS/saio/samba4-4-centos cannot pre-exist"
        exit 1
    else
        echo "\$GOPATH/src/github.com/swiftstack/ProxyFS/saio/samba4-4-centos assumed to be as desired"
    fi
else
    if [[ -L samba4-4-centos ]]
    then
        echo "non-directory symlink \$GOPATH/src/github.com/swiftstack/ProxyFS/saio/samba4-4-centos cannot pre-exist"
        exit 1
    else
        git clone -b v4-4-stable --single-branch --depth 1 https://github.com/samba-team/samba.git samba4-4-centos
    fi
fi
if [[ -L samba ]]
then
    samba_symlink_target=`readlink "samba"`
    if [[ "samba4-4-centos" == "$samba_symlink_target" ]]
    then
        echo "symlink samba -> samba4-4-centos already"
    else
        echo "symlink samba must point to samba4-4-centos"
        exit 1
    fi
else
    if [[ -e samba ]]
    then
        echo "non-symlink \$GOPATH/src/github.com/swiftstack/ProxyFS/saio/samba cannot pre-exist"
        exit 1
    else
        ln -s samba4-4-centos samba
    fi
fi
cd samba
if [[ -d bin ]]
then
    echo "./configure has already been run"
else
    ./configure
fi
if [[ -f bin/default/librpc/gen_ndr/ndr_smb_acl.h ]]
then
    echo "make GEN_NDR_TABLES has already been run"
else
    make GEN_NDR_TABLES
fi
echo "export SAMBA_SOURCE=\$GOPATH/src/github.com/swiftstack/ProxyFS/saio/samba" >> /etc/profile

# Setup Swift - TODO (2 replica-1 policies... in one drive)

# All done

echo "SAIO for ProxyFS provisioned"
