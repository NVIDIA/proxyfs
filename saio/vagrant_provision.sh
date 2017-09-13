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
echo "export PATH=\$PATH:/usr/local/go/bin" >> ~vagrant/.bash_profile

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
echo "export GOPATH=/vagrant" >> ~vagrant/.bash_profile
echo "export PATH=\$PATH:\$GOPATH/bin" >> ~vagrant/.bash_profile
echo "alias cdpfs=\"cd \$GOPATH/src/github.com/swiftstack/ProxyFS\"" >> ~vagrant/.bash_profile
echo "user_allow_other" >> /etc/fuse.conf

# Setup Samba

yum -y install gcc \
               gcc-c++ \
               python-devel \
               gnutls-devel \
               libacl-devel \
               openldap-devel \
               samba \
               samba-client \
               cifs-utils
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
echo "export SAMBA_SOURCE=\$GOPATH/src/github.com/swiftstack/ProxyFS/saio/samba" >> ~vagrant/.bash_profile

# Setup Swift
#
# Guided by https://docs.openstack.org/swift/latest/development_saio.html

# [Setup Swift] Create the swift:swift user

useradd --user-group --groups wheel swift
chmod 755 ~swift

# Using a loopback device for storage

mkdir -p /srv
truncate -s 1GB /srv/swift-disk
mkfs.xfs -f /srv/swift-disk
echo "/srv/swift-disk /mnt/sdb1 xfs loop,noatime,nodiratime,nobarrier,logbufs=8 0 0" >> /etc/fstab
mkdir -p /mnt/sdb1
mount /mnt/sdb1
sudo mkdir /mnt/sdb1/1 /mnt/sdb1/2 /mnt/sdb1/3 /mnt/sdb1/4
chown swift:swift /mnt/sdb1/*
for x in {1..4}; do ln -s /mnt/sdb1/$x /srv/$x; done
mkdir -p /srv/1/node/sdb1 /srv/1/node/sdb5 \
         /srv/2/node/sdb2 /srv/2/node/sdb6 \
         /srv/3/node/sdb3 /srv/3/node/sdb7 \
         /srv/4/node/sdb4 /srv/4/node/sdb8 \
         /var/run/swift
chown -R swift:swift /var/run/swift
for x in {1..4}; do chown -R swift:swift /srv/$x/; done

# [Setup Swift] Common Post-Device Setup (Add /var boot-time provisioning to /etc/rc.d/rc.local)

echo "mkdir -p /var/cache/swift /var/cache/swift2 /var/cache/swift3 /var/cache/swift4" >> /etc/rc.d/rc.local
echo "chown swift:swift /var/cache/swift*" >> /etc/rc.d/rc.local
echo "mkdir -p /var/run/swift" >> /etc/rc.d/rc.local
echo "chown swift:swift /var/run/swift" >> /etc/rc.d/rc.local
chmod +x /etc/rc.d/rc.local

# [Setup Swift] Do boot-time provisioning now... as if we just booted

mkdir -p /var/cache/swift /var/cache/swift2 /var/cache/swift3 /var/cache/swift4
chown swift:swift /var/cache/swift*
mkdir -p /var/run/swift
chown swift:swift /var/run/swift

# [Setup Swift] Getting the code

yum -y install memcached sqlite xfsprogs \
               libffi-devel xinetd liberasurecode-devel \
               openssl-devel python-setuptools \
               python-coverage python-devel python-nose \
               pyxattr python-eventlet \
               python-greenlet python-paste-deploy \
               python-netifaces python-pip python-dns \
               python-mock

yum -y install http://www.rpmfind.net/linux/fedora/linux/releases/25/Everything/x86_64/os/Packages/l/liberasurecode-1.1.1-1.fc25.x86_64.rpm
yum -y install http://www.rpmfind.net/linux/fedora/linux/releases/25/Everything/x86_64/os/Packages/l/liberasurecode-devel-1.1.1-1.fc25.x86_64.rpm

cd ~swift
git clone -b master --single-branch --depth 1 https://github.com/openstack/python-swiftclient.git
cd python-swiftclient
python setup.py develop

cd ~swift
git clone -b master --single-branch --depth 1 https://github.com/openstack/swift.git
cd swift
pip install --no-binary cryptography -r requirements.txt
python setup.py develop
pip install -r test-requirements.txt

# [Setup Swift] Setting up rsync

cd /etc
cp ~swift/swift/doc/saio/rsyncd.conf .
sed -i "s/<your-user-name>/swift/" rsyncd.conf

cd /etc/xinetd.d
echo "disable = no" >> rsync

systemctl restart xinetd.service
systemctl enable rsyncd.service
systemctl start rsyncd.service

rsync rsync://pub@localhost/

# [Setup Swift] Setting up rsync

systemctl enable memcached.service
systemctl start memcached.service

# [Setup Swift] Configuring each node

rm -rf /etc/swift
cp -R /vagrant/src/github.com/swiftstack/ProxyFS/saio/etc/swift /etc/swift
chown -R swift:swift /etc/swift

# [Setup Swift] Setting up scripts for running Swift

mkdir -p ~swift/bin

cd ~swift/bin
cp /vagrant/src/github.com/swiftstack/ProxyFS/saio/home/swift/bin/* .
echo "export PATH=\$PATH:~swift/bin" >> ~vagrant/.bash_profile

~swift/bin/remakerings

# Ensure proxyfsd logging will work

rm -rf /var/log/proxyfsd
mkdir -p /var/log/proxyfsd
touch /var/log/proxyfsd/proxyfsd.log
chmod 777 /var
chmod 777 /var/log
chmod 777 /var/log/proxyfsd
chmod 666 /var/log/proxyfsd/proxyfsd.log

# Create Mount Points for ProxyFS (FUSE, NFS, & SMB)

rm -rf /CommonMountPoint
mkdir /CommonMountPoint
chmod 777 /CommonMountPoint

rm -rf /mnt/nfs_proxyfs_mount
mkdir /mnt/nfs_proxyfs_mount
chmod 777 /mnt/nfs_proxyfs_mount

rm -rf /mnt/smb_proxyfs_mount
mkdir /mnt/smb_proxyfs_mount
chmod 777 /mnt/smb_proxyfs_mount

# Install systemd .service files for ProxyFS

cp /vagrant/src/github.com/swiftstack/ProxyFS/saio/usr/lib/systemd/system/proxyfsd.service /usr/lib/systemd/system/.

# Enable start/stop tools

echo "export PATH=\$PATH:/vagrant/src/github.com/swiftstack/ProxyFS/saio/bin" >> ~vagrant/.bash_profile

# All done

echo "SAIO for ProxyFS provisioned"
