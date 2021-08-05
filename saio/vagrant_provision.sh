#!/bin/bash
#
# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
#
# Note: This script assumes it is being run as root

set -e
set -x

# Enable core dumps
#
# Core files will be placed in /var/lib/systemd/coredump/
# Core files will be compressed with xz... use unxz to uncompress them
#
# To install the delve debugger, you will need to `go get -u github.com/go-delve/delve/cmd/dlv`
#  - Note that this will compete with the version of dlv installed for your host GOPATH
#  - As such, delve is not installed during provisioning
#  - Instead, an alias for the above, `gogetdlv`, would be issued as and when needed inside this VM

sed -i '/DefaultLimitCORE=/c\DefaultLimitCORE=infinity' /etc/systemd/system.conf

echo "kernel.core_pattern=| /usr/lib/systemd/systemd-coredump %p %u %g %s %t %c %e" > /etc/sysctl.d/90-override.conf
sysctl kernel.core_pattern='| /usr/lib/systemd/systemd-coredump %p %u %g %s %t %c %e'

echo "GOTRACEBACK=crash" >> /etc/environment

# Install yum-utils to deal with yum repos

yum -y install yum-utils

# Disable generic CentOS 7 repos

yum-config-manager --disable CentOS-Base
yum-config-manager --disable CentOS-CR
yum-config-manager --disable CentOS-Debuginfo
yum-config-manager --disable CentOS-fasttrack
yum-config-manager --disable CentOS-Media
yum-config-manager --disable CentOS-Sources
yum-config-manager --disable CentOS-Vault

rm -rf /etc/yum.repos.d/CentOS-Base.repo
rm -rf /etc/yum.repos.d/CentOS-CR.repo
rm -rf /etc/yum.repos.d/CentOS-Debuginfo.repo
rm -rf /etc/yum.repos.d/CentOS-fasttrack.repo
rm -rf /etc/yum.repos.d/CentOS-Media.repo
rm -rf /etc/yum.repos.d/CentOS-Sources.repo
rm -rf /etc/yum.repos.d/CentOS-Vault.repo

# Add and enable CentOS 7.4 repos

yum-config-manager --add-repo http://vault.centos.org/centos/7.4.1708/os/x86_64/
yum-config-manager --add-repo http://vault.centos.org/centos/7.4.1708/updates/x86_64/
yum-config-manager --add-repo http://vault.centos.org/centos/7.4.1708/extras/x86_64/
yum-config-manager --add-repo http://vault.centos.org/centos/7.4.1708/centosplus/x86_64/
yum-config-manager --enable vault.centos.org_centos_7.4.1708_os_x86_64_
yum-config-manager --enable vault.centos.org_centos_7.4.1708_updates_x86_64_
yum-config-manager --enable vault.centos.org_centos_7.4.1708_extras_x86_64_
yum-config-manager --enable vault.centos.org_centos_7.4.1708_centosplus_x86_64_

yum clean all

# Install tools needed above what's in a minimal base box

yum -y install wget git nfs-utils vim lsof

# Install Golang

yum -y --disableexcludes=all install glibc-commmon gcc
cd /tmp
TARFILE_NAME=go1.16.6.linux-amd64.tar.gz
wget -q https://dl.google.com/go/$TARFILE_NAME
tar -C /usr/local -xf $TARFILE_NAME
rm $TARFILE_NAME
echo "export PATH=\$PATH:/usr/local/go/bin" >> ~vagrant/.bash_profile

# Patch Golang's GDB runtime plug-in

mv /usr/local/go/src/runtime/runtime-gdb.py /usr/local/go/src/runtime/runtime-gdb.py_ORIGINAL
cp /vagrant/src/github.com/NVIDIA/proxyfs/saio/usr/local/go/src/runtime/runtime-gdb.py /usr/local/go/src/runtime/.

# Install GDB and enable above Golang GDB runtime plug-in as well as other niceties

yum -y install gdb
echo "add-auto-load-safe-path /usr/local/go/src/runtime/runtime-gdb.py" > /home/vagrant/.gdbinit
echo "set print thread-events off" >> /home/vagrant/.gdbinit
echo "set print pretty on" >> /home/vagrant/.gdbinit
echo "set print object on" >> /home/vagrant/.gdbinit
echo "set pagination off" >> /home/vagrant/.gdbinit
chown vagrant:vagrant /home/vagrant/.gdbinit
chmod 644 /home/vagrant/.gdbinit
cp /home/vagrant/.gdbinit /root/.

# Install Python 3.6

yum -y install centos-release-scl
yum -y install rh-python36
ln -s /opt/rh/rh-python36/root/bin/python3.6 /bin/python3.6
ln -s /bin/python3.6 /bin/python3
ln -s /opt/rh/rh-python36/root/usr/include /opt/rh/rh-python36/root/include

# Install Python pip

yum -y install epel-release
yum -y install python-pip
pip install --upgrade 'pip<21.0'

# Setup ProxyFS build environment

pip install requests
yum -y install json-c-devel
yum -y install fuse
echo "export GOPATH=/vagrant" >> ~vagrant/.bash_profile
echo "export PATH=\$PATH:\$GOPATH/bin" >> ~vagrant/.bash_profile
echo "alias cdpfs=\"cd \$GOPATH/src/github.com/NVIDIA/proxyfs\"" >> ~vagrant/.bash_profile
echo "alias goclean=\"go clean;go clean --cache;go clean --testcache\"" >> ~vagrant/.bash_profile
echo "alias gogetdlv=\"go get -u github.com/go-delve/delve/cmd/dlv\"" >> ~vagrant/.bash_profile
echo "user_allow_other" >> /etc/fuse.conf

# Install Python tox

pip install tox==3.5.3

# Setup Swift
#
# Guided by https://docs.openstack.org/swift/latest/development_saio.html

# [Setup Swift] Create the swift:swift user

useradd --user-group --groups wheel swift
chmod 755 ~swift

# Using loopback devices for storage

mkdir -p /srv

for x in 11 22 33 44 15 26 37 48 19 2A 3B 4C
do
    node=${x:0:1}
    drive=${x:1:1}
    truncate -s 0 /srv/swift-disk-$drive
    truncate -s 1GB /srv/swift-disk-$drive
    mkfs.xfs -f /srv/swift-disk-$drive
    mkdir -p /srv/$node/node/sdb$drive
    echo "/srv/swift-disk-$drive /srv/$node/node/sdb$drive xfs loop,noatime,nodiratime,nobarrier,logbufs=8 0 0" >> /etc/fstab
    mount /srv/$node/node/sdb$drive
    chown swift:swift /srv/$node/node/sdb$drive
done

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

yum -y install \
    memcached \
    sqlite \
    xfsprogs \
    libffi-devel \
    xinetd \
    openssl-devel \
    python-setuptools \
    python-coverage \
    python-devel \
    python-nose \
    pyxattr \
    python-eventlet \
    python-greenlet \
    python-paste-deploy \
    python-netifaces \
    python-pip \
    python-dns \
    python-mock

pip install --upgrade setuptools

# Build liberasure.so from source

cd ~swift
git clone https://github.com/openstack/liberasurecode.git
cd liberasurecode
yum install -y gcc make autoconf automake libtool
./autogen.sh
./configure
make
make install

# Install it where Python/PyECLib will see it

echo "/usr/local/lib" > /etc/ld.so.conf.d/liberasurecode.conf
ldconfig
# Alternatively, we could simply have done
#   ln -s /usr/local/lib/liberasurecode.so.1 /lib64/liberasurecode.so.1

# Install PyECLib from source

cd ~swift
git clone https://github.com/openstack/pyeclib.git
cd pyeclib
pip install -e .
pip install -r test-requirements.txt

# Install python-swiftclient from source & setup ENVs for its use

cd ~swift
git clone -b master --single-branch --depth 1 https://github.com/openstack/python-swiftclient.git
cd python-swiftclient
python setup.py develop

echo "export ST_AUTH=http://localhost:8080/auth/v1.0" >> ~vagrant/.bash_profile
echo "export ST_USER=test:tester" >> ~vagrant/.bash_profile
echo "export ST_KEY=testing" >> ~vagrant/.bash_profile

# Now we can actually install Swift from source

cd ~swift
git clone https://github.com/NVIDIA/swift.git
cd swift
export SWIFT_TAG="$( git describe --tags --abbrev=0 $(git rev-list --tags --max-count=1) )"
git checkout $SWIFT_TAG
pip install wheel
python setup.py bdist_wheel
yum remove -y python-greenlet
pip install --constraint py2-constraints.txt -r requirements.txt
python setup.py develop
# The following avoid dependency on pip-installed pyOpenSSL being newer than required
pip install python-openstackclient==3.12.0 python-glanceclient==2.7.0
# This is a temporary fix while bandit gets added to py2-constraints.txt
pip install bandit==1.6.2
pip install --constraint py2-constraints.txt -r test-requirements.txt

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

# [Setup Swift] Setting up memcached

systemctl enable memcached.service
systemctl start memcached.service

# [Setup Swift] Configuring each node

rm -rf /etc/swift
cp -R /vagrant/src/github.com/NVIDIA/proxyfs/saio/etc/swift /etc/swift
chown -R swift:swift /etc/swift

# [Setup Swift] Setting up scripts for running Swift

mkdir -p ~swift/bin

cd ~swift/bin
cp /vagrant/src/github.com/NVIDIA/proxyfs/saio/home/swift/bin/* .
echo "export PATH=\$PATH:~swift/bin" >> ~vagrant/.bash_profile

~swift/bin/remakerings

# Install ProxyFS's pfs_middleware into the "normal" Swift Proxy pipeline

cd /vagrant/src/github.com/NVIDIA/proxyfs/pfs_middleware
python setup.py develop

# Install ProxyFS's meta_middleware into the "NoAuth" Swift Proxy pipeline

cd /vagrant/src/github.com/NVIDIA/proxyfs/meta_middleware
python setup.py develop

# Setup AWS access for local vagrant user

pip install awscli-plugin-endpoint
mkdir -p ~vagrant/.aws
cat > ~vagrant/.aws/config << EOF
[plugins]
endpoint = awscli_plugin_endpoint

[default]
s3 =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 64MB
     multipart_chunksize = 16MB
s3api =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 64MB
     multipart_chunksize = 16MB
EOF
cat > ~vagrant/.aws/credentials << EOF
[default]
aws_access_key_id = test:tester
aws_secret_access_key = testing
EOF
chown -R vagrant:vagrant ~vagrant/.aws

# Ensure proxyfsd logging will work

rm -rf /var/log/proxyfsd
mkdir -p /var/log/proxyfsd
touch /var/log/proxyfsd/proxyfsd.log
chmod 777 /var
chmod 777 /var/log
chmod 777 /var/log/proxyfsd
chmod 666 /var/log/proxyfsd/proxyfsd.log

# Create Mount Points for ProxyFS (embedded FUSE)

rm -rf /CommonMountPoint
mkdir /CommonMountPoint
chmod 777 /CommonMountPoint

# Install systemd .service files for ProxyFS

cp /vagrant/src/github.com/NVIDIA/proxyfs/saio/usr/lib/systemd/system/proxyfsd.service /usr/lib/systemd/system/.
cp /vagrant/src/github.com/NVIDIA/proxyfs/saio/usr/lib/systemd/system/pfsagentd.service /usr/lib/systemd/system/.

# Place symlink in root's $PATH to locate pfsagentd-swift-auth-plugin referenced without a path

ln -s /vagrant/bin/pfsagentd-swift-auth-plugin /usr/bin/pfsagentd-swift-auth-plugin

# Enable Samba service in an SELinux environment

yum -y install policycoreutils-python
semanage port -a -t smbd_port_t -p tcp 12345
semanage port -a -t smbd_port_t -p tcp 32345

# Enable start/stop tools

echo "export PATH=\$PATH:/vagrant/src/github.com/NVIDIA/proxyfs/saio/bin" >> ~vagrant/.bash_profile

# Install wireshark

yum -y install wireshark-gnome \
               xorg-x11-fonts-Type1 \
               xorg-x11-xauth \
               xeyes
echo "X11Forwarding yes" >> /etc/sysconfig/sshd
systemctl restart sshd
usermod -aG wireshark vagrant

# Install benchmark support tools

yum -y install atop-2.3.0-8.el7 bc fio gawk

# Install ssh helper

yum -y install sshpass-1.06-2.el7

# Install dstat

yum -y install dstat

# Install tree

yum -y install tree

# Install jq... a very handy JSON parser

yum -y install jq

# Install and configure a localhost-only one-node etcd cluster

ETCD_VERSION=3.4.7
wget https://github.com/etcd-io/etcd/releases/download/v${ETCD_VERSION}/etcd-v${ETCD_VERSION}-linux-amd64.tar.gz
tar xzf etcd-v${ETCD_VERSION}-linux-amd64.tar.gz
rm -rf etcd-v${ETCD_VERSION}-linux-amd64.tar.gz
install -C -m 755 etcd-v${ETCD_VERSION}-linux-amd64/etcd /usr/local/bin/
install -C -m 755 etcd-v${ETCD_VERSION}-linux-amd64/etcdctl /usr/local/bin/
rm -rf etcd-v${ETCD_VERSION}-linux-amd64

mkdir /etcd

cat > /etc/systemd/system/proxyfs-etcd.service << EOF
[Unit]
Description=ProxyFS etcd instance
After=network.target
StartLimitIntervalSec=0

[Service]
Type=simple
Restart=always
RestartSec=1
User=root
ExecStart=/usr/local/bin/etcd --name proxyfs --data-dir /etcd/proxyfs.etcd --initial-advertise-peer-urls http://localhost:2380 --listen-peer-urls http://localhost:2380 --listen-client-urls http://localhost:2379 --advertise-client-urls http://localhost:2379 --initial-cluster-token etcd-cluster --initial-cluster default=http://localhost:2380 --initial-cluster-state new

[Install]
WantedBy=multi-user.target
EOF

# Inform systemd that we've updated .service files

systemctl daemon-reload

# All done

echo "SAIO for ProxyFS provisioned"
