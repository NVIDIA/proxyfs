#!/bin/bash
#
# Note: This script assumes it is being run as root

set -e
set -x

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

yum -y --disableexcludes=all install gcc
cd /tmp
TARFILE_NAME=go1.11.4.linux-amd64.tar.gz
wget -q https://dl.google.com/go/$TARFILE_NAME
tar -C /usr/local -xf $TARFILE_NAME
rm $TARFILE_NAME
echo "export PATH=\$PATH:/usr/local/go/bin" >> ~vagrant/.bash_profile

# Patch Golang's GDB runtime plug-in

mv /usr/local/go/src/runtime/runtime-gdb.py /usr/local/go/src/runtime/runtime-gdb.py_ORIGINAL
cp /vagrant/src/github.com/swiftstack/ProxyFS/saio/usr/local/go/src/runtime/runtime-gdb.py /usr/local/go/src/runtime/.

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
ln -s /opt/rh/rh-python36/root/usr/include /opt/rh/rh-python36/root/include

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
echo "alias goclean=\"go clean;go clean --cache;go clean --testcache\"" >> ~vagrant/.bash_profile
echo "user_allow_other" >> /etc/fuse.conf

# Setup Samba

yum -y install gcc-c++-4.8.5-16.el7_4.2 \
               python-devel-2.7.5-58.el7 \
               gnutls-devel-3.3.26-9.el7 \
               libacl-devel-2.2.51-12.el7 \
               openldap-devel-2.4.44-5.el7 \
               samba-4.6.2-12.el7_4 \
               samba-client-4.6.2-12.el7_4 \
               cifs-utils-6.2-10.el7
cd /vagrant/src/github.com/swiftstack/ProxyFS/vfs
OS_DISTRO=centos
OS_DISTRO_VERSION=7.4
SAMBA_VERSION=4.6.12
SAMBA_DIR=build-samba-`echo $SAMBA_VERSION | tr . -`-${OS_DISTRO}-`echo $OS_DISTRO_VERSION | tr . -`
if [[ -d $SAMBA_DIR ]]
then
    if [[ -L $SAMBA_DIR ]]
    then
        echo "directory symlink \$GOPATH/src/github.com/swiftstack/ProxyFS/vfs/$SAMBA_DIR cannot pre-exist"
        exit 1
    else
        echo "\$GOPATH/src/github.com/swiftstack/ProxyFS/vfs/$SAMBA_DIR assumed to be as desired"
    fi
else
    if [[ -L $SAMBA_DIR ]]
    then
        echo "non-directory symlink \$GOPATH/src/github.com/swiftstack/ProxyFS/vfs/$SAMBA_DIR cannot pre-exist"
        exit 1
    else
        git clone -b samba-$SAMBA_VERSION --single-branch --depth 1 https://github.com/samba-team/samba.git $SAMBA_DIR
    fi
fi
if [[ -L samba ]]
then
    samba_symlink_target=`readlink "samba"`
    if [[ "$SAMBA_DIR" == "$samba_symlink_target" ]]
    then
        echo "symlink samba -> $SAMBA_DIR already"
    else
        echo "redirecting samba -> $SAMBA_DIR"
        rm samba
        ln -s $SAMBA_DIR samba
    fi
else
    if [[ -e samba ]]
    then
        echo "non-symlink \$GOPATH/src/github.com/swiftstack/ProxyFS/vfs/samba cannot pre-exist"
        exit 1
    else
        echo "establishing samba -> $SAMBA_DIR"
        ln -s $SAMBA_DIR samba
    fi
fi
cd samba
if [[ -d bin ]]
then
    echo "./configure has already been run"
else
    ./configure
    make clean
fi
if [[ -f bin/default/librpc/gen_ndr/ndr_smb_acl.h ]]
then
    echo "make GEN_NDR_TABLES has already been run"
else
    make clean
    make GEN_NDR_TABLES
fi
echo "export SAMBA_SOURCE=\$GOPATH/src/github.com/swiftstack/ProxyFS/vfs/samba" >> ~vagrant/.bash_profile

# Install Python tox

pip install tox

# Setup Swift
#
# Guided by https://docs.openstack.org/swift/latest/development_saio.html

# [Setup Swift] Create the swift:swift user

useradd --user-group --groups wheel swift
chmod 755 ~swift

# Using a loopback device for storage

mkdir -p /srv
truncate -s 0 /srv/swift-disk
truncate -s 10GB /srv/swift-disk
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

pip install --upgrade setuptools

yum -y install https://rpmfind.net/linux/fedora/linux/releases/28/Everything/x86_64/os/Packages/l/liberasurecode-1.5.0-5.fc28.x86_64.rpm
yum -y install https://rpmfind.net/linux/fedora/linux/releases/28/Everything/x86_64/os/Packages/l/liberasurecode-devel-1.5.0-5.fc28.x86_64.rpm

cd ~swift
git clone -b master --single-branch --depth 1 https://github.com/openstack/python-swiftclient.git
cd python-swiftclient
python setup.py develop

echo "export ST_AUTH=http://localhost:8080/auth/v1.0" >> ~vagrant/.bash_profile
echo "export ST_USER=test:tester" >> ~vagrant/.bash_profile
echo "export ST_KEY=testing" >> ~vagrant/.bash_profile

cd ~swift
git clone https://github.com/swiftstack/swift.git
cd swift
git checkout ss-release-2.21.0.4
pip install --no-binary cryptography -r requirements.txt
python setup.py develop
# The following avoid dependency on pip-installed pyOpenSSL being newer than required
pip install python-openstackclient==3.12.0 python-glanceclient==2.7.0
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

# Install ProxyFS's pfs_middleware into the "normal" Swift Proxy pipeline

cd /vagrant/src/github.com/swiftstack/ProxyFS/pfs_middleware
python setup.py develop

# Install ProxyFS's meta_middleware into the "NoAuth" Swift Proxy pipeline

cd /vagrant/src/github.com/swiftstack/ProxyFS/meta_middleware
python setup.py develop

# Setup AWS access for local vagrant user

pip install awscli-plugin-endpoint
mkdir -p ~vagrant/.aws
cat > ~vagrant/.aws/config << EOF
[plugins]
endpoint = awscli_plugin_endpoint

[profile default]
aws_access_key_id = test:tester
aws_secret_access_key = testing
s3 =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 64MB
     multipart_chunksize = 16MB
s3api =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 64MB
     multipart_chunksize = 16MB
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

# Configure exports (NFS) / shares (SMB)

cp /vagrant/src/github.com/swiftstack/ProxyFS/saio/etc/exports /etc/exports
cp /vagrant/src/github.com/swiftstack/ProxyFS/saio/etc/samba/smb.conf /etc/samba/smb.conf
echo -e "swift\nswift" | smbpasswd -a swift

# Install systemd .service files for ProxyFS

cp /vagrant/src/github.com/swiftstack/ProxyFS/saio/usr/lib/systemd/system/proxyfsd.service /usr/lib/systemd/system/.

# Enable Samba service in an SELinux environment

yum -y install policycoreutils-python
semanage port -a -t smbd_port_t -p tcp 12345
semanage port -a -t smbd_port_t -p tcp 32345

# Enable start/stop tools

echo "export PATH=\$PATH:/vagrant/src/github.com/swiftstack/ProxyFS/saio/bin" >> ~vagrant/.bash_profile

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

# Install and configure a localhost-only one-node etcd cluster

yum -y install etcd

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
ExecStart=/usr/bin/etcd --name proxyfs --data-dir /etcd/proxyfs.etcd --initial-advertise-peer-urls http://localhost:2380 --listen-peer-urls http://localhost:2380 --listen-client-urls http://localhost:2379 --advertise-client-urls http://localhost:2379 --initial-cluster-token etcd-cluster --initial-cluster default=http://localhost:2380 --initial-cluster-state new

[Install]
WantedBy=multi-user.target
EOF

# All done

echo "SAIO for ProxyFS provisioned"
