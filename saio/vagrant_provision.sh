#!/bin/bash
#
# Note: This script assumes it is being run as root

set -e
set -x

# Experiments getting a 4.4 version of Samba installed

cd /tmp
mkdir samba
cd samba

wget -q http://pkgs.fedoraproject.org/repo/pkgs/samba/samba-4.4.14.tar.xz/sha512/6fce503974e48ad120c42bc53b88c67b4b9ccd72b921cd854f6e46d51fe29db7ff37316c3317c8b16ac74fcb8cad1924aa490fa0d1fee2a8152e94ff75d8ce38/samba-4.4.14.tar.xz

yum -y install cups-devel
: '
wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/updates/24/x86_64/c/cups-2.1.4-7.fc24.x86_64.rpm
rpm -i cups-2.1.4-7.fc24.x86_64.rpm
'

wget -q ftp://fr2.rpmfind.net/linux/fedora-secondary/updates/24/s390x/s/samba-common-4.4.14-0.fc24.noarch.rpm
rpm -i samba-common-4.4.14-0.fc24.noarch.rpm
wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/releases/24/Everything/x86_64/os/Packages/l/libtdb-1.3.9-1.fc24.x86_64.rpm
rpm -i libtdb-1.3.9-1.fc24.x86_64.rpm
wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/releases/24/Everything/x86_64/os/Packages/l/libldb-1.1.26-1.fc24.x86_64.rpm
rpm -i libldb-1.1.26-1.fc24.x86_64.rpm
wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/updates/24/x86_64/g/glibc-all-langpacks-2.23.1-12.fc24.x86_64.rpm
rpm -i --nodeps --replacefiles glibc-all-langpacks-2.23.1-12.fc24.x86_64.rpm
wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/updates/24/x86_64/g/glibc-2.23.1-12.fc24.x86_64.rpm
rpm -i --nodeps --replacefiles glibc-2.23.1-12.fc24.x86_64.rpm
wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/updates/24/x86_64/g/glibc-common-2.23.1-12.fc24.x86_64.rpm
rpm -i --nodeps --replacefiles glibc-common-2.23.1-12.fc24.x86_64.rpm
wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/releases/24/Everything/x86_64/os/Packages/n/nettle-3.2-2.fc24.x86_64.rpm
rpm -i --replacefiles nettle-3.2-2.fc24.x86_64.rpm
wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/releases/24/Everything/x86_64/os/Packages/c/crypto-policies-20151104-2.gitf1cba5f.fc24.noarch.rpm
rpm -i crypto-policies-20151104-2.gitf1cba5f.fc24.noarch.rpm
wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/updates/24/x86_64/l/libtasn1-4.9-1.fc24.x86_64.rpm
rpm -i --replacefiles libtasn1-4.9-1.fc24.x86_64.rpm
wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/updates/24/x86_64/g/gnutls-3.4.17-2.fc24.x86_64.rpm
rpm -i --replacefiles gnutls-3.4.17-2.fc24.x86_64.rpm
wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/releases/24/Everything/x86_64/os/Packages/l/libacl-2.2.52-11.fc24.x86_64.rpm
rpm -i --replacefiles libacl-2.2.52-11.fc24.x86_64.rpm
wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/releases/24/Everything/x86_64/os/Packages/l/libarchive-3.1.2-17.fc24.x86_64.rpm
rpm -i libarchive-3.1.2-17.fc24.x86_64.rpm
wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/updates/24/x86_64/n/ncurses-base-6.0-6.20160709.fc24.noarch.rpm
rpm -i --replacefiles ncurses-base-6.0-6.20160709.fc24.noarch.rpm

echo "Still working on the below..."

wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/releases/24/Everything/x86_64/os/Packages/n/ncurses-libs-6.0-5.20160116.fc24.x86_64.rpm
: rpm -i ncurses-libs-6.0-5.20160116.fc24.x86_64.rpm
wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/updates/24/x86_64/n/ncurses-libs-6.0-6.20160709.fc24.x86_64.rpm
: rpm -i ncurses-libs-6.0-6.20160709.fc24.x86_64.rpm
wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/releases/24/Everything/x86_64/os/Packages/l/lua-5.3.2-3.fc24.x86_64.rpm
: rpm -i --replacefiles lua-5.3.2-3.fc24.x86_64.rpm

: '
error: Failed dependencies:
	rpm = 4.13.0-0.rc1.27.fc24 is needed by rpm-libs-4.13.0-0.rc1.27.fc24.x86_64
	rpm-plugin-selinux(x86-64) = 4.13.0-0.rc1.27.fc24 is needed by rpm-libs-4.13.0-0.rc1.27.fc24.x86_64'

wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/releases/24/Everything/x86_64/os/Packages/r/rpm-libs-4.13.0-0.rc1.27.fc24.x86_64.rpm
: rpm -i rpm-libs-4.13.0-0.rc1.27.fc24.x86_64.rpm

: '
error: Failed dependencies:
	librpm.so.7()(64bit) is needed by rpm-4.13.0.1-1.fc24.x86_64
	librpmio.so.7()(64bit) is needed by rpm-4.13.0.1-1.fc24.x86_64
'

wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/updates/24/x86_64/r/rpm-4.13.0.1-1.fc24.x86_64.rpm
: rpm -i rpm-4.13.0.1-1.fc24.x86_64.rpm

: '
error: Failed dependencies:
	librpm.so.7()(64bit) is needed by rpm-plugin-selinux-4.13.0-0.rc1.27.fc24.x86_64
	librpmio.so.7()(64bit) is needed by rpm-plugin-selinux-4.13.0-0.rc1.27.fc24.x86_64
	rpm-libs(x86-64) = 4.13.0-0.rc1.27.fc24 is needed by rpm-plugin-selinux-4.13.0-0.rc1.27.fc24.x86_64
'

wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/releases/24/Everything/x86_64/os/Packages/r/rpm-plugin-selinux-4.13.0-0.rc1.27.fc24.x86_64.rpm
: rpm -i rpm-plugin-selinux-4.13.0-0.rc1.27.fc24.x86_64.rpm

: '
error: Failed dependencies:
	rpm = 4.13.0-0.rc1.27.fc24 is needed by rpm-libs-4.13.0-0.rc1.27.fc24.x86_64
	rpm-plugin-selinux(x86-64) = 4.13.0-0.rc1.27.fc24 is needed by rpm-libs-4.13.0-0.rc1.27.fc24.x86_64
'

wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/releases/24/Everything/x86_64/os/Packages/r/rpm-libs-4.13.0-0.rc1.27.fc24.x86_64.rpm
: rpm -i rpm-libs-4.13.0-0.rc1.27.fc24.x86_64.rpm

: '
error: Failed dependencies:
	librpm.so.7()(64bit) is needed by rpm-4.13.0.1-1.fc24.x86_64
	librpmio.so.7()(64bit) is needed by rpm-4.13.0.1-1.fc24.x86_64
'
wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/updates/24/x86_64/r/rpm-4.13.0.1-1.fc24.x86_64.rpm
: rpm -i rpm-4.13.0.1-1.fc24.x86_64.rpm

: '
error: Failed dependencies:
	libreplace-samba4.so()(64bit) is needed by libwbclient-2:4.4.14-0.fc24.x86_64
	libreplace-samba4.so(SAMBA_4.4.14)(64bit) is needed by libwbclient-2:4.4.14-0.fc24.x86_64
	samba-client-libs = 2:4.4.14-0.fc24 is needed by libwbclient-2:4.4.14-0.fc24.x86_64
'

wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/updates/24/x86_64/l/libwbclient-4.4.14-0.fc24.x86_64.rpm
: rpm -i libwbclient-4.4.14-0.fc24.x86_64.rpm

: '
error: Failed dependencies:
	libwbclient = 2:4.4.14-0.fc24 is needed by samba-client-libs-2:4.4.14-0.fc24.x86_64
	libwbclient.so.0()(64bit) is needed by samba-client-libs-2:4.4.14-0.fc24.x86_64
	libwbclient.so.0(WBCLIENT_0.9)(64bit) is needed by samba-client-libs-2:4.4.14-0.fc24.x86_64
'

wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/updates/24/x86_64/s/samba-client-libs-4.4.14-0.fc24.x86_64.rpm
: rpm -i samba-client-libs-4.4.14-0.fc24.x86_64.rpm

: '
error: Failed dependencies:
	nss-softokn < 3.28.3 conflicts with nss-softokn-freebl-3.30.2-1.0.fc24.x86_64
'

wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/updates/24/x86_64/n/nss-softokn-freebl-3.30.2-1.0.fc24.x86_64.rpm
: rpm -i nss-softokn-freebl-3.30.2-1.0.fc24.x86_64.rpm

: '
Lots :-(
'

wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/updates/24/x86_64/s/samba-common-libs-4.4.14-0.fc24.x86_64.rpm
: rpm -i samba-common-libs-4.4.14-0.fc24.x86_64.rpm

: '
Lots :-(
'

wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/updates/24/x86_64/s/samba-common-tools-4.4.14-0.fc24.x86_64.rpm
: rpm -i samba-common-tools-4.4.14-0.fc24.x86_64.rpm

: '
Lots :-(
'

wget -q ftp://fr2.rpmfind.net/linux/fedora/linux/updates/24/x86_64/s/samba-4.4.14-0.fc24.x86_64.rpm
: rpm -i samba-4.4.14-0.fc24.x86_64.rpm

echo "All done with samba manual provision"

: exit 0

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

# Install ProxyFS's pfs_middleware

cd /vagrant/src/github.com/swiftstack/ProxyFS/pfs_middleware
python setup.py develop

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

# All done

echo "SAIO for ProxyFS provisioned"
