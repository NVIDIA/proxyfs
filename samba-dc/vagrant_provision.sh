#!/bin/bash
#
# Notes:
#   1) This script assumes it is being run as root
#   2) This script assumes the 1st arg is to be the hostname
#   3) This script assumes the 2nd arg is to be the IP Addr exposed by DC
#   4) This script assumes the 3rd arg is to be the domain & netbios name

set -e
set -x

# Canonicalize hostname and domain

hostnameLC=`echo $1 | tr A-Z a-z`
hostnameUC=`echo $1 | tr a-z A-Z`

domainLC=`echo $3 | tr A-Z a-z`
domainUC=`echo $3 | tr a-z A-Z`

# Preserve IP Addr exposed by DC

ipAddr=$2

# Preserve current nameserver

currentNameserver=`nmcli dev show | grep DNS | awk '{print $2}'`

# Set hostname and update /etc/hosts

hostnamectl set-hostname $hostnameLC
echo "$ipAddr $hostnameLC $hostnameLC.$domainLC.local" >> /etc/hosts

# Fixup /etc/resolv.conf & prevent NetworkManager from modifying it

mv /etc/resolv.conf /etc/resolv.conf_ORIGINAL

cat > /etc/resolv.conf_MODIFIED <<EOF
# Provisioned by Vagrant launching vagrant_provision.sh
nameserver $ipAddr
nameserver $currentNameserver
EOF

ln -s /etc/resolv.conf_MODIFIED /etc/resolv.conf

# Install Development Tools

yum -y --disableexcludes=all group install "Development Tools"
# yum -y --setopt=group_package_types=mandatory,default,optional --disableexcludes=all groupinstall "Development Tools"

yum -y install \
  iniparser \
  libldb \
  libtalloc \
  libtdb \
  libtevent \
  python-devel \
  gnutls-devel \
  libacl-devel \
  openldap-devel \
  pam-devel \
  readline-devel \
  krb5-devel \
  cups-devel

# Install Samba

cd /tmp

curl -O https://download.samba.org/pub/samba/stable/samba-4.8.3.tar.gz
tar -zxvf samba-4.8.3.tar.gz

cd samba-4.8.3

./configure \
--prefix=/usr \
--localstatedir=/var \
--with-configdir=/etc/samba \
--libdir=/usr/lib64 \
--with-modulesdir=/usr/lib64/samba \
--with-pammodulesdir=/lib64/security \
--with-lockdir=/var/lib/samba \
--with-logfilebase=/var/log/samba \
--with-piddir=/run/samba \
--with-privatedir=/etc/samba \
--enable-cups \
--with-acl-support \
--with-ads \
--with-automount \
--enable-fhs \
--with-pam \
--with-quotas \
--with-shared-modules=idmap_rid,idmap_ad,idmap_hash,idmap_adex \
--with-syslog \
--with-utmp \
--with-dnsupdate

make
make install

cd /tmp

rm -rf samba-4.8.3*

# Prepare Samba Domain

rm -rf /run/samba /etc/samba/smb.conf
mkdir -p /run/samba /etc/samba

samba-tool domain provision --domain=$domainUC --realm=$domainUC.LOCAL --host-ip=$ipAddr --adminpass=ProxyFS$
samba-tool user setexpiry --noexpiry Administrator

mv /etc/samba/krb5.conf /etc/krb5.conf

# Adjust dns-forwarder to be currentNameserver in smb.conf

mv /etc/samba/smb.conf /etc/samba/smb.conf_ORIGINAL

sed "s/$ipAddr/$currentNameserver/" /etc/samba/smb.conf_ORIGINAL > /etc/samba/smb.conf

# Configure systemd to manage Samba DC

cat > /usr/lib/systemd/system/samba.service <<EOF
[Unit]
Description=Samba AD Daemon
Wants=network-online.target
After=network.target network-online.target rsyslog.service

[Service]
Type=forking
PIDFile=/run/samba/samba.pid
LimitNOFILE=16384
ExecStart=/usr/sbin/samba --daemon
ExecReload=/bin/kill -HUP $MAINPID

[Install]
WantedBy=multi-user.target
EOF

cat > /etc/tmpfiles.d/samba.conf <<EOF
d /var/run/samba 0755 root root -
EOF

# Start-up Samba (now and each reboot)

systemctl start samba
systemctl enable samba

# TODO: List existing zones

# samba-tool dns zonelist $hostnameLC.$domainLC.local --username Administrator --password=ProxyFS$

# TODO: Add an AD User (needed for other samba-tool commands)

# samba-tool user create user1 ProxyFS$

# TODO: List Zones

# samba-tool dns zonelist $hostnameLC.$domainLC.local --username user1 --password=ProxyFS$

# TODO: Add a Zone

# samba-tool dns zonecreate $hostnameLC.$domainLC.local 128.28.172.in-addr-arpa --username user1 --password ProxyFS$

# TODO: Add an A Record

# samba-tool dns add $hostnameLC.$domainLC.local $domainLC.local machine1 A 172.28.128.21 --username user1 --password ProxyFS$

# All done

echo "Samba DC provisioned"
