#!/bin/bash
#
# Notes:
#   1) This script assumes it is being run as root
#   2) This script assumes the 1st arg is to be the hostname
#   3) This script assumes the 2nd arg is to be the ip addr
#   4) This script assumes the 3rd arg is to be the domain & netbios name

set -e
set -x

# Canonicalize hostname and domain

hostnameLC=`echo $1 | tr A-Z a-z`
hostnameUC=`echo $1 | tr a-z A-Z`

domainLC=`echo $3 | tr A-Z a-z`
domainUC=`echo $3 | tr a-z A-Z`

# Preserve IP Addr

ipAddr=$2

# Set hostname and update /etc/hosts

hostnamectl set-hostname $hostnameLC
echo "$ipAddr $hostnameLC $hostnameLC.$domainLC.local" >> /etc/hosts

# Fixup /etc/resolv.conf & prevent NetworkManager from modifying it

sed "/nameserver/ i nameserver $ipAddr" /etc/resolv.conf > /etc/resolv.conf_MODIFIED

cat /etc/resolv.conf
cat /etc/resolv.conf_MODIFIED

mv /etc/resolv.conf /etc/resolv.conf_ORIGINAL
ln -s /etc/resolv.conf_MODIFIED /etc/resolv.conf

cat /etc/resolv.conf

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

samba-tool domain provision --domain=SDOM1 --realm=SDOM1.LOCAL --adminpass=ProxyFS-Samba-DC

mv /etc/samba/krb5.conf /etc/krb5.conf

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

# All done

echo "Samba DC provisioned"
