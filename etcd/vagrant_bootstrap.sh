#!/bin/bash

set -e

yum -y update
yum -y install ansible

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

yum -y install wget git nfs-utils vim

# Install Golang
cd /tmp
wget -q https://dl.google.com/go/go1.10.linux-amd64.tar.gz
tar -C /usr/local -xf go1.10.linux-amd64.tar.gz
rm go1.10.linux-amd64.tar.gz
echo "export PATH=\$PATH:/usr/local/go/bin" >> ~vagrant/.bash_profile
echo "export PATH=\$PATH:/usr/local/go/bin" >> ~root/.bash_profile

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

# Install wireshark

yum -y install wireshark-gnome \
               xorg-x11-fonts-Type1 \
               xorg-x11-xauth \
               xeyes
echo "X11Forwarding yes" >> /etc/sysconfig/sshd
systemctl restart sshd
usermod -aG wireshark vagrant

# Install benchmark support tools

yum -y install atop bc
##yum -y install fio

# Install ssh helper

yum -y install sshpass-1.06-2.el7

# Install dstat

yum -y install dstat

# Install Samba
yum -y install samba samba-client samba-common
