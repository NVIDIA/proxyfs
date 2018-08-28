#!/bin/bash

set -e

yum -y update
yum -y install ansible

# Install tools needed above what's in a minimal base box

yum -y install wget git nfs-utils vim

# Install Golang

yum -y --disableexcludes=all install gcc
cd /tmp
wget -q https://dl.google.com/go/go1.10.linux-amd64.tar.gz
tar -C /usr/local -xf go1.10.linux-amd64.tar.gz
rm go1.10.linux-amd64.tar.gz
echo "export PATH=\$PATH:/usr/local/go/bin" >> ~vagrant/.bash_profile
echo "export PATH=\$PATH:/usr/local/go/bin" >> ~root/.bash_profile
echo "export PATH=\$PATH:/usr/local/go/bin" >> ~root/.bashrc

# Patch Golang's GDB runtime plug-in

mv /usr/local/go/src/runtime/runtime-gdb.py /usr/local/go/src/runtime/runtime-gdb.py_ORIGINAL
#TODO - cp /vagrant/src/github.com/swiftstack/ProxyFS/saio/usr/local/go/src/runtime/runtime-gdb.py /usr/local/go/src/runtime/.
cp /vagrant/saio/usr/local/go/src/runtime/runtime-gdb.py /usr/local/go/src/runtime/.

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