#!/bin/bash

if [ "$EUID" -ne 0 ]
  then echo "Please run as root (e.g. sudo $0)"
  exit 1
fi

if [ "$#" -ne 5 ]
  then echo "Please provide Account User Password HashPrefix HashSuffix args"
  exit 1
fi

# Pre-compute vars

Account=$1
User=$2
Password=$3
HashPrefix=$4
HashSuffix=$5

MyIP=`hostname -I`

# Enable script logging

set -e
set -x

# Ensure ~/.bash_profile is properly owned

touch /home/${SUDO_USER}/.bash_profile
chown ${SUDO_UID}:${SUDO_GID} /home/${SUDO_USER}/.bash_profile

# Install Python tools

apt-get install -y python-pip
pip install --upgrade pip
pip install tox

# Install Golang

cd /tmp
TARFILE_NAME=go1.12.5.linux-armv6l.tar.gz
wget -q https://dl.google.com/go/$TARFILE_NAME
tar -C /usr/local -xf $TARFILE_NAME
rm $TARFILE_NAME
echo "export PATH=\$PATH:/usr/local/go/bin" >> /home/${SUDO_USER}/.bash_profile

# Setup Golang development environment

GOPATH=/home/${SUDO_USER}/GOPATH
mkdir -p $GOPATH/bin
mkdir -p $GOPATH/src/github.com/swiftstack/ProxyFS
chown -R ${SUDO_UID}:${SUDO_GID} $GOPATH
echo "export GOPATH=$GOPATH" >> /home/${SUDO_USER}/.bash_profile
echo "export PATH=\$PATH:\$GOPATH/bin" >> /home/${SUDO_USER}/.bash_profile
echo "alias cdpfs=\"cd \$GOPATH/src/github.com/swiftstack/ProxyFS\"" >> /home/${SUDO_USER}/.bash_profile
echo "alias goclean=\"go clean;go clean --cache;go clean --testcache\"" >> /home/${SUDO_USER}/.bash_profile

# Configure FUSE

echo "user_allow_other" >> /etc/fuse.conf

# Create Swift user

useradd --create-home --user-group --groups sudo swift

# Configure rsyncd
# Note: targeting just $address causes systemd startup of rsync at boot to fail

cat >/etc/rsyncd.conf <<EOF
uid = swift
gid = swift
log file = /var/log/rsyncd.log
pid file = /var/run/rsyncd.pid
# address = $MyIP

[account]
max connections = 10
path = /srv
read only = False
lock file = /var/lock/account.lock

[container]
max connections = 10
path = /srv
read only = False
lock file = /var/lock/container.lock

[object]
max connections = 10
path = /srv
read only = False
lock file = /var/lock/object.lock
EOF

sed -i "s/RSYNC_ENABLE=false/RSYNC_ENABLE=true/" /etc/default/rsync

systemctl start rsync

# Install Swift

apt-get install -y swift swift-proxy swift-account swift-container swift-object memcached

# Configure Swift to simply serve /srv

ln -s /srv /mnt/srv

# Build Swift Account/Container/Object Rings

cd /etc/swift

rm -f *.builder *.ring.gz backups/*.builder backups/*.ring.gz

swift-ring-builder object.builder create 10 3 1
swift-ring-builder object.builder add r1z1-10.0.0.101:6200/srv 1
swift-ring-builder object.builder add r1z1-10.0.0.102:6200/srv 1
swift-ring-builder object.builder add r1z1-10.0.0.103:6200/srv 1
swift-ring-builder object.builder rebalance

swift-ring-builder container.builder create 10 3 1
swift-ring-builder container.builder add r1z1-10.0.0.101:6201/srv 1
swift-ring-builder container.builder add r1z1-10.0.0.102:6201/srv 1
swift-ring-builder container.builder add r1z1-10.0.0.103:6201/srv 1
swift-ring-builder container.builder rebalance

swift-ring-builder account.builder create 10 3 1
swift-ring-builder account.builder add r1z1-10.0.0.101:6202/srv 1
swift-ring-builder account.builder add r1z1-10.0.0.102:6202/srv 1
swift-ring-builder account.builder add r1z1-10.0.0.103:6202/srv 1
swift-ring-builder account.builder rebalance

# Configure Account/Server/Object Servers

cat >account-server.conf <<EOF
[DEFAULT]
bind_port = 6202
devices = /srv

[pipeline:main]
pipeline = healthcheck recon account-server

[app:account-server]
use = egg:swift#account

[filter:healthcheck]
use = egg:swift#healthcheck

[filter:recon]
use = egg:swift#recon

[filter:xprofile]
use = egg:swift:xprofile

[account-replicator]

[account-auditor]

[account-reaper]
EOF

cat >container-server.conf <<EOF
[DEFAULT]
bind_port = 6201
devices = /srv

[pipeline:main]
pipeline = healthcheck recon container-server

[app:container-server]
use = egg:swift#container

[filter:healthcheck]
use = egg:swift#healthcheck

[filter:recon]
use = egg:swift#recon

[filter:xprofile]
use = egg:swift:xprofile

[container-replicator]

[container-updater]

[container-auditor]

[container-sync]

[container-sharder]
EOF

cat >object-server.conf <<EOF
[DEFAULT]
bind_port = 6200
devices = /srv

[pipeline:main]
pipeline = healthcheck recon object-server

[app:object-server]
use = egg:swift#object

[filter:healthcheck]
use = egg:swift#healthcheck

[filter:recon]
use = egg:swift#recon

[filter:xprofile]
use = egg:swift:xprofile

[object-replicator]

[object-reconstructor]

[object-updater]

[object-auditor]
EOF

chown -R swift:swift /srv

mkdir -p /var/cache/swift
chown -R root:swift /var/cache/swift
chmod -R 775 /var/cache/swift

# Configure Proxy Server

cat >proxy-server.conf <<EOF
[DEFAULT]
bind_port = 7080

[pipeline:main]
pipeline = # catch_errors gatekeeper healthcheck proxy-logging cache container_sync bulk tempurl ratelimit tempauth copy container_quotes account_quotes slo dlo versioned_writes proxy-logging proxy-server

[app:proxy-server]
use = egg:swift#proxy

[filter:catch_errors]
use = egg:swift#catch_errors

[filter:gatekeeper]
use = egg:swift#gatekeeper

[filter:healthcheck]
use = egg:swift#healthcheck

[filter:proxy-logging]
use = egg:swift#proxy-logging

[filter:cache]
use = egg:swift#memcache

[filter:container_sync]
use = egg:swift#container_sync

[filter:bulk]
use = egg:swift#bulk

[filter:tempurl]
use = egg:swift#tempurl

[filter:ratelimit]
use = egg:swift#ratelimit

[filter:tempauth]
use = egg:swift#tempauth
user ${Account}_${User} = ${Password} .admin .reseller_admin

[filter:copy]
use = egg:swift#copy

[filter:container_quotes]
use = egg:swift#container_quotes

[filter:account_quotes]
use = egg:swift#account_quotes

[filter:slo]
use = egg:swift#slo

[filter:dlo]
use = egg:swift#dlo

[filter:versioned_writes]
use = egg:swift#versioned_writes

[filter:proxy-logging]
use = egg:swift#proxy-logging
EOF

# Finalize Swift configuration

cat >swift.conf <<EOF
swift_hash_path_prefix = $HashPrefix
swift_hash_path_suffix = $HashSuffix

[storage-policy:0]
name = Policy-0
default = yes
EOF

chown -R root:swift /etc/swift

# (Re)Start all Swift services

systemctl stop swift\* --all
systemctl stop memcached

systemctl start memcached
systemctl start swift\* --all

echo "Starting ProxyFS is a work in progress..."
