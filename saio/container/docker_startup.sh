#!/bin/bash

# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

mount /srv/1/node/sdb1
chown -R swift:swift /srv/1/node/sdb1

rsyslogd

memcached -u swift -d

swift-init main start

while true
do
    curl http://127.0.0.1:8090/info 2>/dev/null >/dev/null
    if [ $? -eq 0 ]
    then
        break
    fi
    echo "Waiting for Swift to be started..."
    sleep 1
done

echo "Swift is up... starting ProxyFS..."

myPublicIPAddr=`ip addr | grep inet | grep eth0 | awk '{print $2}' | awk 'BEGIN {FS="/"} {print $1}'`

echo "myPublicIPAddr == $myPublicIPAddr"

proxyfsd proxyfs.conf Peer:Peer0.PublicIPAddr=$myPublicIPAddr
