#!/bin/bash -x 
CMD="$1"
VGNAME="$2"
IPADDR="$3"
NETMASK="$4"
NIC="$5"

echo "args cmd:$CMD vgname:$VGNAME ipaddr:$IPADDR netmask:$NETMASK nic:$NIC"

ERR=""

# TODO - should use ip command and not ifconfig
# How store new virtual network interface and make it unique?
if [ "X$CMD" == "Xup" ]
then
    /usr/sbin/ifconfig $NIC:0 $IPADDR
    ERR="$?"
else
    if [ "X$CMD" == "Xdown" ]
    then
        /usr/sbin/ifconfig $NIC:0 down
        ERR="$?"
    fi
fi

# TODO - steps to accomplish this...
exit $ERR
