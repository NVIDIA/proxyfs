#!/bin/bash -x 
CMD="$1"
VGNAME="$2"
IPADDR="$3"
NETMASK="$4"
NIC="$5"

echo "args cmd:$CMD vgname:$VGNAME ipaddr:$IPADDR netmask:$NETMASK nic:$NIC"

ERR=""

# TODO - what other processes must be started/stopped?  Such
# as noauth-proxy, nmbd, smbd, winbindd, rpcbind, rpc.mountd,
# and rpc.statd?
if [ "X$CMD" == "Xup" ]
then
    /usr/sbin/ip addr add $IPADDR dev $NIC
    ERR="$?"
    if [ "X$ERR" != "X0" ]
    then
        exit $ERR
    fi
    /sbin/smbd -s /etc/samba/smb-$VGNAME.conf
    ERR="$?"
else
    if [ "X$CMD" == "Xdown" ]
    then

        pkill -f "/sbin/smbd -s /etc/samba/smb-$VGNAME.conf"

        /usr/sbin/ip addr del ${IPADDR}/${NETMASK} dev $NIC

	# We ignore any errors from the above commands.  This is
	# because the commands could fail because smbd or the VIP
	# may already be offline.
    fi
fi

# TODO - steps to accomplish this...
exit $ERR
