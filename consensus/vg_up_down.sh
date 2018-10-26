#!/bin/bash -x
CMD="$1"
VGNAME="$2"
IPADDR="$3"
NETMASK="$4"
NIC="$5"

echo "args cmd:$CMD vgname:$VGNAME ipaddr:$IPADDR netmask:$NETMASK nic:$NIC"

ERR=0

up()
{
    /usr/sbin/ip addr add ${IPADDR}/${NETMASK} dev $NIC
    ERR=$?
    if [ "X$ERR" != "X0" ]
    then
        exit $ERR
    fi
    /sbin/arping -c 1 -b -U -I $NIC $IPADDR

    /sbin/smbd -s /etc/samba/smb-$VGNAME.conf
    /sbin/nmbd -s /etc/samba/smb-$VGNAME.conf
    ERR=$?
}


down()
{
    pkill -f "/sbin/nmbd -s /etc/samba/smb-$VGNAME.conf"
    pkill -f "/sbin/smbd -s /etc/samba/smb-$VGNAME.conf"

    /usr/sbin/ip addr del ${IPADDR}/${NETMASK} dev $NIC

    # We ignore any errors from the above commands.  This is
    # because the commands could fail because smbd or the VIP
    # may already be offline.
}

# mockUp and mockDown are used for unit tests
#
# For now they just return success.
mockUp()
{
    ERR=0
}

mockDown()
{
    ERR=0
}

# TODO - what other processes must be started/stopped?  Such
# as noauth-proxy, winbindd, rpcbind, rpc.mountd, and rpc.statd?
case $CMD in
    up)
        up
        ;;
    down)
        down
        ;;
    mockUp)
        mockUp
        ;;
    mockDown)
        mockDown
        ;;
esac

# TODO - steps to accomplish this...
exit $ERR
