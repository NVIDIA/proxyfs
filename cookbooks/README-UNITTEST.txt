This is the unit test when changing SAIO.

This test must be executed on all SAIO versions.

First, remove Samba source as follows:

    # cdpfs
    # cd ..
    # rm -rf samba*

Now, test on all SAIO versions:

1. First test Centos:

    # vagrant destroy
    # export SAIO_OS=centos
    # vagrant up

    Need a reboot to change SELinux settings unfortunately.

    # vagrant halt
    # vagrant up
    # vagrant ssh

    Log onto the box and see if everything can be mounted.

    # /usr/bin/start_and_mount_pfs
    # /usr/bin/start_and_mount_pfs
    will start swift, proxyfsd, smb and mount the share on a Centos VM.   Must do twice and
    I have a tracker (https://www.pivotaltracker.com/story/show/135190081) to get this fixed.

    Make sure the mount point shows up:

    # mount | grep proxyfs
    should show something like this:

        //127.0.0.1/proxyfs on /mnt/smb_proxyfs_mount type cifs (rw,relatime,vers=1.0,cache=strict,username=vagrant,domain=LOCALHOST,uid=0,noforceuid,gid=0,noforcegid,addr=127.0.0.1,unix,posixpaths,serverino,acl,rsize=1048576,wsize=65536,actimeo=1)

    Make sure the aliases all work:
    # cdpfs
    # exit

2. Now test Ubuntu:

    # vagrant destroy
    # export SAIO_OS=ubuntu
    # vagrant up

    Same workflow, reboot here.

    # vagrant halt
    # vagrant up
    # vagrant ssh

    Log onto the box and see if everything can be mounted.

    # /usr/bin/start_and_mount_pfs

    will start swift, proxyfsd, smb and mount the share on a Ubuntu VM.  Should only need to do this
    once although the bug shown on Centos probably can happen on Ubuntu.

    Make sure the mount point shows up:

    # mount | grep proxyfs
    should show something like this:

        //127.0.0.1/proxyfs on /mnt/smb_proxyfs_mount type cifs (rw,password=vagrant,user=vagrant)

    Make sure the aliases all work:
    # cdpfs
