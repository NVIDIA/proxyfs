## Following are the steps to build and configure samba
**Note: Rest of the document assumes /vagrant/src/github.com/swiftstack/samba as the root of the samba source directory.**

## Bring up and ssh to the VM:
	1. Bring up the proxyfs_saio VM using the vagrant file under src/github.com/swiftstack/ProxyFS of the repository:
		cd src/github.com/swiftstack/ProxyFS
		vagrant up proxyfs_saio
		vagrant ssh proxyfs_saio

		Note: To enable X apps (e.g. wireshark): vagrant ssh proxyfs_saio -- -Y
        Note2: You may need to do:
            # vagrant plugin install vagrant-vbguest

## Install samba
	2. sudo apt-get install samba
        (Actually, this should already have been done by Chef during VM provisioning.)

## Configure samba
**Required to build proxyfs VFS module.**

	3. Get the samba source (skip if already have it):
	   *This step is run from the host machine:*
	   3.1 cd src/github.com/swiftstack

      NOTE:  If you are using Centos you need Samba 4.2 or:
           git clone -b v4-2-stable --single-branch --depth 1 https://github.com/samba-team/samba.git
      If you are using Ubuntu you need Samba 4.3 or:
	       git clone -b v4-3-stable --single-branch --depth 1 https://github.com/samba-team/samba.git

	   * Back to VM after this step *

	4. cd /vagrant/src/github.com/swiftstack/samba

	5. ./configure

	6. make clean

	7. Compile the source just enough to generate the header files.
	   make GEN_NDR_TABLES

## Make & Install ProxyFS and SambaVFS components
	8. 8.1 Install Proxyfs
	   cd /vagrant/src/github.com/swiftstack/ProxyFS
	   ./regression_test.py

		8.2 Install libproxyfs (jason rpc client to talk to proxyfsd)

		8.3 Install Proxyfs VFS samba module.
			run make install in the current directory

## Configuring samba:
**In this example we are using local user `vagrant` for the share:
Note: We assume the default location is /usr/local/samba; if not, the path must be changed.**

	9. sudo smbpasswd -a vagrant
	    [enter 'vagrant' for the password]

    10. Add the following proxyfs share section to /etc/samba/smb.conf:
        (e.g. sudo vi /etc/samba/smb.conf)
        In this example:
			- we are assuming NT ACLs are supported. If not, then remove "acl_xattr" in vfs_objects.
			- Enabling AIO for reads and writes.


        # ProxyFS Share:
		[proxyfs]
		comment = ProxyFS test volume
		path = /mnt/CommonVolume
		vfs objects = proxyfs
		proxyfs:volume = CommonVolume
		valid users = vagrant
		public = yes
		writable = yes
		printable = no
		browseable = yes
		read only = no
		oplocks = False
		level2 oplocks = False
		aio read size = 1
		aio write size = 1

## Running proxyfsd:
	11. Running swift:

		Check if swift is running:
		`sudo swift-init main status`

		If it is not running:
		`sudo swift-init main start`

		Note: restarting the VM will not restart Swift

	12. Running proxyfsd (Assuming single node):
	    # TODO: daemonize; hook into startup infrastructure

		cd /vagrant/src/github.com/swiftstack/ProxyFS
		# NOTE: you could alternately use the 'cdpfs' alias for the step above 

		# NOTE: To optionally clean up proxyfsd-related files left over from previous runs, call cleanproxyfs proxyfsd/saioproxyfsd0.conf

        # Start proxyfsd
		proxyfsd proxyfsd/saioproxyfsd0.conf &

		NOTE: the log file proxyfsd.log will be created in the directory you start proxyfsd from, if it isn't there already



## Running samba daemon and client:
### Running samba daemon

	13. smbd is running by default. To stop if it is running:

        For Centos do:
        sudo systemctl stop smb

        For Ubuntu do:
		sudo service smbd stop

	14. Running smbd with full debugging info:
	        sudo smbd -i --debuglevel=10
        Without (noisy) debugging info:
            sudo smbd -i
        As daemon:

          For Centos do:
          sudo systemctl start smb

          For Ubuntu do:
          sudo service smbd start

### Option - Mounting from a client
**In this example client is running on localhost.**

	15a. Create a mount directory - /tmp/vol
	     sudo mkdir /tmp/vol

         Mount assuming user/passward is vagrant/vagrant.
		 sudo mount -t cifs  -o user=vagrant,password=vagrant //127.0.0.1/proxyfs /tmp/vol

         Unmount (note: this will cause smbd -i to exit)

         sudo umount /tmp/vol

### Option - Running smbclient
** In this example connecting to localhost. **

	15b. sudo smbclient -U vagrant //127.0.0.1/proxyfs

## Tracing/Debugging:
    16. Assuming you have an X Server running on your host,
        and started your ssh session with the "-- -Y" option:

        wireshark
