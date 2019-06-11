You need to have cloned ProxyFS into your GOPATH (i.e. $GOPATH/src/github.com/swiftstack/ProxyFS) for this to work

    vagrant up

Then `vagrant ssh` and you can start services:

`start_and_mount_pfs` - starts Swift (including NoAuth Proxy), ProxyFS, Samba, and NFS... and performs an SMB and NFS mount of CommonVolume
`start_proxyfs_and_swift` - starts Swift (including NoAuth Proxy) and ProxyFS
`start_swift_only` - starts Swift (including NoAuth Proxy)
`unmount_and_stop_pfs` - stops any of the above

The swift source is installed from a git clone in /home/swift/swift

The vagrant user has some env setup to help you
... but expect most things to require "sudo -E"
