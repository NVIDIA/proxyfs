# Deployment on Armbian

Instructions and tools enabling deployment of Swift and ProxyFS on Armbian-based systems.
The following example will deploy three (3) Swift+ProxyFS nodes in a Swift Cluster. Each
node (peer) will be an `arm7l`-based (i.e. 32-bit) computer with a single HD/SDD attached.
While all three peers will actively perform each of the Proxy, Account, Container, and
Object functions of Swift, only a single ProxyFS instance will be serving Volumes at a
time. A High Availability (HA) function will be deployed to ensure that the chosen to be
active ProxyFS instance will be running on a "live" node.

## Prerequisites

* Three `arm7l`-based systems with at least 2GB DRAM
* Default/single (root) file system deployed on adequate storage device supporting xttrs
* Hard/DHCP-assigned IPv4 Address of 10.0.0.100 for the floating Virtual IP
* Hard/DHCP-assigned IPv4 Address for each peer of 10.0.0.101, 10.0.0.102, & 10.0.0.103
* A sudo-capable (not root!) User with GitHub access (e.g. ~/.ssh/id_rsa pre-registered)
* Open/routable TCP Ports for SSH & Swift configured:
    * Port 705{1-3} mapped to 10.0.0.10{1-3}
    * Port 7080     mapped to 10.0.0.100

## Example Component for each peer

* ODroid HC1 - ARM-based SBC w/ case for 2.5" HD/SSD - $64
* 5V/4A Power Adapter - POS 2.1mm inner, NEG 5.5mm outer barrel style - $16
* Samsung 64GB EVO microSD - $11
* Samsung 1TB EVO 2.5" SATA SSD - $148

## Useful Tools

* Rocketek USB 3.0 microSD Adapter - both USB-A and USB-C versions available - $8-$9
* Armbian Site: https://www.armbian.com/odroid-hc1/
    * Armbian Stretch: 4.14y "mainline" kernel based on Debian (used in this example)
    * Armbian Bionic: 4.14y "mainline" kernel based on Ubuntu
* The Unarchiver: https://theunarchiver.com/
* Balena Etcher: https://www.balena.io/etcher

## Setup Instructions

* Initial login
    * ssh root@10.0.0.10{1-3} (initial pwd = "1234"; you will be forced to change it)
    * create non-root/sudo-capable user... let's call it "ed"
* ssh ed@10.0.0.10{1-3}
    * sudo apt-get update
    * sudo apt-get upgrade
    * mkdir ~/.ssh
    * chmod 700 ~/.ssh
    * sudo vi /etc/ssh/sshd_config to include the following two lines:
        * ListenAddress 0.0.0.0:22
        * ListenAddress 10.0.0.10{1-3}:705{1-3}
* From client
    * Ensure your (GitHub-registered) user has SSH Keys (~/.ssh/id_rsa}|.pub)
    * cd ~/.ssh
    * scp id_rsa.pub ed@10.0.0.10{1-3}:.ssh/authorized_keys
    * scp id_rsa.pub ed@10.0.0.10{1-3}:.ssh/id_rsa.pub
    * scp id_rsa ed@10.0.0.10{1-3}:.ssh/id_rsa
* Shutdown ODroid (sudo shutdown -h now)
* Install SSD
* Reboot ODroid
* Migrate root drive from SCCard to SSD
    * ssh ed@10.0.0.10{1-3}
        * sudo nand-sata-install
        * Select "Boot from SD"
            * If "/dev/sda1" is unavailable:
                * You need to partition a newly installed SSD
                * Take the defaults (i.e. a single Linux partition)
        * Select "/dev/sda1"
        * Format as "btrfs"
        * Reboot (sudo shutdown -r now)
* ssh ed@10.0.0.10{1-3}
    * sudo armbian-config
        * System->DTB->hc1
        * System->SSH->Uncheck "Allow root login"
        * System->SSH->Uncheck "Password login"
        * Personal->Timezone->US->Pacific-New
        * Personal->Hostname->odroid{1-3}
* To add additional client
    * ssh ed@10.0.0.10{1-3} (from 1st client, since 2nd client can't SSH yet)
        * sudo-armbian-config
            * System->SSH->Check "Password login"
    * cd ~/.ssh (from 2nd client)
    * scp id_rsa.pub ed@10.0.0.10{1-3}:.ssh/authorized_key_to_append
    * ssh ed@10.0.0.10{1-3}
        * sudo armbian-config
            * System->SSH->Uncheck "Password login"
        * cd ~/.ssh
        * cat authorized_key_to_append >> authorized_keys
        * rm authorized_key_to_append

## Provision Swift & ProxyFS

* ssh ed@10.0.0.10{1-3}
* cd /tmp
* wget https://raw.githubusercontent.com/swiftstack/ProxyFS/ArmbianExample/armbian/provision.sh
* chmod +x provision.sh
* sudo ./provision.sh
