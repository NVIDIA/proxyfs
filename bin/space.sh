#!/bin/bash
#
# Outputs:
#   VmSize (kB) sum for all smbd and proxyfsd processes on this node
#   Disk space used (in 1K-blocks) by all Swift devices on this node

vmsize=$(pidof proxyfsd smbd | xargs -n 1 | awk '{print "/proc/"$1"/status"}' | xargs -n 1 grep VmSize | awk '{print $2}' | awk '{sum +=1 $1} END {print sum}')

vdevlist=$(mktemp)

find /etc/swift -name 20_settings.conf | grep account-server   | xargs grep devices | awk '{print $3}' >> $vdevlist
find /etc/swift -name 20_settings.conf | grep container-server | xargs grep devices | awk '{print $3}' >> $vdevlist
find /etc/swift -name 20_settings.conf | grep object-server    | xargs grep devices | awk '{print $3}' >> $vdevlist

pdevspace=$(mktemp)

(cat $vdevlist | sort | uniq | xargs df | sort | uniq | grep -v Filesystem | awk '{print $1}' | sort | uniq | xargs df | awk 'NR > 1 {print $3}') > $pdevspace

rm $vdevlist

diskspace=$(cat $pdevspace | awk '{sum += $1} END {print sum}')

rm $pdevspace

echo "smbd/proxyfsd-VmSize(KB) $vmsize Swift-ACO-space(KB) $diskspace"
