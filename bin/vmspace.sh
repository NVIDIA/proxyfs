#!/bin/bash
#
# Output:
#   VmSize (kB) sum for all smbd and proxyfsd processes on this node

vmsize=$(pidof proxyfsd smbd | xargs -n 1 | awk '{print "/proc/"$1"/status"}' | xargs -n 1 grep VmSize | awk '{print $2}' | awk '{sum +=1 $1} END {print sum}')

echo "smbd/proxyfsd-VmSize(KB) $vmsize"
