#!/bin/bash
while true
do
	for i in `seq 100`
	do
		dd if=/dev/zero of=$1/dd1M_$i bs=4k count=256
	done
done
