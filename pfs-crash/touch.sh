#!/bin/bash
while true
do
	for i in `seq 100`
	do
		touch $1/touch_$i
	done
done
