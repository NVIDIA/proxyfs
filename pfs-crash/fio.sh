#!/bin/bash
while true
do
	fio --name=writetest --size=100MiB --bs=4k --rw=randwrite --direct=1 --buffered=0 --iodepth=32 --filename=$1/fio_scratch_file
done
