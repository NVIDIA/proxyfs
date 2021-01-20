#!/bin/bash

# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

while true
do
	fio --name=writetest --size=100MiB --bs=4k --rw=randwrite --direct=1 --buffered=0 --iodepth=32 --filename=$1/fio_scratch_file
done
