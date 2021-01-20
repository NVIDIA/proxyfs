#!/bin/bash

# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

while true
do
	for i in `seq 100`
	do
		dd if=/dev/zero of=$1/dd1M_$i bs=4k count=256
	done
done
