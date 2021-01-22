#!/bin/bash

# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

while true
do
	for i in `seq 100`
	do
		touch $1/touch_$i
	done
done
