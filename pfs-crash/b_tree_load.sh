#!/bin/bash

# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

while true
do
    for j in `seq 10`
    do
        for k in `seq 100`
        do
            echo "Hi" >> $1/f_$k
        done
    done
    for k in `seq 100`
    do
        rm $1/f_$k
    done
done
