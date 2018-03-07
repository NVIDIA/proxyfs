#!/bin/bash
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
