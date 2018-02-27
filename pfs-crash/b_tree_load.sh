#!/bin/bash
for i in `seq 100`;do
  echo "Iteration $i"
  for j in `seq 10`;do
    for k in `seq 100`;do
      echo "Hi" >> CommonMountPoint/f_$k
    done
  done
    for k in `seq 100`;do
      rm CommonMountPoint/f_$k
    done
done
