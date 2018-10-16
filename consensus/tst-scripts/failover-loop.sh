#!/bin/bash -x 

i="myTestVg1"
n=etcd0
#for n in etcd0 etcd1 etcd2
while :
do
	sleep 120
	../hacli/hacli offline -vg $i
	../hacli/hacli online -vg $i -node $n

	if [ X"$i" ==  X"myTestVg1" ]
	then
		i="myTestVg2"
	else
		i="myTestVg1"
	fi

	if [ X"$n" ==  X"etcd0" ]
	then
		n="etcd1"
	else
		if [ X"$n" ==  X"etcd1" ]
		then
			n="etcd2"
		else
			n="etcd0"
		fi
	fi
done
