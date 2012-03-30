#!/bin/bash

proc_count=`cat /proc/cpuinfo | grep processor | wc -l`

while [ 1 ];
do
 git pull
 sleep 2
 echo "starting"
 for i in `seq 1 $proc_count`
 do
    echo $i
    python util.py &
 done
 wait
 echo "ended"
done
