#!/bin/bash

while [ 1 ];
do
 git pull
 sleep 2
 echo "starting"
 python util.py
 echo "ended"
done
