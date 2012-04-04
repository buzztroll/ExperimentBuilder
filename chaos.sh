#!/bin/bash

p=`dirname $0`
cd $p

./chaos.py ${@} &
