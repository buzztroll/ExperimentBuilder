#!/bin/bash

size=2048
total=4
for i in `seq 0 $total`
do
    time python node.py ./ $i $total $size &
done

wait

time python merge.py ./ $size mand$size-$total.png
