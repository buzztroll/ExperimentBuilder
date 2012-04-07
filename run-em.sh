#!/bin/bash

mkdir /tmp/exprdata
logfile="/tmp/exprdata/overall.log"
touch $logfile

unset CHAOS_KILL_COUNT

for i in `seq 0 10`
do
    echo "XXXXXXXXXXXXXX starting $i XXXXXXXXXXXXXXXXXXXXX" >> $logfile
    python cleanup.py >> $logfile

    run_logfile="/tmp/exprdata/run$i.log"

    python runtest_new_domain.py data$i round$i > $run_logfile

    echo "waiting a minute for no good reason..."
    sleep 60

done