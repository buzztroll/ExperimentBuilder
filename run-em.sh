#!/bin/bash

logdir="/usr/local/exprdata-`date +%s`"
rm -rf $logdir
mkdir $logdir
logfile="$logdir/overall.log"
touch $logfile

echo $logdir

unset CHAOS_KILL_COUNT

for i in `seq 1 10`
do
    echo "XXXXXXXXXXXXXX starting $i XXXXXXXXXXXXXXXXXXXXX" tee -a $logfile
    python cleanup.py tee -a $logfile

    sleep 60
    run_logfile="$logdir/run$i.log"

    python runtest_new_domain.py data$i round$i | tee $run_logfile

    echo "waiting a minute for no good reason..."
    sleep 60

    export CHAOS_KILL_COUNT=$i
done
