#!/bin/bash

dtname=`date +%s`
logdir="/usr/local/exprdata-$dtname"
rm -rf $logdir
mkdir $logdir
logfile="$logdir/overall.log"
touch $logfile

echo $logdir

unset CHAOS_KILL_COUNT

cp rabbitmq.config /etc/rabbitmq/
/etc/init.d/rabbitmq-server restart

for i in `seq 1 11`
do
    echo "XXXXXXXXXXXXXX starting $i XXXXXXXXXXXXXXXXXXXXX" tee -a $logfile
    python cleanup.py tee -a $logfile

    sleep 60
    run_logfile="$logdir/run$i.log"

    echo "running the test $i"
    python runtest_new_domain.py "run$dtname""$i" round$i | tee $run_logfile

    echo "waiting a minute for no good reason..."
    sleep 60

    export CHAOS_KILL_COUNT=$i
done
