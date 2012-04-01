from datetime import datetime
import logging
import time

logging.basicConfig(level=logging.DEBUG)
import os
import sys
from kombu import BrokerConnection, Exchange, Queue, Producer
from dashi import DashiConnection
import uuid

g_done_count = 0

def client_finished(rank=None, hostname=None):
    global g_done_count
    n = datetime.now()
    print "XXX CLIENT_DONE %d %s %s" % (rank, hostname, n)
    g_done_count = g_done_count + 1

def client_started(rank=None, hostname=None, message=None):
    n = datetime.now()
    print "XXX %s %d %s %s" % (message, rank, hostname, str(n))

def get_dashi_connection(amqpurl, name, exchange):
    dashi = DashiConnection(name, amqpurl, exchange, ssl=False, serializer='json')
    return dashi

def fake_cb(workload=None):
    print "NO!!!!!!!!!"

def dashi_wait(dashi, total):
    global g_done_count

    while g_done_count < total:
        print "X"
        dashi.consume(count=1)

def main():
    filename = "meta"
    fptr = open(filename, "r")
    amqpurl = fptr.readline().strip()
    worker_queue_name = fptr.readline().strip()

    dashi_name = str(uuid.uuid4()).split('-')[0]
    print dashi_name

    fake_dashi_con = get_dashi_connection(amqpurl, worker_queue_name, 'default_exchange')
    fake_dashi_con.handle(fake_cb, "work_queue")

    dashi_con = get_dashi_connection(amqpurl, dashi_name, 'default_exchange')
    #dashi_con = fake_dashi_con
    dashi_con.handle(client_finished, "done")
    dashi_con.handle(client_started, "start")

    total_workers = int(sys.argv[1])
    imgsize = int(sys.argv[2])
    name = sys.argv[3]

    s3url = ""
    if 'S3_URL' in os.environ:
        s3url = os.environ['S3_URL']
    s3id = os.environ['EC2_ACCESS_KEY']
    s3pw = os.environ['EC2_SECRET_KEY']

    n = datetime.now()
    print "XXX PRODUCER_STARTING %s" % (str(n))

    dashi_name = str(uuid.uuid4()).split('-')[0]
    print "dashi name %s" % (dashi_name)
    for i in range(0, total_workers):
        msg = {'program': 'python node.py %d %d %d' % (i, total_workers, imgsize),
                'rank': i,
                's3url': s3url,
                's3id': s3id,
                's3pw': s3pw,
                'testname': name,
                'dashiname': dashi_name}
        print "fire %s work_queue %s" % (worker_queue_name, str(msg))
        dashi_con.fire(worker_queue_name, "work_queue", workload="")

    print "waiting for dashi"
    dashi_wait(dashi_con, total_workers)
    n = datetime.now()
    print "XXX PRODUCER_DONE %s" % (str(n))


if __name__ == "__main__":
    rc = main()
    sys.exit(rc)
