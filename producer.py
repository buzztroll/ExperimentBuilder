from datetime import datetime
import logging
import os
import sys
from kombu import BrokerConnection, Exchange, Queue, Producer
from dashi import DashiConnection
import uuid
from kombu.transport.librabbitmq import Connection
import urlparse


g_done_count = 0
logging.basicConfig()
logger = logging.getLogger("dashi")
logger.setLevel(logging.INFO)

def client_finished(rank=None, hostname=None, time=None):
    global g_done_count
    n = datetime.now()
    print "XXX CLIENT_DONE %d %s %s || %s" % (rank, hostname, n, time)
    sys.stdout.flush()
    g_done_count = g_done_count + 1

def client_started(rank=None, hostname=None, message=None, time=None):
    n = datetime.now()
    print "XXX %s %d %s %s || %s" % (message.strip(), rank, hostname, n, time)
    sys.stdout.flush()

def get_dashi_connection(amqpurl, name, total):
    exchange = "default_dashi_exchange"
    dashi = DashiConnection(name, amqpurl, exchange, ssl=False)
    dashi.handle(client_finished, "done")
    dashi.handle(client_started, "start")
    global g_done_count
    while g_done_count < total:
        dashi.consume(count=1)

def main():
    filename = "meta"
    fptr = open(filename, "r")
    amqpurl = fptr.readline().strip()
    exchange_name = fptr.readline().strip()

    exchange = Exchange(exchange_name, type="direct")
    D_queue = Queue(exchange_name, exchange, routing_key=exchange_name, auto_delete=False, exclusive=False)


    #connection = BrokerConnection(amqpurl)

    u = self.amqpurl.replace('amqp', 'http')
    parts = urlparse.urlparse(u)
    connection = Connection(host=parts.hostname, userid=parts.username, password=parts.password, port=parts.port, heartbeat=30)
    
    channel = connection.channel()

    queue = D_queue(channel)
    queue.declare()
    producer = Producer(channel, exchange, routing_key=exchange_name)

    total_workers = int(sys.argv[1])
    imgsize = int(sys.argv[2])
    name = sys.argv[3]

    s3url = ""
    if 'S3_URL' in os.environ:
        s3url = os.environ['S3_URL']
    s3id = os.environ['EC2_ACCESS_KEY']
    s3pw = os.environ['EC2_SECRET_KEY']

    n = datetime.now()
    print "XXX starting %s" % (str(n))

    dashi_name = str(uuid.uuid4()).split('-')[0]
    for i in range(0, total_workers):
        msg = {'program': 'python node.py %d %d %d' % (i, total_workers, imgsize),
                'rank': i,
                's3url': s3url,
                's3id': s3id,
                's3pw': s3pw,
                'testname': name,
                'dashiname': dashi_name}
        print str(msg)
        sys.stdout.flush()
        producer.publish(msg,
                     exchange=exchange,
                     routing_key=exchange_name,
                     serializer="json")

    get_dashi_connection(amqpurl, dashi_name, total_workers)

    n = datetime.now()
    print "XXX done %s" % (str(n))


if __name__ == "__main__":
    rc = main()
    sys.exit(rc)
