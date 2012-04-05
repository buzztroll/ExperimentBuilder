import boto
from boto.ec2.connection import EC2Connection
from kombu import BrokerConnection
import os
from subprocess import Popen, PIPE
import tempfile
import urllib
import sys
from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.connection import S3Connection
import urlparse
from kombu import BrokerConnection, Exchange, Queue, Consumer


done = False

def work(body, message):
    global done
    done = True
    message.ack()

filename = "meta"
fptr = open(filename, "r")
amqpurl = fptr.readline().strip()
exchange_name = fptr.readline().strip()
try:
    exchange = Exchange(exchange_name, type="direct")
    D_queue = Queue(exchange_name, exchange, routing_key=exchange_name, exclusive=False)
    connection = BrokerConnection(amqpurl)
    channel = connection.channel()
    queue = D_queue(channel)
    queue.declare()
    consumer = Consumer(channel, queue, callbacks=[work])
    consumer.qos(prefetch_size=0, prefetch_count=1, apply_global=False)
    consumer.consume(no_ack=False)
    print "about to drain"
    for i in range(0, 25):
        try:
            connection.drain_events(timeout=1)
        except:
            pass
except Exception, ex:
    print ex
s3url = os.environ['EC2_URL']
s3id = os.environ['EC2_ACCESS_KEY']
s3pw = os.environ['EC2_SECRET_KEY']

parts = urlparse.urlparse(s3url)
host = parts.hostname
port = parts.port
is_secure = parts.scheme == "https"
path = parts.path

ec2conn = EC2Connection(s3id, s3pw, host=host, port=port, debug=0, is_secure=is_secure)
ec2conn.host = host
all_inst = ec2conn.get_all_instances()
for res in all_inst:
    for i in res.instances:
        print "terminating %s" % (str(i))
        i.terminate()

