import boto
from boto.ec2.connection import EC2Connection
from kombu import BrokerConnection
import os
from subprocess import Popen, PIPE
import tempfile
import urllib
import sys
from boto.regioninfo import RegionInfo
from boto.s3.connection import OrdinaryCallingFormat
import boto.ec2.autoscale
import urlparse
from kombu import BrokerConnection, Exchange, Queue, Consumer
import time

done = False

def work(body, message):
    global done
    done = True
    message.ack()

def kill_asgs():
    print "get phatom con"
    url = os.environ['PHANTOM_URL']
    s3id = os.environ['EC2_ACCESS_KEY']
    s3pw = os.environ['EC2_SECRET_KEY']

    uparts = urlparse.urlparse(url)
    is_secure = uparts.scheme == 'https'
    region = RegionInfo(uparts.hostname)

    con = boto.ec2.autoscale.AutoScaleConnection(aws_access_key_id=s3id, aws_secret_access_key=s3pw, is_secure=is_secure, port=uparts.port, region=region)
    con.host = uparts.hostname

    asg_a = con.get_all_groups()

    for asg in asg_a:
        asg.delete()

def kill_instances():
    s3url = os.environ['EC2_URL']
    s3id = os.environ['EC2_ACCESS_KEY']
    s3pw = os.environ['EC2_SECRET_KEY']

    parts = urlparse.urlparse(s3url)
    host = parts.hostname
    port = parts.port
    is_secure = parts.scheme == "https"

    ec2conn = EC2Connection(s3id, s3pw, host=host, port=port, debug=0, is_secure=is_secure)
    ec2conn.host = host
    ec2conn = EC2Connection(s3id, s3pw, host=host, port=8444, debug=0)
    ec2conn.host = host

    # kill everything this account is running on that host
    all_inst = ec2conn.get_all_instances()
    for res in all_inst:
        for i in res.instances:
            print "terminating %s" % (str(i))
            if i.image_id == 'expr2.gz':
                try:
                    i.terminate()
                except Exception, ex:
                    print ex

def queue_drain():
    filename = "meta"
    fptr = open(filename, "r")
    amqpurl = fptr.readline().strip()
    exchange_name = fptr.readline().strip()

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
    for i in range(0, 100):
        try:
            connection.drain_events(timeout=1)
        except:
            pass


try:
    print "killing the asgs"
    kill_asgs()
except Exception, ex:
    print ex

try:
    print "killing all instances"
    kill_instances()
except Exception, ex:
    print ex

os.system("echo 60 > /proc/sys/net/ipv4/tcp_keepalive_time")
print "waiting out the keepalive"
time.sleep(60)
try:
    print "draining the queue"
    queue_drain()
except Exception, ex:
    print ex

os.system("rabbitmqctl list_queues")
os.system("pkill epu-provisioner")
os.system("pkill epu-managem")
os.system("echo 60 > /proc/sys/net/ipv4/tcp_keepalive_time")

