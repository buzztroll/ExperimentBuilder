from datetime import datetime, timedelta
import logging
import os
import socket
import sys
from kombu import BrokerConnection, Exchange, Queue, Producer
from dashi import DashiConnection
import uuid
import urlparse
import boto
from boto.ec2.connection import EC2Connection
from boto.regioninfo import RegionInfo
import random
import boto.ec2.autoscale

g_time_interval = 300
g_next_kill = None
g_to_kill_count = 0
g_client_started = False
g_done_count = 0
logging.basicConfig()
logger = logging.getLogger("dashi")
logger.setLevel(logging.INFO)


def kill_ready():
    global g_next_kill
    global g_time_interval
    n = datetime.now()
    if g_next_kill is None:
        g_next_kill = n + timedelta(seconds=g_time_interval)
    if n > g_next_kill:
        g_next_kill = n + timedelta(seconds=g_time_interval)
        return True
    return False

def get_phantom_con(s3id, s3pw):
    url = os.environ['PHANTOM_URL']
    print "get phatom con"
    uparts = urlparse.urlparse(url)
    is_secure = uparts.scheme == 'https'
    region = RegionInfo(uparts.hostname)
    con = boto.ec2.autoscale.AutoScaleConnection(aws_access_key_id=s3id, aws_secret_access_key=s3pw, is_secure=is_secure, port=uparts.port, region=region)
    con.host = uparts.hostname
    return con

def get_boto_con():
    id = os.environ['EC2_ACCESS_KEY']
    pw = os.environ['EC2_SECRET_KEY']
    cloud_url = os.environ['EC2_URL']
    uparts = urlparse.urlparse(cloud_url)
    is_secure = uparts.scheme == 'https'
    ec2conn = EC2Connection(id, pw, host=uparts.hostname, port=uparts.port, is_secure=is_secure)
    ec2conn.host = uparts.hostname
    return ec2conn

def get_instances(ec2conn, id_list):
    instance_list = []
    all_inst = ec2conn.get_all_instances()
    for res in all_inst:
        for i in res.instances:
            if i.state == 'running':
                instance_list.append(i)
    return instance_list


def kill_one(p_con, name):
    asg_a = p_con.get_all_groups(names=[name,])
    if not asg_a:
        print "no group"
        return False
    instances = asg_a[0].instances
    inst_ids = [i.instance_id for i in instances]
    boto_con = get_boto_con()
    insts = get_instances(boto_con, inst_ids)

    inst = random.choice(insts)
    inst.terminate()

    n = datetime.now()
    print "XXX killing %s %s %s %s" % (str(inst), str(inst.ip_address), inst.public_dns_name, str(n))
    return True


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

    global g_client_started
    if not g_client_started:
        g_client_started = True
        count_str = 'CHAOS_KILL_COUNT'
        if count_str in os.environ:
            count = int(os.environ[count_str])
            global g_to_kill_count
            g_to_kill_count = count

            g_next_kill
            

def get_dashi_connection(amqpurl, name):
    exchange = "default_dashi_exchange"
    dashi = DashiConnection(name, amqpurl, exchange, ssl=False)
    dashi.handle(client_finished, "done")
    dashi.handle(client_started, "start")
    return dashi

def wait_till_done(dashi, total, p_con, name):
    global g_done_count
    global g_to_kill_count
    while g_done_count < total:
        try:
            dashi.consume(count=1, timeout=5)
        except socket.timeout, ex:
            pass
        if g_to_kill_count > 0 and kill_ready():
            rc = kill_one(p_con, name)
            if rc:
                g_to_kill_count = g_to_kill_count - 1

def main():
    filename = "meta"
    fptr = open(filename, "r")
    amqpurl = fptr.readline().strip()
    exchange_name = fptr.readline().strip()

    exchange = Exchange(exchange_name, type="direct")
    D_queue = Queue(exchange_name, exchange, routing_key=exchange_name, auto_delete=False, exclusive=False)


    connection = BrokerConnection(amqpurl)
    print amqpurl
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

    dashi = get_dashi_connection(amqpurl, dashi_name)
    p_con = get_phantom_con(s3id, s3pw)
    wait_till_done(dashi, total_workers, p_con, name)

    n = datetime.now()
    print "XXX done %s" % (str(n))


if __name__ == "__main__":  
    rc = main()
    sys.exit(rc)
