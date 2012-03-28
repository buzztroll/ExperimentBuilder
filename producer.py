import os
import sys
from kombu import BrokerConnection, Exchange, Queue, Producer


filename = "meta"
fptr = open(filename, "r")
amqpurl = fptr.readline().strip()
exchange_name = fptr.readline().strip()

print exchange_name
print amqpurl
exchange = Exchange(exchange_name, type="direct")
D_queue = Queue(exchange_name, exchange, routing_key=exchange_name, auto_delete=False, exclusive=False)
connection = BrokerConnection(amqpurl)
channel = connection.channel()

queue = D_queue(channel)
queue.declare()
producer = Producer(channel, exchange, routing_key=exchange_name)

total_workers = int(sys.argv[1])
imgsize = int(sys.argv[2])
name = sys.argv[3]

s3url = ""
if 'EC2_URL' in os.environ:
    s3url = os.environ['EC2_URL']
s3id = os.environ['EC2_ACCESS_KEY']
s3pw = os.environ['EC2_SECRET_KEY']

for i in range(0, total_workers):
    msg = {'program': 'python node.py %d %d %d' % (i, total_workers, imgsize),
            'rank': i,
            's3url': s3url,
            's3id': s3id,
            's3pw': s3pw,
            'testname': name}
    print "sending message %s" % (str(msg))
    producer.publish(msg,
                 exchange=exchange,
                 routing_key=exchange_name,
                 serializer="json")

