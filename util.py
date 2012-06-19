import boto
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
from kombu import BrokerConnection, Exchange, Queue, Consumer
import os
import socket
from subprocess import Popen, PIPE
import tempfile
import sys
from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.connection import S3Connection
import urlparse
import bz2
from dashi import DashiConnection
from kombu.transport.librabbitmq import Connection


def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("gmail.com",80))
    ip = s.getsockname()[0]
    s.close()
    return ip

def get_dashi_connection(amqpurl):
    exchange = "default_dashi_exchange"
    name = "nimbusclient"
    print "using dashi info: %s, %s, %s" % (name, amqpurl, exchange)
    dashi = DashiConnection(name, amqpurl, exchange, ssl=False)
    return dashi

class EPMessage(object):

    def __init__(self, m):
        self.message = m

    def get_rank_total(self):
        return (self.message.payload['rank'], self.message.payload['total_workers'])

    def get_parameter(self, name):
        return self.message.payload[name]

    def done_with_it(self):
        self.message.ack()

class ClientWorker(object):

    def __init__(self, core):
        self.checkpoint_threshold = 500000
        self.checkpoint_token = "CHECKPOINT:"
        self.s3conn = None
        self.rank = None
        self.testname = None
        self._get_from_gitfile()
        self.output_file = None
        self.core = core
        self.ip = get_ip()
        self.host_id = "%s__%s" % (self.ip, str(self.core))
        self.message = None

    def _get_from_gitfile(self):
        filename = "/usr/local/src/ExperimentBuilder/meta"
        fptr = open(filename, "r")
        self.amqpurl = fptr.readline().strip()
        self.testname = fptr.readline().strip()

    def get_latest_checkpoint(self):
        b = self.s3conn.get_bucket(self.bucketname)
        checkpoint = 0
        for k in b.list():
            name = k.name.replace(".bz2", "")

            rank_ndx =name.find('.')
            ndx = name.rfind('.') + 1
            rank_i = int(name[rank_ndx+1:ndx-1])
            if rank_i == self.rank:
                m = name[ndx:]
                if m == "final":
                    # return the last checkpoint.  if final is there but the message was not acked something could
                    # be wrong
                    return checkpoint
                i = int(m)
                if i > checkpoint:
                    checkpoint = i
        print "Checkpoint is %d" % (checkpoint)
        return checkpoint

    def get_stage_file(self):
        (self.stage_osf, self.stage_fname) = tempfile.mkstemp()
        os.close(self.stage_osf)
        self.output_file = bz2.BZ2File(self.stage_fname, "w")
        self.checkpoint_ctr = 0
        print "staging output to %s" % (self.stage_fname)

    def upload_stage_file(self, line):
        self.output_file.close()
        checkpoint_n = line.replace(self.checkpoint_token, "")
        print checkpoint_n
        key_file_name = "%s.%d.%s" % (self.testname, self.rank, checkpoint_n)
        k = boto.s3.key.Key(self.bucket)

        st = os.stat(self.stage_fname)
        print "file size is %d" %(st.st_size)

        k.key = key_file_name
        print "uploading %s to %s" % (self.stage_fname, key_file_name)
        k.set_contents_from_filename(self.stage_fname)
        os.remove(self.stage_fname)

    def test_for_checkpoint_time(self, line):
        self.checkpoint_ctr = self.checkpoint_ctr + 1
        if self.checkpoint_ctr > self.checkpoint_threshold:
            n = str(datetime.utcnow())
            self.dashi.fire(self.dashiname, "start", rank=self.rank, hostname=self.host_id, message="CLIENT_CHECKPOINT_%s" % (line), time=n)
            self.upload_stage_file(line)
            self.get_stage_file()

    def get_s3_conn(self, m):
        s3url = m.get_parameter('s3url')
        s3id = m.get_parameter('s3id')
        s3pw = m.get_parameter('s3pw')

        host = None
        port = None
        is_secure = True
        path = "expr1"
        if s3url:
            parts = urlparse.urlparse(s3url)
            host = parts.hostname
            port = parts.port
            is_secure = parts.scheme == "https"
            path = parts.path

        print "%s %s %s" % (s3id, s3pw, s3url)
        cf = OrdinaryCallingFormat()
        if s3url:
            self.s3conn = S3Connection(s3id, s3pw, host=host, port=port, is_secure=is_secure, calling_format=cf)
        else:
            self.s3conn = S3Connection(s3id, s3pw, calling_format=cf)

        bucketname = self.testname
        while bucketname[0] == "/":
            bucketname = bucketname[1:]
        self.bucketname = bucketname
        try:
            print "making bucket %s" % (self.bucketname)
            self.s3conn.create_bucket(self.bucketname)
        except Exception, ex:
            print ex

        self.bucket = self.s3conn.get_bucket(bucketname)

    def run(self):
        print "exchange = %s, queue = %s, routing_key = %s, amqpurl = %s" % (self.testname, self.testname, self.testname, self.amqpurl)
        exchange = Exchange(self.testname, type="direct")
        D_queue = Queue(self.testname, exchange, routing_key=self.testname, exclusive=False)
        connection = BrokerConnection(self.amqpurl)

        #u = self.amqpurl.replace('amqp', 'http')
        #parts = urlparse.urlparse(u)

        #connection = Connection(host=parts.hostname, userid=parts.username, password=parts.password, port=parts.port, heartbeat=30)

        channel = connection.channel()
        queue = D_queue(channel)
        queue.declare()
        consumer = Consumer(channel, queue, callbacks=[self.work])
        consumer.qos(prefetch_size=0, prefetch_count=1, apply_global=False)
        self.done = False
        consumer.consume(no_ack=False)
        print "about to drain"
        while  not self.done:
            connection.drain_events()
        self.real_work()

    def work(self, body, message):
        print "work call received"
        self.message = message
        self.done = True

    def real_work(self):
        m = EPMessage(self.message)
        exe = m.get_parameter('program')
        self.rank = int(m.get_parameter('rank'))
        self.testname = m.get_parameter('testname')

        self.dashiname = m.get_parameter('dashiname')
        self.dashi = get_dashi_connection(self.amqpurl)
        n = str(datetime.utcnow())
        self.dashi.fire(self.dashiname, "start", rank=self.rank, hostname=self.host_id, message="CLIENT_START", time=n)

        print "my rank is %d" % (self.rank)

        self.get_s3_conn(m)

        checkpoint = self.get_latest_checkpoint()
        if checkpoint is None:
            raise Exception("This one is already complete")
        exe = "%s %d" % (exe, checkpoint)
        p = Popen(exe, shell=True, bufsize=1024*1024, stdout=PIPE)

        self.get_stage_file()
        line = p.stdout.readline()
        while line:
            ndx = line.find(self.checkpoint_token)
            if ndx == 0:
                self.test_for_checkpoint_time(line)
            else:
                self.output_file.write(line)
            line = p.stdout.readline()

        self.upload_stage_file("%sfinal" % (self.checkpoint_token))
        m.done_with_it()

        print "sending dashi done message to %s" % (self.dashiname)
        n = str(datetime.utcnow())
        self.dashi.fire(self.dashiname, "done", rank=self.rank, hostname=self.host_id, time=n)


def main(argv=sys.argv):
    print "client"
    core = sys.argv[1]
    cw = ClientWorker(core)
    cw.run()

if __name__ == "__main__":
    rc = main()
    sys.exit(rc)
