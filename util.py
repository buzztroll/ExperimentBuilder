import boto
from kombu import BrokerConnection, Exchange, Queue, Consumer
import os
from subprocess import Popen, PIPE
import tempfile
import urllib
import sys
from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.connection import S3Connection
import urlparse
import time


class EPInfo(object):

    def __init__(self):
        # get data
        self.queue = None
        self._get_from_gitfile()


    def _get_from_metadata(self):
        nimbus_filename = '/var/nimbus-metadata-server-url'
        url = "http://169.254.169.254"
        if os.path.exists(nimbus_filename):
            fptr = open(nimbus_filename, "r")
            url = fptr.readline()
            fptr.close()
        url = url + "/latest/user-data"

        fu = urllib.urlopen(url)
        data = fu.readlines()
        self.amqpurl = data[0]
        self.testname = data[1]

    def get_kombu_queue(self):
        if self.queue:
            return self.queue

        connection = BrokerConnection(self.amqpurl)
        connection.connect()
        exchange = Exchange(name=self.testname, type='direct',
                                  durable=False, auto_delete=False)
        queue = Queue(name=self.testname, exchange=exchange, routing_key=self.testname,
                         exclusive=True, durable=False, auto_delete=True)
        #queue.declare()

        self.connection = connection


message_format = {
    'program_prep': None,
    'program': None,
    'result_location': None,
}

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

    def __init__(self):
        self.checkpoint_threshold = 500000
        self.checkpoint_token = "CHECKPOINT:"
        self.s3conn = None
        self.rank = None
        self.testname = None
        self._get_from_gitfile()

    def _get_from_gitfile(self):
        filename = "/usr/local/src/ExperimentBuilder/meta"
        fptr = open(filename, "r")
        self.amqpurl = fptr.readline().strip()
        self.testname = fptr.readline().strip()

    def get_latest_checkpoint(self):
        b = self.s3conn.get_bucket(self.bucketname)
        checkpoint = 0
        for k in b.list():
            rank_ndx = k.name.find('.')
            ndx = k.name.rfind('.') + 1
            rank_i = int(k.name[rank_ndx+1:ndx-1])
            if rank_i == self.rank:

                m = k.name[ndx:]
                if m == "final":
                    return None
                i = int(m)
                if i > checkpoint:
                    checkpoint = i
        print "Checkpoint is %d" % (checkpoint)
        return checkpoint

    def get_stage_file(self):
        (self.stage_osf, self.stage_fname) = tempfile.mkstemp()
        self.checkpoint_ctr = 0
        print "staging output to %s" % (self.stage_fname)

    def upload_stage_file(self, line):
        checkpoint_n = line.replace(self.checkpoint_token, "")
        print checkpoint_n
        key_file_name = "%s.%d.%s" % (self.testname, self.rank, checkpoint_n)
        k = boto.s3.key.Key(self.bucket)
        k.key = key_file_name
        os.close(self.stage_osf)
        k.set_contents_from_filename(self.stage_fname)
        os.remove(self.stage_fname)

    def get_s3_conn(self, m):
        s3url = m.get_parameter('s3url')
        s3id = m.get_parameter('s3id')
        s3pw = m.get_parameter('s3pw')

        host = None
        port = None
        is_secure = True
        path = "Expr1"
        if s3url:
            parts = urlparse.urlparse(s3url)
            host = parts.hostname
            port = parts.port
            is_secure = parts.scheme == "https"
            path = parts.path

        print "%s %s %s" % (s3id, s3pw, s3url)
        cf = OrdinaryCallingFormat()
        self.s3conn = S3Connection(s3id, s3pw, host=host, port=port, is_secure=is_secure, calling_format=cf)

        bucketname = path + "nimbus"
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
        exchange = Exchange(self.testname, type="direct")
        D_queue = Queue(self.testname, exchange, routing_key=self.testname, exclusive=False)
        connection = BrokerConnection(self.amqpurl)
        channel = connection.channel()
        queue = D_queue(channel)
        queue.declare()
        consumer = Consumer(channel, queue, callbacks=[self.work])
        consumer.qos(prefetch_size=0, prefetch_count=0, apply_global=False)
        self.done = False
        consumer.consume(no_ack=False)
        print "about to drain"
        while  not self.done:
            connection.drain_events()

    def work(self, body, message):
        print "work call received"
        self.done = True
        m = EPMessage(message)
        exe = m.get_parameter('program')
        self.rank = int(m.get_parameter('rank'))
        self.testname = m.get_parameter('testname')

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
                self.checkpoint_ctr = self.checkpoint_ctr + 1
                if self.checkpoint_ctr > self.checkpoint_threshold:
                    self.upload_stage_file(line)
                    self.get_stage_file()
            else:
                os.write(self.stage_osf, line)
            line = p.stdout.readline()
        self.upload_stage_file("%sfinal" % (self.checkpoint_token))
        m.done_with_it()


def client_worker_main():
    cw = ClientWorker()
    cw.run()


def main(argv=sys.argv):
    print "client"
    client_worker_main()

if __name__ == "__main__":
    rc = main()
    sys.exit(rc)
