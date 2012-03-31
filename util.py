import boto
import logging
from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.connection import S3Connection
import os
import socket
from subprocess import Popen, PIPE
import tempfile
import sys
import urlparse
import bz2
logging.basicConfig(level=logging.DEBUG)
from kombu import BrokerConnection, Exchange, Queue, Consumer
from dashi import DashiConnection

def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("gmail.com",80))
    ip = s.getsockname()[0]
    s.close()
    return ip

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

    def get_dashi_connection(self, amqpurl):
        exchange = "default_exchange"
        print "using dashi info: %s, %s, %s" % (self.worker_queue_name, amqpurl, exchange)
        dashi = DashiConnection(self.worker_queue_name, amqpurl, exchange, ssl=False)
        return dashi

    def _get_from_gitfile(self):
        filename = "/usr/local/src/ExperimentBuilder/meta"
        fptr = open(filename, "r")
        self.amqpurl = fptr.readline().strip()
        self.worker_queue_name = fptr.readline().strip()

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
                    return None
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
            self.dashi.fire(self.dashiname, "start", rank=self.rank, hostname=self.host_id, message="CLIENT_CHECKPOINT_%s" % (line))
            self.upload_stage_file(line)
            self.get_stage_file()

    def get_s3_conn(self, m):
        s3url = m['s3url']
        s3id = m['s3id']
        s3pw = m['s3pw']

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
        self.dashi = self.get_dashi_connection(self.amqpurl)
        self.done = False
        self.dashi.handle(self.work, "work_queue")

        while not self.done:
            print "X"
            self.dashi.consume(count=1)


    def work(self, workload=None):
        print "work call received"
        self.done = True
        exe = workload['program']
        self.rank = int(workload['rank'])
        self.testname = workload['testname']

        self.dashiname = workload['dashiname']
        self.dashi.fire(self.dashiname, "start", rank=self.rank, hostname=self.host_id, message="CLIENT_START")

        self.get_s3_conn(workload)

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

        print "sending dashi done message to %s" % (self.dashiname)
        self.dashi.fire(self.dashiname, "done", rank=self.rank, hostname=self.host_id)


def main(argv=sys.argv):
    print "client"
    core = sys.argv[1]
    cw = ClientWorker(core)
    cw.run()

if __name__ == "__main__":
    rc = main()
    sys.exit(rc)
