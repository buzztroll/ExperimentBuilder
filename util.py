from kombu import BrokerConnection
import os
from subprocess import Popen, PIPE
import tempfile
import urllib
import sys

class EPInfo(object):

    def __init__(self):
        # get data
        self.queue = None
        self._get_from_gitfile()

    def _get_from_gitfile(self):
        filename = "/usr/local/src/ExperimentBuilder/meta"
        fptr = open(filename, "r")
        self.amqpurl = fptr.readline().strip()
        self.testname = fptr.readline().strip()

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
        queue = connection.SimpleQueue(self.testname, serializer='json')
        self.queue = queue
        return self.queue


message_format = {
    'program_prep': None,
    'program': None,
    'result_location': None,
}

class EPMessage(object):

    def __init__(self, queue):
        self.message = queue.get()

    def get_rank_total(self):
        return (self.message.payload['rank'], self.message.payload['total_workers'])

    def get_parameter(self, name):
        return self.message.payload[name]

    def done_with_it(self):
        self.message.ack()

def client_worker_main():
    EPI = EPInfo()
    q = EPI.get_kombu_queue()
    m = EPMessage(q)
    exe = m.get_parameter('program')
    print "running %s" % (exe)
    p = Popen(exe, shell=True, bufsize=1024*1024, stdout=PIPE)

    (osf, outfname) = tempfile.mkstemp()
    print "staging output to %s" % (outfname)

    checkpoint_threshold = 1000
    checkpoint_ctr = 0
    checkpoint_n = 0
    token = "CHECKPOINT:"
    line = p.stdout.readline()
    while line:
        ndx = line.find(token)
        if ndx == 0:
            checkpoint_ctr = checkpoint_ctr + 1
            if checkpoint_ctr > checkpoint_threshold:
                checkpoint_n = int(line[len(token):])
                checkpoint_ctr = 0
                print line + " " + checkpoint_n
        else:
            os.write(osf, line)

        line = p.stdout.readline()
    os.close(osf)
    m.done_with_it()
    
def prep_messages():
    total_workers = 1
    EPI = EPInfo()
    queue = EPI.get_kombu_queue()
    for i in range(0, total_workers):
        msg = {'program': 'python node.py 0 %d %d 1024' % (i, total_workers)}
        queue.put(msg, serializer='json')

def main(argv=sys.argv):
    if len(argv) > 1:
        print "preping messages"
        prep_messages()
    else:
        print "client"
        client_worker_main()

if __name__ == "__main__":
    rc = main()
    sys.exit(rc)
