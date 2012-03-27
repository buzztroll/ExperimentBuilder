from kombu import BrokerConnection
import os
import urllib
import sys

class EPInfo(object):

    def __init__(self):
#        nimbus_filename = '/var/nimbus-metadata-server-url'
#        url = "http://169.254.169.254"
#        if os.path.exists(nimbus_filename):
#            fptr = open(nimbus_filename)
#            url = fptr.readline()
#            fptr.close()
#        url = url + "/latest/user-data"
#
#        fu = urllib.urlopen(url)
#        data = fu.readlines()
#        self.amqpurl = data[0]
#        self.testname = data[1]

        self.amqpurl = "amqp://guest:guest@localhost:5672//" #data[0]
        self.testname = "XXX"
        self.queue = None

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
    rc = os.system(exe)
    if rc != 0:
        raise Exception('the program %s failed' % (exe))
    m.done_with_it()
    
def prep_messages():
    total_workers = 1
    EPI = EPInfo()
    queue = EPI.get_kombu_queue()
    for i in range(0, total_workers):
        msg = {'program': '/bin/sleep 50', 'total': total_workers, 'rank': i, 'output_url': ""}
        queue.put(msg, serializer='json')

def main(argv=sys.argv):
    if len(argv) > 1:
        print "preping messages"
        prep_messages()
    else:
        print "client"
        client_worker_main()

main()