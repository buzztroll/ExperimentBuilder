#!/usr/bin/env python

from datetime import datetime
from boto.ec2.connection import EC2Connection
import os
import random
import sys
import urlparse
import time

def get_node_count(ec2conn, image_name="expr2.gz"):
    instance_list = []
    all_inst = ec2conn.get_all_instances()
    for res in all_inst:
        for i in res.instances:
            if i.image_id == image_name and i.state == 'running':
                instance_list.append(i)
    return instance_list

def get_boto_con():
    id = os.environ['EC2_ACCESS_KEY']
    pw = os.environ['EC2_SECRET_KEY']
    cloud_url = os.environ['EC2_URL']
    uparts = urlparse.urlparse(cloud_url)
    is_secure = uparts.scheme == 'https'
    ec2conn = EC2Connection(id, pw, host=uparts.hostname, port=uparts.port, is_secure=is_secure)
    ec2conn.host = uparts.hostname

    return ec2conn

def kill_a_vm(boto_con):
    instlist = get_node_count(boto_con)
    if not instlist:
        return False
    inst = random.choice(instlist)
    inst.terminate()


    
    n = datetime.now()
    print "XXX killing %s %s" % (str(inst), str(n))
    return True
    

def main(argv=sys.argv):
    interval = int(argv[1])
    to_kill_count = int(argv[1])

    boto_con = get_boto_con()

    random.seed()

    for i in range(0, to_kill_count):
        time.sleep(interval)
        kill_a_vm(boto_con)


    return 0

if __name__ == "__main__":
    rc = main()
    sys.exit(rc)
