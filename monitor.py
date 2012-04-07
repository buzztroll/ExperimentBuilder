import boto
from boto.ec2.connection import EC2Connection
from boto.regioninfo import RegionInfo
from datetime import datetime
import os
import sys
import urlparse
import boto.ec2.autoscale

def get_phantom_con(url, s3id, s3pw):
    print "get phatom con"
    uparts = urlparse.urlparse(url)
    is_secure = uparts.scheme == 'https'
    region = RegionInfo(uparts.hostname)
    con = boto.ec2.autoscale.AutoScaleConnection(aws_access_key_id=s3id, aws_secret_access_key=s3pw, is_secure=is_secure, port=uparts.port, region=region)
    con.host = uparts.hostname

    return con


def check_state_asg(asg):
    inst = asg.instances
    # get instances and kill them
    #host = 's83r.idp.sdsc.futuregrid.org'
    #host = 'svc.uc.futuregrid.org'
    #ec2conn = EC2Connection(s3id, s3pw, host=host, port=8444, debug=0)
    #ec2conn.host = host
    #all_inst = ec2conn.get_all_instances(instance_ids=inst_list)
    for i in asg_a[0].instances:
        print i.lifecycle_state


s3id = os.environ['EC2_ACCESS_KEY']
s3pw = os.environ['EC2_SECRET_KEY']
url = os.environ['PHANTOM_URL']

print "getting phantom con"
con = get_phantom_con(url, s3id, s3pw)
print "have phantom con"

asg_name = sys.argv[1]
asg_a = con.get_all_groups(names=[asg_name,])
if not asg_a:
    raise Exception('No group named %s' % (asg_name))
check_state_asg(asg_a[0])

