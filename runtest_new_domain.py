import boto
from boto.ec2.connection import EC2Connection
from boto.regioninfo import RegionInfo
from datetime import datetime
import os
import sys
import urlparse
import boto.ec2.autoscale

def _find_or_create_config(con, size, image, keyname, lc_name):
    lcs = con.get_all_launch_configurations(names=[lc_name,])
    if not lcs:
        lc = boto.ec2.autoscale.launchconfig.LaunchConfiguration(con, name=lc_name, image_id=image, key_name=keyname, security_groups='default', instance_type=size)
        con.create_launch_configuration(lc)
        return lc
    return lcs[0]

def get_phantom_con(s3id, s3pw):
    print "get Phatom connection..."
    url = os.environ['PHANTOM_URL']
    uparts = urlparse.urlparse(url)
    is_secure = uparts.scheme == 'https'
    region = RegionInfo(uparts.hostname)

    con = boto.ec2.autoscale.AutoScaleConnection(aws_access_key_id=s3id, aws_secret_access_key=s3pw, is_secure=is_secure, port=uparts.port, region=region)
    con.host = uparts.hostname

    return con

def create_autoscale_group(con, name, node_count):
    image = "expr2.gz"
    lc_name = "expr2@%s" % (os.environ['FG_CLOUD_NAME'])
    lc = _find_or_create_config(con, "m1.small", image, "nimbusphantom", lc_name)

    asg_name = name
    print "Create the domain %s" % (asg_name)

    asg = boto.ec2.autoscale.group.AutoScalingGroup(launch_config=lc, connection=con, group_name=asg_name, availability_zones=[os.environ['FG_CLOUD_NAME'],], min_size=node_count, max_size=node_count)
    con.create_auto_scaling_group(asg)
    asg.set_capacity(node_count)

def terminate_asg(con, name, s3id, s3pw):
    url = os.environ['EC2_URL']
    uparts = urlparse.urlparse(url)
    is_secure = uparts.scheme == 'https'

    asg_a = con.get_all_groups(names=[name,])
    inst = asg_a[0].instances
    inst_list = [i.instance_id for i in inst]
    asg_a[0].delete()
    # get instances and kill them
    host = uparts.hostname
#    ec2conn = EC2Connection(s3id, s3pw, host=host, port=8444, debug=0)
#    ec2conn.host = host
#    all_inst = ec2conn.get_all_instances(instance_ids=inst_list)
#    for res in all_inst:
#        for i in res.instances:
#            print "terminating %s" % (str(i))
#            try:
#                i.terminate()
#            except Exception, ex:
#                print ex

def check_state_asg(con, name):
    print "checking on the state of the EPU"
    asg_a = con.get_all_groups(names=[name,])
    for i in asg_a[0].instances:
        print i.lifecycle_state

s3id = os.environ['EC2_ACCESS_KEY']
s3pw = os.environ['EC2_SECRET_KEY']

con = get_phantom_con(s3id, s3pw)

datafile = sys.argv[1]
outf = open(datafile, "w")
picture_size = int(sys.argv[2])
worker_count = int(sys.argv[3])

corepervm = 1
message_count = worker_count * 1

#name = "%s%d_%d_%s_%d" % (datafile, worker_count, picture_size, rnd, message_count)
name = datafile
name = name.lower()

asg_name = name
try:
    terminate_asg(con, asg_name, s3id, s3pw)
except Exception, ex:
    print "a service with the same name existed"
    print ex
extra = 0
if (worker_count % corepervm) > 0:
    extra = 1
node_count = worker_count / corepervm + extra
node_count = worker_count

cmd = "python listbucket.py %s delete" % (name)
os.system(cmd)

start_tm = datetime.now()

print "creating asg %s" % (asg_name)
create_autoscale_group(con, asg_name, node_count)

cmd = "python producer.py %d %d %s" % (message_count, picture_size, name)
print cmd
rc = os.system(cmd)
end_tm = datetime.now()

tm = end_tm - start_tm
outf.write("%s %d %d %d %d\n" % (name, worker_count, picture_size, tm.total_seconds(), message_count))
outf.flush()
print "DDD time %s" % (str(tm))

check_state_asg(con, asg_name)

terminate_asg(con, asg_name, s3id, s3pw)
