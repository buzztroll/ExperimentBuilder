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
    print "get phatom con"
    url = "http://svc.uc.futuregrid.org:8445"
    uparts = urlparse.urlparse(url)
    is_secure = uparts.scheme == 'https'
    region = RegionInfo(uparts.hostname)

    con = boto.ec2.autoscale.AutoScaleConnection(aws_access_key_id=s3id, aws_secret_access_key=s3pw, is_secure=is_secure, port=uparts.port, region=region)
    con.host = uparts.hostname

    return con

def create_autoscale_group(con, name, node_count):
    image = "expr2.gz"
    lc_name = "expr2"
    lc = _find_or_create_config(con, "m1.xlarge", image, "nimbusphantom", lc_name)

    asg_name = name
    print "create %s" % (asg_name)

    asg = boto.ec2.autoscale.group.AutoScalingGroup(launch_config=lc, connection=con, group_name=asg_name, availability_zones=["sierra",], min_size=node_count, max_size=node_count)
    con.create_auto_scaling_group(asg)
    asg.set_capacity(node_count)

def terminate_asg(con, name, s3id, s3pw):
    asg_a = con.get_all_groups(names=[name,])
    inst = asg_a[0].instances
    inst_list = [i.instance_id for i in inst]
    asg_a[0].delete()
    # get instances and kill them
    host = 's83r.idp.sdsc.futuregrid.org'
    #host = 'svc.uc.futuregrid.org'
    ec2conn = EC2Connection(s3id, s3pw, host=host, port=8444, debug=0)
    ec2conn.host = host
    all_inst = ec2conn.get_all_instances(instance_ids=inst_list)
    for res in all_inst:
        for i in res.instances:
            print "terminating %s" % (str(i))
            try:
                i.terminate()
            except Exception, ex:
                print ex


s3id = os.environ['EC2_ACCESS_KEY']
s3pw = os.environ['EC2_SECRET_KEY']

print "getting phantom con"
con = get_phantom_con(s3id, s3pw)
print "have phantom con"

datafile = sys.argv[1]
rnd= sys.argv[2].lower()
outf = open(datafile, "w")

worker_count = 200
picture_size = 1024*64

asg_name = "test%d%d" % (picture_size, worker_count)
print "going to terminate %s if it exists" % (asg_name)
try:
    terminate_asg(con, asg_name, s3id, s3pw)
except:
    pass
extra = 0
if (worker_count % 4) > 0:
    extra = 1
node_count = worker_count / 4 + extra

name = "exp%d_%d_%s" % (worker_count, picture_size, rnd)
name = name.lower()

cmd = "python listbucket.py %snimbus delete" % (name)
os.system(cmd)

start_tm = datetime.now()

print "creating asg %s" % (asg_name)
create_autoscale_group(con, asg_name, node_count)

if 'CHAOS_KILL_TIME' in os.environ:
    cmd = "%s/chaos.sh %d" % (os.getcwd(), int(os.environ['CHAOS_KILL_TIME']))
    print "start chaos %s" % (cmd)
    os.system(cmd)

print "NAME | %s |" % (name)
cmd = "python producer.py %d %d %s" % (worker_count, picture_size, name)
print cmd
rc = os.system(cmd)
end_tm = datetime.now()

tm = end_tm - start_tm
outf.write("%s %d %d %d\n" % (name, worker_count, picture_size, tm.total_seconds()))
outf.flush()
print "DDD time %d" % (str(tm))

os.system("killall chaos.py")
terminate_asg(con, asg_name, s3id, s3pw)
