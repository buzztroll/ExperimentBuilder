from datetime import datetime
import os
import sys

max_worker_count = 128
max_picture_size = 1024 * 16

datafile = sys.argv[1]
rnd= sys.argv[2]
outf = open(datafile, "w")

worker_count = max_worker_count
while worker_count > 1:
    
    picture_size = 1024*8
    while picture_size < max_picture_size:

        name = "exp%d_%d_%s" % (worker_count, picture_size, rnd)
        name = name.lower()

        cmd = "python listbucket.py %snimbus delete" % (name)
        os.system(cmd)

        start_tm = datetime.now()
        cmd = "python producer.py %d %d %s" % (worker_count, picture_size, name)
        print cmd
        rc = os.system(cmd)
        end_tm = datetime.now()

        tm = end_tm - start_tm
        outf.write("%s %d %d %d\n" % (name, worker_count, picture_size, tm.total_seconds()))
        outf.flush()

        worker_count = worker_count / 2
    
    picture_size = picture_size * 2
