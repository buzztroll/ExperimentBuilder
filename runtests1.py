from datetime import datetime
import os
import sys

max_worker_count = 16
max_picture_size = 1024 * 2
picture_size = 1024

datafile = sys.argv[1]

outf = open(datafile, "w")

while picture_size < max_picture_size:
    worker_count = 1
    while worker_count <= max_worker_count:

        name = "exp%d.%d" % (worker_count, picture_size)
        start_tm = datetime.now()
        cmd = "python producer.py %d %d %s" % (worker_count, picture_size, name)
        print cmd
        rc = os.system(cmd)
        end_tm = datetime.now()

        tm = end_tm - start_tm
        outf.write("%s %d %d %d\n" % (name, worker_count, picture_size, tm.total_seconds()))

        worker_count = worker_count + 1
    
    picture_size = picture_size * 2