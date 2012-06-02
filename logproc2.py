import datetime
import math
import sys
from pylab import savefig
import matplotlib.pyplot as plt
from math import pi
from numpy import array, arange, sin
import pylab as P
from matplotlib.ticker import EngFormatter
import numpy as np

def make_graph(line_x, line_y, kill_lines, picture_resolution, worker_count, rnd, directory):
    ax = plt.subplot(111)

    plt.title("%d workers making a %dx%d image" % (worker_count, picture_resolution, picture_resolution))

    ax.plot(line_x, line_y, label="Worker count")
    count = 0
    for (k_x, k_y) in kill_lines:
        count = count + 1
        ax.plot(k_x, k_y, label="Kill event %d" % (count))

    plt.xlabel("Seconds")
    plt.ylabel("Worker counts")
    plt.legend()
    savefig("%s/group%d-%d-killcount_%d_%s.png" % (directory, worker_count, picture_resolution, count, rnd), format='png' )


def parse_entry(line):
    ndx = line.find('||')
    line = line[:ndx]
    sub_array = line.split(' ', 2)

    entry = {}
    entry['rank'] = int(sub_array[0])
    entry['uid'] = sub_array[1].strip()

    (hostip, index) = entry['uid'].split('__')

    entry['ip'] = hostip
    entry['worker_index'] = index
    entry['time'] = convert_time(start_time, sub_array[2].strip()) # change later

    return entry


def parse_kill(line):
    # XXX killing Instance:i-78a71bab None None 2012-04-16 20:29:31.552147
    sub_array = line.split(' ', 3)
    entry = {}
    entry['uid'] = sub_array[0].strip()
    entry['time'] = convert_time(start_time, sub_array[3].strip()) # change later

    return entry


def convert_time(start_time, time_str):
    tm = datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S.%f")
    if start_time is None:
        return tm
    else:
        return tm - start_time

def add_entry(time_dict, entry, max_time):
    tm_list = []
    tm_int = int(math.ceil(entry['time'].total_seconds()))
    if tm_int in time_dict:
        tm_list = time_dict[tm_int]
    tm_list.append(entry)
    time_dict[tm_int] = tm_list

fname = sys.argv[1]
fptr = open(fname, "r")

start_time = None
max_time = 0
start_time_dict = {}
end_time_dict = {}
kill_time_dict = {}
restart_time_dict = {}


active_ranks = {}

start_counts = 0
end_counts = 0

picture_resolution = None
worker_count = None
rnd = None

for line in fptr:
    time_dict = None
    line = line.strip()
    line_array = line.split(" ", 2)

    if line_array[0] == "NAME":
        # NAME | exp128_32768_round1 |

        print line_array
        name = line_array[2]
        name_a = name.split("_")
        picture_resolution = int(name_a[1])
        worker_count = int(name_a[0].replace("exp", ""))
        rnd = name_a[2].split()[0]
        continue
    if line_array[0] != "XXX":
        continue
    if line_array[1] == "starting":
        start_time = convert_time(start_time, line_array[2])
    elif line_array[1] == "CLIENT_START":
        time_dict = start_time_dict
        entry = parse_entry(line_array[2])
        start_counts = start_counts + 1

        rank = entry['rank']
        if rank in active_ranks:
            add_entry(restart_time_dict, entry, max_time)
        active_ranks[rank] = entry

    elif line_array[1] == "CLIENT_DONE":
        time_dict = end_time_dict
        entry = parse_entry(line_array[2])
        end_counts = end_counts + 1

        rank = entry['rank']
        if rank not in active_ranks:
            print "NOT EXISTED!"
            raise Exception("NOT EXIST!")
        del active_ranks[rank]

    elif line_array[1] == "killing":
        entry = parse_kill(line_array[2])
        time_dict = kill_time_dict

    if time_dict is not None:
        tm_list = []
        tm_int = int(math.ceil(entry['time'].total_seconds()))
        if tm_int in time_dict:
            tm_list = time_dict[tm_int]
        tm_list.append(entry)
        time_dict[tm_int] = tm_list

        if tm_int > max_time:
            max_time = tm_int


nsc = 0
nec = 0

times = {}
vm_count = 0
kill_count = 0

line_x = []
line_y = []

kill_lines = []

for t in range(0, max_time+1):
    kill_val = 0
    if t in start_time_dict:
        start_val = len(start_time_dict[t])
        vm_count = vm_count + start_val
        nsc = nsc + start_val
        #print start_val
    if t in end_time_dict:
        end_val = len(end_time_dict[t])
        #print end_val
        vm_count = vm_count - end_val
        nec = nec + end_val

    if t in kill_time_dict:
        kill_count = kill_count + 1
        kill_val = len(kill_time_dict[t])
        vm_count = vm_count - kill_val
        msg = "%d\t%d" % (t, vm_count)
        for i in range(0, kill_count):
            msg = msg + "\t "
        print "%s\t0" % (msg)
        print "%s\t%d" % (msg, start_counts)

        k_x = [t, t]
        k_y = [0, start_counts]

        kill_lines.append((k_x, k_y))

    else:
        times[t] = vm_count
        msg = "%d\t%d" % (t, vm_count)
        print msg
        line_x.append(t)
        line_y.append(vm_count)


for e in restart_time_dict:
    print restart_time_dict[e]


make_graph(line_x, line_y, kill_lines, picture_resolution, worker_count, rnd, sys.argv[2])
