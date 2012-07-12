import sys
import urlparse
from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.connection import S3Connection
import os
import pygame
from pygame.locals import *
import threading
import signal

g_window_size = 768


class Get1File(threading.Thread):

    def __init__(self, key, screen_obj, scale_ratio, file_dir):
        threading.Thread.__init__(self)
        self.key = key
        self.screen_obj = screen_obj
        self.file_dir = file_dir
        self.scale_ratio = scale_ratio

    def get_file(self):
        try:
            root_filename = os.path.join(self.file_dir, self.key.name)
            filename = root_filename + ".bz2"
            print "getting %s -> %s" % (self.key.name, filename)
            self.key.get_contents_to_filename(filename)
            print "unzip %s" % (filename)
            os.system("bunzip2 %s" % (filename))
            print "draw %s" % (filename)
            #self.draw_file(root_filename)
            self.screen_obj.draw_file(root_filename)
            print "removing %s" % (filename)
            os.remove(root_filename)
        except Exception, ex:
            print ex

    def draw_file(self, data_file):
        line_num = 0
        fptr = open(data_file, "r")
        for line in fptr:
            try:
                la = line.split()
                s_kx = int(int(la[0]) * self.scale_ratio)
                s_ky = int(int(la[1]) * self.scale_ratio)
                s_red = int(la[2])
                s_green = int(la[3])
                s_blue = int(la[4])
                self.screen_obj.set_pixel((s_kx, s_ky), (s_red, s_green, s_blue, 255))
            except Exception, ex:
                print ex
                print line
                print line_num
            line_num = line_num + 1

    def run(self):
        self.get_file()


class GetEvents(threading.Thread):

    def __init__(self, scale_ratio, bucket_name, file_dir):
        threading.Thread.__init__(self)
        pygame.init()
        window = pygame.display.set_mode((g_window_size, g_window_size))
        pygame.display.set_caption("Phantom Demo")
        self.scale_ratio = scale_ratio
        self.screen = pygame.display.get_surface()
        self.bucket_name = bucket_name
        self.already_got = []
        self.done = False
        self.lock = threading.Lock()
        self.file_dir = file_dir

    def is_done(self):
        return self.done

    def set_done(self):
        self.done = True

    def get_files(self):
        s3con = cb_get_conn()
        bucket = s3con.get_bucket(self.bucket_name)

        keys = bucket.get_all_keys()
        for k in keys:
            if k.name in self.already_got:
                continue
            if self.done:
                break
            self.already_got.append(k.name)
            gf = Get1File(k, self, self.scale_ratio, self.file_dir)
            gf.start()

    def set_pixel(self, loc, c):
        self.lock.acquire()
        try:
            self.screen.set_at(loc, c)
        finally:
            self.lock.release()

    def draw_file(self, data_file):
        self.lock.acquire()
        try:
            self._draw_file(data_file)
        finally:
            self.lock.release()

    def _draw_file(self, data_file):
        line_num = 0
        fptr = open(data_file, "r")
        for line in fptr:
            try:
                la = line.split()
                s_kx = int(int(la[0]) * self.scale_ratio)
                s_ky = int(int(la[1]) * self.scale_ratio)
                s_red = int(la[2])
                s_green = int(la[3])
                s_blue = int(la[4])
                self.screen.set_at((s_kx, s_ky), (s_red, s_green, s_blue, 255))
            except Exception, ex:
                print ex
                print line
                print line_num
            line_num = line_num + 1


    def run(self):
        while not self.done:
            try:
                self.get_files()
            except Exception, ex:
                print ex
                pygame.time.delay(5000)
            pygame.time.delay(int(10))


def cb_get_conn(): 
    id = os.environ['EC2_ACCESS_KEY']
    pw = os.environ['EC2_SECRET_KEY']
    cloud_url = os.environ['S3_URL']
    uparts = urlparse.urlparse(cloud_url)
    is_secure = uparts.scheme == 'https'
    cf = OrdinaryCallingFormat()
    conn = S3Connection(id, pw, host=uparts.hostname, port=uparts.port, is_secure=is_secure, calling_format=cf, debug=0)
    return conn

def setup_screen():
    pygame.init()
    window = pygame.display.set_mode((g_window_size, g_window_size))
    pygame.display.set_caption("Phantom Demo")
    screen = pygame.display.get_surface()
    return screen

def main(argv=sys.argv):
    bucket_name = argv[1]
    file_dir = argv[2]
    image_size = int(argv[3])
    scale_ratio = float(g_window_size) / float(image_size)
    try:
        os.mkdir(file_dir)
    except:
        pass
    
    g = GetEvents(scale_ratio, bucket_name, file_dir)
    g.start()

    def handler(signum, frame):
        print "KILL"
        g.set_done()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    while not g.is_done():

        events = pygame.event.get()
        for event in events:
            if event.type == QUIT:
                g.set_done()
        pygame.display.flip()
        pygame.time.delay(int(500))

main()
            