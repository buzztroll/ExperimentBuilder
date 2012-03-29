from boto.s3.connection import S3Connection, OrdinaryCallingFormat
import os
import sys
from PIL import Image
import urlparse
import bz2


def main(argv=sys.argv):
    w = int(argv[1])
    out_file = argv[2]
    f_list = argv[3:]

    h = w
    l_image = Image.new("RGB", (w, h))
    line_num = 0
    for f in f_list:
        fptr = open(f, "r")
        for line in fptr:
            try:
                (s_kx, s_ky, s_red, s_green, s_blue) = line.split()
                l_image.putpixel((int(s_kx), int(s_ky)), (int(s_red), int(s_green), int(s_blue)))
            except Exception, ex:
                print ex
                print line
                print line_num
            line_num = line_num + 1
    l_image.save(out_file, "PNG")


if __name__ == "__main__":
    rc = main()
    sys.exit(rc)
