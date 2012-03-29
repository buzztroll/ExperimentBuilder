from boto.s3.connection import S3Connection, OrdinaryCallingFormat
import os
import sys
from PIL import Image
import urlparse

def get_s3_conn():
    s3url = os.environ['EC2_URL']
    s3id = os.environ['EC2_ACCESS_KEY']
    s3pw = os.environ['EC2_SECRET_KEY']


    host = None
    port = None
    is_secure = True
    path = "Expr1"
    if s3url:
        parts = urlparse.urlparse(s3url)
        host = parts.hostname
        port = parts.port
        is_secure = parts.scheme == "https"
        path = parts.path

    print "%s %s %s" % (s3id, s3pw, s3url)
    cf = OrdinaryCallingFormat()
    s3conn = S3Connection(s3id, s3pw, host=host, port=port, is_secure=is_secure, calling_format=cf)
    return s3conn


def main(argv=sys.argv):
    bucketname = argv[1]
    w = int(argv[2])
    out_file = argv[3]

    con = get_s3_conn()
    b = con.get_bucket(bucketname)
    f_list = []
    for k in b.list():
        print "downloading %s" % (k.name)
        zipname = "%s.gz" % (k.name)
        k.get_contents_to_filename(zipname)
        os.system('gunzip %s' % (zipname))
        f_list.append(k.name)

    h = w
    l_image = Image.new("RGB", (w, h))
    for f in f_list:
        fptr = open(f, "r")
        for line in fptr:
            (s_kx, s_ky, s_red, s_green, s_blue) = line.split()
            l_image.putpixel((int(s_kx), int(s_ky)), (int(s_red), int(s_green), int(s_blue)))
    l_image.save(out_file, "PNG")


if __name__ == "__main__":
    rc = main()
    sys.exit(rc)
