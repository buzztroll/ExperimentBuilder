from boto.s3.connection import S3Connection, OrdinaryCallingFormat
import os
import sys
from PIL import Image
import urlparse
import bz2

def get_s3_conn():
    try:
        s3url = os.environ['S3_URL']
    except:
        s3url = None
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
    if s3url:
        s3conn = S3Connection(s3id, s3pw, host=host, port=port, is_secure=is_secure, calling_format=cf)
    else:
        s3conn = S3Connection(s3id, s3pw, calling_format=cf)
    return s3conn


def main(argv=sys.argv):
    bucketname = argv[1]

    con = get_s3_conn()
    b = con.get_bucket(bucketname)
    f_list = []
    for k in b.list():
        print "downloading %s" % (k.name)
        zipname = "%s.bz2" % (k.name)
        outfname = "%s.out"
        try:
            os.remove(zipname)
        except Exception, ex:
            print ex
        k.get_contents_to_filename(zipname)

#        print zipname
#        out_f = open(outfname, "w")
#        f = bz2.BZ2File(zipname, "r")
#        sz = 1024 * 1024
#        try:
#            data = f.read(sz)
#        except Exception, ex:
#            print ex
#            data = None
#        while data:
#            out_f.write(data)
#            data = f.read(sz)
#        out_f.close()
#        f.close()
#        f_list.append(outfname)
        f_list.append(zipname)

#    h = w
#    l_image = Image.new("RGB", (w, h))
#    line_num = 0
#    for f in f_list:
#        fptr = open(f, "r")
#        for line in fptr:
#            try:
#                (s_kx, s_ky, s_red, s_green, s_blue) = line.split()
#                l_image.putpixel((int(s_kx), int(s_ky)), (int(s_red), int(s_green), int(s_blue)))
#            except Exception, ex:
#                print ex
#                print line
#                print line_num
#            line_num = line_num + 1
#    l_image.save(out_file, "PNG")


if __name__ == "__main__":
    rc = main()
    sys.exit(rc)
