from boto.s3.connection import OrdinaryCallingFormat, S3Connection
import os
import urlparse
import sys

def get_s3_conn():
    s3url = os.environ['S3_URL']
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

    cf = OrdinaryCallingFormat()
    s3conn = S3Connection(s3id, s3pw, host=host, port=port, is_secure=is_secure, calling_format=cf)
    return s3conn

delete = False
download = False
if len(sys.argv) > 2:
    delete = sys.argv[2] == "delete"
    download = sys.argv[2] == "download"

con = get_s3_conn()

b = con.get_bucket(sys.argv[1])

for k in b.list():
    if download:
        k.get_contents_to_filename(k.name)
    if delete:
        k.delete()
if delete:
    b.delete()
