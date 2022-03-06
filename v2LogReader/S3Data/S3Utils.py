import boto3

DEFAULT_SOURCE_BUCKET = 'acm-stats'
DEFAULT_SOURCE_PREFIX = 'collected-data.v2'  # + ${tbcdid} + ${ts}.zip : uploaded here from TB-Loader

ARCHIVE_BUCKET = 'acm-stats'
ARCHIVE_PREFIX = 'archived-data.v2'  # + ${year}/${month}/${day} + ${ts}.zip # moved here after processing

PROCESSED_BUCKET = 'acm-stats'
PROCESSED_PREFIX = 'processed-data.v2'  # + ${year}/${month}/${day} + ${ts} # processed data extracted here

UF_BUCKET = 'amplio-uf'
UF_PREFIX = 'collected'

s3 = boto3.client('s3')


def list_objects(bucket, prefix=''):
    paginator = s3.get_paginator("list_objects_v2")
    kwargs = {'Bucket': bucket, 'Prefix': prefix}
    for objects in paginator.paginate(**kwargs):
        for obj in objects.get('Contents', []):
            yield obj
