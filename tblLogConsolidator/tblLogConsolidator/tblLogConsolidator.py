import re
import time
from typing import List, Dict

import boto3

"""
Consolidate individual TB-Loader logs into daily logs.

The TB-Loaders create individual log files named by this scheme:
{bucket}/log/tbcd1234/yyyymmddThhmmss\.mmmZ?\.log

We want to read those files, in timestamp order, and concatenate 
them into a file named like this:
{bucket}/log/yyyy/mm/dd/tbcd1234.log 

This runs on a timer in CloudWatch. Go to the CloudWatch console,
look at Rules, at ConsolidateDailyLogs.
"""

s3 = boto3.client('s3', region_name='us-west-2')
bucket = 'acm-stats'
log_prefix = 'log'

num_log_files = 0
total_bytes = 0
num_tb_ids = 0


# List the objects with the given prefix.
# noinspection PyPep8Naming
def _list_prefixes(bucket=bucket, prefix='', **kwargs):
    paginator = s3.get_paginator("list_objects_v2")
    if not prefix.endswith('/'):
        prefix += '/'
    for objects in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/', **kwargs):
        for obj in objects.get('CommonPrefixes', []):
            yield obj['Prefix']


# List the objects with the given prefix.
# noinspection PyPep8Naming
def _list_objects(Bucket=bucket, Prefix='', **kwargs):
    paginator = s3.get_paginator("list_objects_v2")
    params = {'Bucket': Bucket, 'Prefix': Prefix, **kwargs}
    for objects in paginator.paginate(**params):
        for obj in objects.get('Contents', []):
            yield obj


# Gets a list of all log/tbcd1234 paths; all of the unconsolidated log prefixes.
def get_tbloader_paths() -> List[str]:
    result = []
    for obj in _list_prefixes(prefix=log_prefix + '/'):
        if obj.lower().startswith(log_prefix + '/tbcd'):
            print(obj)
            result.append(obj)

    return result


# Given a log/tbcd1234/ path, return a list of all the keys in that path, the individual log files.
def get_tbloader_logs(tbloader: str) -> Dict[str, Dict[str, Dict[str, List[str]]]]:
    ts_re = re.compile(r'(?i)^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})\.(\d{3})Z?.log$')
    result = {}
    # Find the log files for this tb-loader.
    for obj in _list_objects(Prefix=tbloader):
        # see if the name has a valid timestamp part.
        key = obj.get('Key')
        timestamp = key[len(tbloader):]
        ts_match = ts_re.match(timestamp)
        if ts_match:
            # Save this key with others from the same day
            year = ts_match.group(1)
            month = ts_match.group(2)
            day = ts_match.group(3)
            result.setdefault(year, {}).setdefault(month, {}).setdefault(day, []).append(key)

    return result


# For a given log/tbcd1234/ path, and a given year-month-day, and a list of keys from that day,
# consolidate the individual log/tbcd1234/yyyymmddThhmmss.mmmZ.log files into a single
# log/yyyy/mm/dd/tbcd1234.log file. If such a file already exists, append to it.
def consolidate_day(tbloader_path, year, month, day, keys):
    global num_log_files, total_bytes
    # 'log/tbcd1234/' => 'tbcd1234'
    tbloader_id = tbloader_path[len(log_prefix) + 1:].lower()
    if tbloader_id.endswith('/'):
        tbloader_id = tbloader_id[:-1]
    consolidated_data_key = '{prefix}/{year}/{month}/{day}/{tbid}.log' \
        .format(prefix=log_prefix, year=year, month=month, day=day, tbid=tbloader_id)

    # Get any existing data.
    try:
        obj = s3.get_object(Bucket=bucket, Key=consolidated_data_key)
        consolidated_data = obj.get('Body').read()
        if consolidated_data[-1:].decode() != '\n':
            consolidated_data += b'\n'
    except:
        consolidated_data = b''

    # Gather the new log data
    # Does s3 already guarantee the keys in sorted order?
    num_log_files += len(keys)
    for key in sorted(keys):
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj.get('Body').read()
        if len(data) == 0:
            continue
        if data[-1:].decode() != '\n':
            data += b'\n'
        consolidated_data += data
        total_bytes += len(data)

    # Write the consolidated data
    put_result = s3.put_object(Body=consolidated_data, Bucket=bucket, Key=consolidated_data_key)

    # Delete the individual logs
    while len(keys) > 0:
        to_delete = [{'Key': key} for key in keys[:1000]]
        keys = keys[1000:]
        delete_result = s3.delete_objects(Delete={'Objects': to_delete, 'Quiet': True}, Bucket=bucket)


# For a given log/tbcd1234/ path, consolidate the log files for that tbloader id.
def consolidate_tbloader(tbloader_path: str):
    # returns a dict { 'yyyy' : {'mm' : {'dd' : [log, log, ...]}}}
    years = get_tbloader_logs(tbloader_path)
    for year, months in years.items():
        for month, days in months.items():
            for day, keys in days.items():
                consolidate_day(tbloader_path, year, month, day, keys)


# Consolidate individual tbloader log files into one per day per tbloader id.
def consolidate():
    global num_tb_ids
    tbloader_paths = get_tbloader_paths()
    num_tb_ids += len(tbloader_paths)
    for tbloader_path in tbloader_paths:
        consolidate_tbloader(tbloader_path)


def lambda_handler(event=None, context=None):
    start = time.time_ns()

    consolidate()

    end = time.time_ns()
    print('{ms:.3f} ms, consolidated {bytes} bytes in {files} log files for {tbids} TB-Loader ids'
          .format(ms=(end - start) / 1000000.0, bytes=total_bytes, files=num_log_files, tbids=num_tb_ids))


if __name__ == '__main__':
    lambda_handler();
