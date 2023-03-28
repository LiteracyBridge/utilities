import copy
import csv
import datetime
import re
import time
import urllib
from typing import List, Dict

import boto3 as boto3
import dateutil
from sqlalchemy import text

from db import get_db_engine, get_db_connection

s3_client = boto3.client('s3')

# RE to recognize a metadata CSV file from a deployment
METADATA = re.compile(
    r"^(?P<program>[\w-]+)/TB-Loaders/published/" +
    r"(?P<deployment>[\w-]+)-(?P<revision>[\w-]+)/metadata/" +
    r"(?P<file>\w+)\.csv$")

"""
In the struct below:
    the key : is the name of the metadata .csv file
    programid : is the name of the programid column in the SQL table
    table : is the name of the table IF it differs from the metadata .csv file name
    pk : is the name of the table's primary key
    pk_columns : is a list of the SQL columns in the primary key
    uc : if it exists, is a list of CSV columns that should be forced to upper case
    dates : if it exists, is a list of CSV columns to be forced to date (or None)
    csv_to_sql : is a map of the CSV column names to the SQL column names
    defaults : if it exists, is a map of "optional" CSV columns, and the value to use if they're missing
    csv_to_csv : if it exists, is a {from:to} map of CSV columns to be renamed
"""
LOAD_DATA_PARAMS = {
    'categories': {
        'programid': 'projectcode',
        'table': 'categories',
        'pk': 'categories_pkey',
        'pk_columns': ['categoryid', 'categoryname', 'projectcode'],
        'csv_to_sql': {'ID': 'categoryid', 'Name': 'categoryname', 'Project': 'projectcode'}
    }, 'categoriesinpackages': {
        'programid': 'project',
        'table': 'categoriesinpackage',
        'pk': 'categoriesinpackage_pkey',
        'pk_columns': ['project', 'contentpackage', 'categoryid'],
        'uc': ['project', 'contentpackage'],
        'csv_to_sql': {'project': 'project', 'contentpackage': 'contentpackage', 'categoryid': 'categoryid',
                       'order': 'position'}
    }, 'contentinpackages': {
        'programid': 'project',
        'table': 'contentinpackage',
        'pk': 'contentinpackage_pkey',
        'pk_columns': ['project', 'contentpackage', 'contentid', 'categoryid'],
        'uc': ['project', 'contentpackage'],
        'csv_to_sql': {'project': 'project', 'contentpackage': 'contentpackage', 'contentid': 'contentid',
                       'categoryid': 'categoryid', 'order': 'position'}
    }, 'languages': {
        'programid': 'projectcode',
        'pk': 'languages_pkey',
        'pk_columns': ['languagecode', 'language', 'projectcode'],
        'csv_to_sql': {'ID': 'languagecode', 'Name': 'language', 'Project': 'projectcode'}
    }, 'metadata': {
        'programid': 'project',
        'table': 'contentmetadata2',
        'pk': 'contentmetadata2_pkey',
        'pk_columns': ['project', 'contentid'],
        'defaults': {'LB_SDG_GOALS': '', 'LB_SDG_TARGETS': ''},
        'csv_to_sql': {'DC_TITLE': 'title', 'DC_PUBLISHER': 'dc_publisher', 'DC_IDENTIFIER': 'contentid',
                       'DC_SOURCE': 'source', 'DC_LANGUAGE': 'languagecode', 'DC_RELATION': 'relatedid',
                       'DTB_REVISION': 'dtb_revision', 'LB_DURATION': 'duration_sec', 'LB_MESSAGE_FORMAT': 'format',
                       'LB_TARGET_AUDIENCE': 'targetaudience', 'LB_DATE_RECORDED': 'daterecorded',
                       'LB_KEYWORDS': 'keywords', 'LB_TIMING': 'timing', 'LB_PRIMARY_SPEAKER': 'speaker',
                       'LB_GOAL': 'goal', 'LB_ENGLISH_TRANSCRIPTION': 'transcriptionurl', 'LB_NOTES': 'notes',
                       'LB_BENEFICIARY': 'community', 'LB_STATUS': 'status', 'CATEGORIES': 'categories',
                       'QUALITY': 'quality', 'PROJECT': 'project', 'LB_SDG_GOALS': 'sdg_goals',
                       'LB_SDG_TARGETS': 'sdg_targets'}
    }, 'packagesindeployment': {
        # The shell script translates '""' to '' (empty string to null string)
        'programid': 'project',
        'pk': 'packagesindeployment_pkey',
        'pk_columns': ['project', 'deployment', 'contentpackage', 'languagecode', 'groups'],
        'uc': ['project', 'contentpackage', 'deployment'],
        'dates': ['startDate', 'endDate'],
        'csv_to_sql': {'project': 'project', 'deployment': 'deployment', 'contentpackage': 'contentpackage',
                       'packagename': 'packagename', 'startDate': 'startdate', 'endDate': 'enddate',
                       'languageCode': 'languagecode', 'grouplangs': 'groups', 'distribution': 'distribution'},
        'csv_to_csv': {'groups': 'grouplangs'},
        'defaults': {'distribution': '{deployment}-{revision}'}
    }
}


def write_sql(programid: str, metadata_filename: str, csv: List[Dict]):
    load_params = LOAD_DATA_PARAMS.get(metadata_filename)

    get_db_engine()  # will use cached copy, if available.
    with get_db_connection() as conn:
        values = copy.deepcopy(csv)
        # Different tables have different names from programid.
        programid_name = load_params.get('programid')
        for v in values:
            v[programid_name] = programid
        table_name = load_params.get('table', metadata_filename)
        csv_to_sql = load_params.get('csv_to_sql')
        conflict_handler = ''
        if pk := load_params.get('pk'):
            pk_columns = load_params.get('pk_columns')
            if [x for x in csv_to_sql.values() if x not in pk_columns]:
                conflict_handler = (f' ON CONFLICT ON CONSTRAINT {pk} DO UPDATE SET ' +
                                    ','.join([f'{x}=EXCLUDED.{x}' for x in csv_to_sql.values() if x not in pk_columns]))
            else:
                conflict_handler = ' ON CONFLICT DO NOTHING'

        command = text(f'INSERT INTO {table_name} ({",".join(csv_to_sql.values())}) '
                       f'VALUES (:{",:".join(csv_to_sql.keys())}) ' +
                       conflict_handler + ';')
        result = conn.execute(command, values)
        print(f'{result.rowcount} rows inserted/updated for {metadata_filename} for {programid}.')


# transaction = conn.begin()

def read_csv(bucket: str, key: str, metadata_filename, **kwargs) -> List[Dict]:
    def normalize_value(k, v):
        if k in uc:
            return str(v).upper()
        elif k in dates:
            if not v:
                return None
            elif isinstance(v, datetime.date):
                return v.date() if isinstance(v, datetime.datetime) else v
            try:
                return dateutil.parser.parse(v).date()
            except Exception:
                pass
        return v

    def normalize_rec(csv_rec):
        rec = {k: normalize_value(k, v) for k, v in csv_rec.items()}
        for fr, to in csv_to_csv.items():
            if fr in rec:
                rec[to] = rec[fr]
                del rec[fr]
        for dk, dv in defaults.items():
            if not rec.get(dk):
                rec[dk] = dv.format(**kwargs)
        return rec

    load_params = LOAD_DATA_PARAMS.get(metadata_filename)
    uc = load_params.get('uc', [])  # columns to force upper case
    dates = load_params.get('dates', [])  # columns which contain dates
    defaults = load_params.get('defaults', {})
    csv_to_csv = load_params.get('csv_to_csv', {})

    obj = s3_client.get_object(Bucket=bucket, Key=key)
    raw_data = obj.get('Body').read().decode('utf-8')
    # Get the list of dicts
    csv_lines = raw_data.splitlines(True)
    csv_reader = csv.DictReader(csv_lines)
    csv_list = [normalize_rec(csv_rec) for csv_rec in csv_reader]
    return csv_list


def lambda_handler(event, context):
    # event = {'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'us-west-2',
    #                       'eventTime': '2020-09-10T19:01:29.427Z', 'eventName': 'ObjectCreated:Put',
    #                       'userIdentity': {'principalId': 'AWS:AIDAJGWFCEB6RK5NV6SBG'},
    #                       'requestParameters': {'sourceIPAddress': '172.92.94.105'},
    #                       'responseElements': {'x-amz-request-id': '567BE63738FECD23',
    #                                            'x-amz-id-2': 'J33azBbvcofuKhk6CZoVmw1BGZcTZecQLiQ+V2cbMTmWUgZT1L0M387VOqrdz6CNJUy7T9otUb/g4O28kiCSQLi0cRQiMkwA'},
    #                       's3': {'s3SchemaVersion': '1.0', 'configurationId': 'SfToAwsPut',
    #                              'bucket': {'name': 'amplio-sf-to-aws',
    #                                         'ownerIdentity': {'principalId': 'A3MRMLZEL3RVRR'},
    #                                         'arn': 'arn:aws:s3:::amplio-sf-to-aws'},
    #                              'object': {'key': 'a1t3l000007rnRUAAY.json', 'size': 1453,
    #                                         'eTag': '47aa7471b336f417227849cab868e99f',
    #                                         'sequencer': '005F5A780A3AF59216'}}}]}
    global email_text
    email_text = []  # reset

    start = time.time_ns()
    # print(f'Event: {event}')

    records = event['Records']
    for record in records:
        email_text.append('\n\n============================================================\n\n')

        bucket = record['s3']['bucket']
        bucket_name = bucket['name']
        object = record['s3']['object']
        key = urllib.parse.unquote_plus(object['key'], encoding='utf-8')

        print(f'Got event for key "{key}" in bucket "{bucket_name}"')

        if (match := METADATA.match(key)) and match['file'] in LOAD_DATA_PARAMS:
            metadata_filename = match['file']
            programid = match['program']
            deployment = match['deployment']
            revision = match['revision']
            # print(f'Got match for {program}, {match["deployment"]}-{match["revision"]}: {metadata_filename}')
            csv_list = read_csv(bucket_name, key, metadata_filename, programid=programid, deployment=deployment,
                                revision=revision)
            if len(csv_list) == 0:
                print(f'No data in {metadata_filename} for program {programid}')
            else:
                write_sql(programid, metadata_filename, csv_list)

    end = time.time_ns()
    print('Event processed in {:,} ns'.format(end - start))
