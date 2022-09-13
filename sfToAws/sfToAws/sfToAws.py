import json
import re
import time
import urllib
from typing import Dict, List

# import amplio.rolemanager.manager as roles_manager
import boto3
from sqlalchemy import text

from db import get_db_engine, get_db_connection

"""
Create an interface between Salesforce and AWS.

"""

# roles_manager.open_tables()

STATUS_OK = 'ok'
STATUS_FAILURE = 'failure'
STATUS_EXTRA_PARAMETER = 'Extraneous parameter'
STATUS_ACCESS_DENIED = 'Access denied'
STATUS_MISSING_PARAMETER = 'Missing parameter'

_SF2AWS_TABLE_NAME = 'sf2aws'
_SF2AWS_TABLE_KEY = 'salesforce_id'
_SF2AWSS_TABLE_CREATE_ARGS = {
    'TableName': _SF2AWS_TABLE_NAME,
    'AttributeDefinitions': [{'AttributeName': _SF2AWS_TABLE_KEY, 'AttributeType': 'S'}],
    'KeySchema': [{'AttributeName': _SF2AWS_TABLE_KEY, 'KeyType': 'HASH'}]
}
# limit read/write capacity units of table (affects cost of table)
_DEFAULT_PROVISIONING = {'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}

# If there is a name in parens, extracts it to a separate group. For parsing things like
# Literacy Bridge Ghana (LBG)
PARENS_RE = re.compile(r'(.*?)?(?:\(([^()]*)\))?$')
BAD_CHARS_RE = re.compile('[<>:"/\\\\|?*\x01-\x1f]')

# The minimum fields to be able to create an ACM database.
REQUIRED_FIELDS = [
    "salesforce_id",
    "program_name",
    "stage",
    "customer",
]
READY_STAGES = [
    # "Aborted",
    # "Analyze & Improve",
    # "Collect",
    # "Completed",
    "Create",
    "Deploy",
    # "Ongoing Implementation",
    # "Plan",
    # "Prepare/Set-up" is really where we want to create the ACM, but if we miss this stage, the other 3 will do.
    "Prepare/Set-up",
    # "Stalled due to COVID19",
    "Train"
]

email_text: list[str] = []


class SfToAws:
    """
    A class to encapsulate adding a record to the dynamodb table 'sf2aws'. Creates the table if needed.
    """

    def __init__(self, **kwargs):
        """Initialize the instance, open the database, ensure the tables exist."""
        self._dynamodb_client = boto3.client('dynamodb')  # specify amazon service to be used
        self._dynamodb_resource = boto3.resource('dynamodb')

        # uncomment to (re-)create tables
        # self._create_tables()
        self._sf2aws_table = self._dynamodb_resource.Table(_SF2AWS_TABLE_NAME)

    def has_record(self, sf_data) -> bool:
        query = self._sf2aws_table.get_item(Key={_SF2AWS_TABLE_KEY: sf_data['salesforce_id']})
        sf_row = query.get('Item')
        return True if sf_row else False

    def get_record(self, sf_data) -> bool:
        query = self._sf2aws_table.get_item(Key={_SF2AWS_TABLE_KEY: sf_data['salesforce_id']})
        sf_row = query.get('Item')
        return sf_row

    def add_record(self, sf_data, **kwargs):
        query = self._sf2aws_table.get_item(Key={_SF2AWS_TABLE_KEY: sf_data['salesforce_id']})
        sf_row = sf_data

        for k, v in kwargs.items():
            sf_row[k] = v
        self._sf2aws_table.put_item(Item=sf_row)

    def _create_tables(self):
        start_time = time.time()
        num_updated = 0
        wait_list = []

        def create(create_params):
            nonlocal num_updated, wait_list
            table_name = create_params['TableName']
            if table_name not in existing_tables:
                try:
                    table = self._dynamodb_client.create_table(
                        AttributeDefinitions=create_params['AttributeDefinitions'],
                        TableName=table_name,
                        KeySchema=create_params['KeySchema'],
                        ProvisionedThroughput=_DEFAULT_PROVISIONING
                    )
                    print('Successfully created table: ', table['TableDescription'])

                except Exception as err:
                    print('ERROR: ' + str(err))

                wait_list.append(table_name)

        def await_tables():
            nonlocal num_updated, wait_list
            for table_name in wait_list:
                # Wait for the table to be deleted before exiting
                print('Waiting for', table_name, '...')
                waiter = self._dynamodb_client.get_waiter('table_exists')
                waiter.wait(TableName=table_name)
                num_updated += 1

        existing_tables = self._dynamodb_client.list_tables()['TableNames']
        create(_SF2AWSS_TABLE_CREATE_ARGS)

        await_tables()

        end_time = time.time()
        print('Created {} tables in {:.2g} seconds'.format(num_updated, end_time - start_time))


s3_client = boto3.client('s3')
SF_TO_AWS_BUCKET: str = 'amplio-sf-to-aws'


# List the objects with the given prefix.
# noinspection PyPep8Naming
def _list_objects(Bucket: str, Prefix: str = '', **kwargs):
    """
    Lists objects in an S3 bucket with the given Prefix. Handles the 1000-at-a-time
    pagination.
    @param Bucket: String containing the S3 bucket to list.
    @param Prefix: String with the prefix to list.
    @param kwargs: Any additional args to be passd to "list_objects_v2"
    @yield: one object listing
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    request_args = {'Bucket': Bucket, 'Prefix': Prefix, **kwargs}
    for objects in paginator.paginate(**request_args):
        for obj in objects.get('Contents', []):
            yield obj


# Format and send an ses message. Options are
# html    - if true, send as html format
# dry_run - if true, do not actually send email
def send_ses(subject: str,
             body: str,
             sender: str = 'ictnotification@amplio.org',
             recipient: str = 'bill@amplio.org',
             as_html: bool = False):
    """Send an email via the Amazon SES service.

    Example:
      send_ses('me@example.com, 'greetings', "Hi!", 'you@example.com)

    Return:
      If 'ErrorResponse' appears in the return message from SES,
      return the message, otherwise return an empty '' string.
    """

    def as_list(obj):
        if isinstance(obj, str):
            return [str(obj)]
        try:
            return [str(x) for x in obj]
        except Exception:
            return [str(obj)]

    destination = {'ToAddresses': as_list(recipient)}
    format = 'Html' if as_html else 'Text'
    message: dict[str, dict] = {'Subject': {'Data': subject},
                                'Body': {format: {'Data': body} } }

    client = boto3.client('ses')
    response = client.send_email(Source=sender, Destination=destination, Message=message)
    print('Sent email, response: {}'.format(response))
    return response


def read_json_from_s3(key: str, bucket: str = SF_TO_AWS_BUCKET) -> Dict[str, str]:
    """
    Reads an object from an S3 bucket and parses it as JSON.
    @param key: key of the object
    @param bucket: bucket in which the object lives
    @return: the json, parsed into a dict.
    """

    def norm_k(k: str) -> str:
        """
        Normalize a key by replacing spaces with underscores, and converting to lowercase.
        """
        k = str(k)
        return k.replace(' ', '_').lower()

    def norm_v(v: str) -> str:
        """
        Normalize a value by escaping double quotes ( replace " with \" ).
        """
        v = str(v)
        return v.replace('"', r'\"')

    obj = s3_client.get_object(Bucket=bucket, Key=key)
    raw_data = obj.get('Body').read()
    data = json.loads(raw_data.decode('utf-8'))
    return {norm_k(k): norm_v(v) for k, v in data.items()}


def update_sql(sf_data: dict):
    def update_db(sf_data: dict):
        get_db_engine() # will use cached copy, if available.
        with get_db_connection() as conn:
            # transaction = conn.begin()
            updates = [{'program_id': sf_data['program_id'],
                        # set to provided tableau_id, or to empty string.
                        'tableau_id': (sf_data.get('tableau_id', ''))}]

            field_names = ['tableau_id']
            command_u = text(f'UPDATE programs SET ({",".join(field_names)}) '
                             f'= row(:{",:".join(field_names)}) WHERE program_id=:program_id;')
            result_u = conn.execute(command_u, updates)
            print(f'{result_u.rowcount} program records updated.')
            if result_u.rowcount == 0:
                msg = f'tableau_id not updated for program {sf_data.get("program_id")}, salesforce_id {sf_data.get("salesforce_id")}'
                print(msg)
                email_text.append(msg)

            # command_q = text('SELECT * FROM programs WHERE program_id = :program_id')
            # result_q = conn.execute(command_q, updates)
            # transaction.rollback()

    global email_text
    # We need program_id to update SQL.
    if 'program_id' not in sf_data:
        return
    # All we're setting at this time is "tableau_id". May change in the future.
    update_db(sf_data)


def has_required_fields(sf_data: dict[str, str]) -> bool:
    """
    Checks that the salesforce record has all the fields we need for it to be useful.
    @param sf_data: dict[str,str] from salesforce (normalized)
    @return: true if it has the required fields, false otherwise.
    """
    global email_text
    result: bool = True
    for f in REQUIRED_FIELDS:
        if f not in sf_data:
            email_text.append(f'Missing value for {f}')
            result = False
    return result


def normalize_data(sf_data: dict) -> None:
    """
    Normalize the keys and values in the data from salesforce. For keys, convert spaces to underscores and
    make lower case. For values, remove trailing '.0' from numbers, parse SDGs and langauges to a better format.

    If the data looks like it may be ready to create the ACM database, add ready_to_create:True.
    @param sf_data: data from salesforce.
    @return: None
    """
    def parse_sdg(sdg: str) -> List[str]:
        sdg_re = re.compile(r'^(\d+(?:\.\d{0,2})?)')
        result = []
        for s in sdg.split(';'):
            sdg_match = sdg_re.match(s)
            if sdg_match:
                result.append(sdg_match.group(1))
        return result

    def parse_language(language: str) -> List[str]:
        languages = [x.strip() for x in language.split(';')]
        return [x for x in languages if x]

    # Salesforce sends integers as floats (eg, "1.0"). Drop the ".0" part.
    float_re = re.compile(r'^(\d+)\.0$')
    for k, v in sf_data.items():
        v_match = float_re.match(v)
        if v_match:
            sf_data[k] = v_match.group(1)
    if 'sdg' in sf_data:
        sf_data['sdg'] = parse_sdg(sf_data['sdg'])
    if 'language' in sf_data:
        sf_data['language'] = parse_language(sf_data['language'])
    if 'program_id' not in sf_data and 'tech_poc' in sf_data and sf_data.get('stage') in READY_STAGES:
        # wouldn't be here without program_name, and customer.
        sf_data['ready_to_create'] = True


def process_data_from_sf(sf_data: dict[str, str]) -> bool:
    global email_text
    if not has_required_fields(sf_data):
        return False
    normalize_data(sf_data)
    sf_db = SfToAws()
    sf_db.add_record(sf_data)
    update_sql(sf_data)
    return True


# noinspection PyUnusedLocal,PyBroadException
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
    print(f'Event: {event}')

    records = event['Records']
    for record in records:
        email_text.append('\n\n============================================================\n\n')

        bucket = record['s3']['bucket']
        bucket_name = bucket['name']
        object = record['s3']['object']
        key = urllib.parse.unquote_plus(object['key'], encoding='utf-8')

        print(f'Got event for key "{key}" in bucket "{bucket_name}"')
        try:
            sf_data = read_json_from_s3(key=key, bucket=bucket_name)
            if process_data_from_sf(sf_data):
                email_text.append('Good record:')
            else:
                email_text.append('Bad record:')
            for k in sorted(sf_data.keys()):
                email_text.append('{}:"{}"'.format(k, sf_data[k]))
        except Exception as ex:
            email_text.append(f'Exception parsing {key}: {ex}')
            continue

    send_ses(subject='New program records', body='\n'.join(email_text))

    end = time.time_ns()
    print('Event processed in {:,} ns'.format(end - start))
