import json
import re
import sys
import time
import traceback
import urllib
from typing import Dict, List

import amplio.rolemanager.manager as roles_manager
from amplio.rolemanager.Roles import *

import boto3

"""
Create an interface between Salesforce and AWS.

"""

roles_manager.open_tables()

STATUS_OK = 'ok'
STATUS_FAILURE = 'failure'
STATUS_EXTRA_PARAMETER = 'Extraneous parameter'
STATUS_ACCESS_DENIED = 'Access denied'
STATUS_MISSING_PARAMETER = 'Missing parameter'

_PENDING_PROGRAM_TABLE_NAME = 'sf2aws'
_PENDING_PROGRAM_TABLE_KEY = 'programid'
_PENDING_PROGRAMS_TABLE_CREATE_ARGS = {
    'TableName': _PENDING_PROGRAM_TABLE_NAME,
    'AttributeDefinitions': [{'AttributeName': _PENDING_PROGRAM_TABLE_KEY, 'AttributeType': 'S'}],
    'KeySchema': [{'AttributeName': _PENDING_PROGRAM_TABLE_KEY, 'KeyType': 'HASH'}]
}
# limit read/write capacity units of table (affects cost of table)
_DEFAULT_PROVISIONING = {'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}

# If there is a name in parens, extracts it to a separate group. For parsing things like
# Literacy Bridge Ghana (LBG)
PARENS_RE = re.compile(r'(.*?)?(?:\(([^()]*)\))?$')
BAD_CHARS_RE = re.compile('[<>:"/\\\\|?*\x01-\x1f]')


class SfToAws:
    def __init__(self, **kwargs):
        """Initialize the instance, open the database, ensure the tables exist."""
        self._dynamodb_client = boto3.client('dynamodb')  # specify amazon service to be used
        self._dynamodb_resource = boto3.resource('dynamodb')

        self._create_tables()
        self._sf2aws_table = self._dynamodb_resource.Table(_PENDING_PROGRAM_TABLE_NAME)

    def has_record(self, sf_data) -> bool:
        query = self._sf2aws_table.get_item(Key={_PENDING_PROGRAM_TABLE_KEY: sf_data['program_id']})
        sf_row = query.get('Item')
        return True if sf_row else False

    def get_record(self, sf_data) -> bool:
        query = self._sf2aws_table.get_item(Key={_PENDING_PROGRAM_TABLE_KEY: sf_data['program_id']})
        sf_row = query.get('Item')
        return sf_row

    def add_record(self, sf_data, **kwargs):
        query = self._sf2aws_table.get_item(Key={_PENDING_PROGRAM_TABLE_KEY: sf_data['program_id']})
        sf_row = query.get('Item')
        if sf_row:
            # update
            sf_row['updated_sf_data'] = sf_data
        else:
            sf_row = {'sf_data': sf_data}

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
        create(_PENDING_PROGRAMS_TABLE_CREATE_ARGS)

        await_tables()

        end_time = time.time()
        print('Created {} tables in {:.2g} seconds'.format(num_updated, end_time - start_time))


s3_client = boto3.client('s3')
SF_TO_AWS_BUCKET: str = 'amplio-sf-to-aws'


# Format and send an ses message. Options are
# html    - if true, send as html format
# dry_run - if true, do not actually send email
def send_ses(subject: str,
             body: str,
             sender: str = 'ictnotification@literacybridge.org',
             recipient: str = 'bill@amplio.org',
             as_html: bool = False):
    """Send an email via the Amazon SES service.

    Example:
      send_ses('me@example.com, 'greetings', "Hi!", 'you@example.com)

    Return:
      If 'ErrorResponse' appears in the return message from SES,
      return the message, otherwise return an empty '' string.
    """

    def is_list_like(obj):
        if isinstance(obj, str):
            return False
        try:
            iter(obj)
        except Exception:
            return False
        else:
            return True

    if is_list_like(recipient):
        to_addresses = [str(x) for x in recipient]
    else:
        to_addresses = [str(recipient)]

    message = {'Subject': {'Data': subject}}
    if as_html:
        message['Body'] = {'Html': {'Data': body}}
    else:
        message['Body'] = {'Text': {'Data': body}}

    client = boto3.client('ses')
    response = client.send_email(
        Source=sender,
        Destination={
            'ToAddresses': to_addresses
        },
        Message=message
    )
    print('Sent email, response: {}'.format(response))
    return response


def get_json(key: str, bucket: str = SF_TO_AWS_BUCKET) -> Dict[str, str]:
    def norm_k(k: str) -> str:
        k = str(k)
        return k.replace(' ', '_').lower()

    def norm_v(v: str) -> str:
        v = str(v)
        return v.replace('"', '\\"')

    obj = s3_client.get_object(Bucket=bucket, Key=key)
    raw_data = obj.get('Body').read()
    data = json.loads(raw_data.decode('utf-8'))
    return {norm_k(k): norm_v(v) for k, v in data.items()}


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

    start = time.time_ns()
    print('Event: {}'.format(event))

    sf_db = SfToAws()

    email_text = []

    records = event['Records']
    for record in records:
        bucket = record['s3']['bucket']
        bucket_name = bucket['name']
        object = record['s3']['object']
        key = urllib.parse.unquote_plus(object['key'], encoding='utf-8')

        print('Got event for key "{}" in bucket "{}"'.format(key, bucket_name))
        sf_data = get_json(key=key, bucket=bucket_name)

        if (check_fields(sf_data, email_text)):
            data = compute_fields(sf_data, is_new=not sf_db.has_record(sf_data))
            sf_db.add_record(sf_data, **data)
            print(data)
            for k, v in data.items():
                email_text.append('{}:"{}"'.format(k, v))

            net_data = sf_db.get_record(sf_data)
            if all([x in net_data for x in ['customer', 'affiliate', 'tech_poc', 'acm']]):
                email_text.append(
                    'newAcm --org "{customer}" --parent "{affiliate}" --admin {tech_poc} "{acm}"'.format(**net_data))
        else:
            email_text.append('Full record:')
            for k, v in sf_data.items():
                email_text.append('{}:"{}"'.format(k, v))
            email_text.append(' ')

    send_ses(subject='New program records', body='\n'.join(email_text))

    end = time.time_ns()
    print('Event processed in {:,} ns'.format(end - start))


def check_fields(sf_data, messages: List[str]) -> bool:
    result = True
    if 'customer' not in sf_data:
        messages.append('Missing value for "Customer"')
        result = False
    if 'affiliate' not in sf_data:
        messages.append('Missing value for "Affiliate"')
        result = False
    if 'program_name' not in sf_data:
        messages.append('Missing value for "Program Name"')
        result = False
    # if 'tech_poc' not in sf_data:
    #     messages.append('Missing value for "Tech POC"')
    #     result = False
    return result


def compute_fields(sf_data, is_new):
    data = {}
    if is_new:
        for k, v in sf_data.items():
            data[k] = v
        if 'acm' not in sf_data:
            affiliate_abbr = abbrev(sf_data['affiliate'], preserve=4)
            prog_abbr = abbrev(sf_data['program_name'], preserve=7)
            data['acm'] = affiliate_abbr + '-' + prog_abbr
        if 'sdg' in sf_data:
            data['sdg'] = parse_sdg(sf_data['sdg'])
        if 'language' in sf_data:
            data['language'] = parse_language(sf_data['language'])

    data[_PENDING_PROGRAM_TABLE_KEY] = sf_data['program_id']
    data['verified'] = False
    return data


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


def abbrev(s: str, preserve: int = 0) -> str:
    a_match = PARENS_RE.match(s)
    if a_match.lastindex == 2:
        abbrev = a_match.group(2)
    else:
        # [<>:"/\\|?*\x01-\x31]
        good_chars = re.sub(BAD_CHARS_RE, '', a_match.group(1))
        if len(good_chars) <= preserve:
            abbrev = re.sub('[ .]', '_', good_chars).upper()
        else:
            words = re.split(r'\W', good_chars)
            letters = [w[0] for w in words if w and not w.isnumeric()]
            abbrev = ''.join(letters).upper()
    return abbrev


# region Testing Code
if __name__ == '__main__':
    programs = ["Niger Smart Villages Parent Program",
                "VSO Zambia",
                "CARE Ethiopia",
                "ECHOES World Cocoa Foundation",
                "Lumana Sale 2010",
                "Performances (Burkina Faso)",
                "UNFM - Cameroon Pilot",
                "UNICEF Rwanda 2017",
                "War Child Holland",
                "Ghana Health Service 2011 Maternal & Child Health Program",
                "Ghana Health Service 2012-2014 Maternal & Child Health Program",
                "Al Turay-Sierra Leon Program",
                "LDS/Papua New Guinea Program - Brett Macdonald - 1/24/2012",
                "LDS/Papua New Guinea Program - Brett Macdonald - 10/6/2011",
                "LDS/Papua New Guinea Program - Brett Macdonald - 4/29/2012",
                "Cecily's Fund",
                "Busara Somalia Talking Book Program Initial Pilot",
                "Busara Somalia Talking Book Pilot PHASE II",
                "LDS/Papua New Guinea Parent Program 2011-12",
                "RTI / FIP Pilot Agriculture and Nutrition Project 2018/2019",
                "IFDC 2scale Meru",
                "IFDC 2scale Uganda",
                "GAIN Maternal, Infant, & Young Child Nutrition",
                "AFYA TIMIZA 2018",
                "AFYA TIMIZA 2019 Q1",
                "AFYA TIMIZA 2019 Q2",
                "AFYA TIMIZA 2019 Q3",
                "Anzilisha 2017-2018",
                "Afya Timiza Parent Program",
                "MEDA GROW Ghana Parent Program 2013-2018",
                "MEDA/Esoko FEATS Project",
                "APME.2A",
                "CARE Ghana Parent Program 2014-2018",
                "UNICEF Ghana Parent Program 2013-2020",
                "Ghana MoFA/VSO Upper West Sale - 2/25/2011",
                "Ghana MoFA/VSO Upper West Sale - 6/17/2010",
                "MEDA Ghana 2013-2014",
                "MEDA Ghana 2015-2018",
                "MEDA Ghana 2018",
                "MEDA Ghana, UNICEF 2017-2018 extra 20",
                "UNICEF 2013-2017",
                "UNICEF 2017-2020",
                "UNICEF CHPS Program 2019",
                "Winrock Ghana 2018",
                "AGRA EISFERM TUDRIDEP Project",
                "CARE Ghana Pathways 2014-2015",
                "CARE Ghana Pathways 2016",
                "CARE Ghana Pathways 2017",
                "CARE Ghana Pathways 2018",
                "CARE Ghana Pathways 2017-2018 with UNICEF 20",
                "LBG COVID19 & Meningitis Response",
                "GHS Parent Program 2011-2014",
                "Ghana MoFA/VSO Parent Program 2010-11",
                "Tsehai Loves English Parent Program",
                "Tsehai Loves English Phase I"]

    affiliates = ['Amplio',
                  'Whiz Kids Workshop',
                  'Literacy Bridge Ghana (LBG)',
                  'Centre for Behaviour Change and Communication (CBCC)']


    def test_handler():
        event = {'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'us-west-2',
                              'eventTime': '2020-09-10T19:01:29.427Z', 'eventName': 'ObjectCreated:Put',
                              'userIdentity': {'principalId': 'AWS:AIDAJGWFCEB6RK5NV6SBG'},
                              'requestParameters': {'sourceIPAddress': '172.92.94.105'},
                              'responseElements': {'x-amz-request-id': '567BE63738FECD23',
                                                   'x-amz-id-2': 'J33azBbvcofuKhk6CZoVmw1BGZcTZecQLiQ+V2cbMTmWUgZT1L0M387VOqrdz6CNJUy7T9otUb/g4O28kiCSQLi0cRQiMkwA'},
                              's3': {'s3SchemaVersion': '1.0', 'configurationId': 'SfToAwsPut',
                                     'bucket': {'name': 'amplio-sf-to-aws',
                                                'ownerIdentity': {'principalId': 'A3MRMLZEL3RVRR'},
                                                'arn': 'arn:aws:s3:::amplio-sf-to-aws'},
                                     'object': {'key': 'a1t3l000006t8QCAAY.json', 'size': 1453,
                                                'eTag': '47aa7471b336f417227849cab868e99f',
                                                'sequencer': '005F5A780A3AF59216'}}}]}
        lambda_handler(event, None)


    def test_abbrev():
        print('Programs:')
        for p in programs:
            print('{}: {}'.format(abbrev(p), p))

        print('\n\nAffiliates:')
        for x in affiliates:
            print('{}: {}'.format(abbrev(x), x))
        # data = get_json(key='a1t3l000007rnRUAAY.json')
        # affiliate = data['affiliate']
        # abbr = abbrev(affiliate)
        # print(abbr)


    def _main():
        test_handler()


    sys.exit(_main())

    # x = {'Event': {'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'us-west-2',
    #                             'eventTime': '2020-09-10T19:01:29.427Z', 'eventName': 'ObjectCreated:Put',
    #                             'userIdentity': {'principalId': 'AWS:AIDAJGWFCEB6RK5NV6SBG'},
    #                             'requestParameters': {'sourceIPAddress': '172.92.94.105'},
    #                             'responseElements': {'x-amz-request-id': '567BE63738FECD23',
    #                                                  'x-amz-id-2': 'J33azBbvcofuKhk6CZoVmw1BGZcTZecQLiQ+V2cbMTmWUgZT1L0M387VOqrdz6CNJUy7T9otUb/g4O28kiCSQLi0cRQiMkwA'},
    #                             's3': {'s3SchemaVersion': '1.0', 'configurationId': 'SfToAwsPut',
    #                                    'bucket': {'name': 'amplio-sf-to-aws',
    #                                               'ownerIdentity': {'principalId': 'A3MRMLZEL3RVRR'},
    #                                               'arn': 'arn:aws:s3:::amplio-sf-to-aws'},
    #                                    'object': {'key': 'a1t3l000007rnRUAAY.json', 'size': 1453,
    #                                               'eTag': '47aa7471b336f417227849cab868e99f',
    #                                               'sequencer': '005F5A780A3AF59216'}}}]}}
# endregion
