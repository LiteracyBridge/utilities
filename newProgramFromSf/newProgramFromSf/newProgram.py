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


class SfToAws:
    def __init__(self, **kwargs):

        self._dynamodb_client = boto3.client('dynamodb')  # specify amazon service to be used
        self._dynamodb_resource = boto3.resource('dynamodb')

        self._create_tables()
        self._sf2aws_table = self._dynamodb_resource.Table(_PENDING_PROGRAM_TABLE_NAME)

    def has_record(self, sf_data) -> bool:
        query = self._sf2aws_table.get_item(Key={_PENDING_PROGRAM_TABLE_KEY: sf_data['program_id']})
        sf_row = query.get('Item')
        return True if sf_row else False

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

    def get_record(self, key):
        query = self._sf2aws_table.get_item(Key={_PENDING_PROGRAM_TABLE_KEY: key})
        sf_row = query.get('Item')
        return sf_row

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


# noinspection PyUnusedLocal,PyBroadException
def lambda_handler(event, context):
    # event = {
    #     "Records": [
    #         {
    #             "eventID": "5fffb628dd876e71b75f22f08a9d68ae",
    #             "eventName": "MODIFY",
    #             "eventVersion": "1.1",
    #             "eventSource": "aws:dynamodb",
    #             "awsRegion": "us-west-2",
    #             "dynamodb": {
    #                 "ApproximateCreationDateTime": 1599787181.0,
    #                 "Keys": {
    #                     "programid": {
    #                         "S": "a1t3l000007rnRUAAY"
    #                     }
    #                 },
    #                 "NewImage": {"fields": "newValues"},
    #                 "OldImae": {"fields": "oldValues"},
    #                 "SequenceNumber": "126400000000000732402219",
    #                 "SizeBytes": 6562,
    #                 "StreamViewType": "NEW_AND_OLD_IMAGES"
    #             },
    #             "eventSourceARN": "arn:aws:dynamodb:us-west-2:856701711513:table/sf2aws/stream/2020-09-11T00:49:58.321"
    #         }
    #     ]
    # }

    start = time.time_ns()

    sf2aws = SfToAws()

    records = event['Records']
    for record in records:
        key = record.get('dynamodb', {}).get('Keys', {}).get('programid', {}).get('S')
        if key:
            sf_data = sf2aws.get_record(key)
            print('sf data: {}'.format(sf_data))

    end = time.time_ns()
    print('Event processed in {:,} ns'.format(end - start))


# region Testing Code
if __name__ == '__main__':
    def test_handler():
        event = {'Records': [{
            'dynamodb': {
                'Keys': {
                    'programid': {
                        'S': 'a1t3l000007rnRUAAY'
                    }
                }
            }
        }]}
        lambda_handler(event, None)


    def _main():
        test_handler()


    sys.exit(_main())

# endregion
