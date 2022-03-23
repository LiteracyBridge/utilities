# -*- coding: utf-8 -*-
import base64
import csv
import io
import time

import boto3
import psycopg2
from amplio.rolemanager import manager
from amplio.utils.AmplioLambda import *
from botocore.exceptions import ClientError

# import requests
from utils.SimpleQueryValidator import SQV2

db_connection = None
debug = False

manager.open_tables()

# A map of the columns that the user may request.
choosable_columns = [
    'deploymentnumber',
    'deployment',
    'deploymentname',
    'startdate',

    'contentpackage',
    'languagecode',
    'language',

    'partner',
    'affiliate',

    'country',
    'region',
    'district',
    'communityname',
    'groupname',
    'agent',
    'recipientid',
    'talkingbookid',
    'deployment_uuid',

    'category',
    'playlist',
    'sdg_goals',
    'sdg_targets',
    'contentid',
    'title',
    'format',
    'duration_seconds',
    'position',

    'timestamp',  # == stats_timestamp
    'deployment_timestamp',

    'played_seconds',
    'completions',
    'threequarters',
    'half',
    'quarter',
    'started',

    'tbcdid'
]

view_query = '''
CREATE OR REPLACE TEMP VIEW temp_usage AS (
  SELECT * FROM usage_info 
  WHERE recipientid IN (SELECT recipientid FROM recipients WHERE project ILIKE %s) 
    AND played_seconds>0
);
'''
view_query_depl = '''
CREATE OR REPLACE TEMP VIEW temp_usage AS (
  SELECT * FROM usage_info 
  WHERE recipientid IN (SELECT recipientid FROM recipients WHERE project ILIKE %s) 
    AND played_seconds>0
    AND deploymentnumber = %s
);
'''

temp_view = 'temp_usage'


# Get the user name and password that we need to sign into the SQL database. Configured through AWS console.
def get_secret():
    result = ''
    secret_name = "lb_stats_access2"
    region_name = "us-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            result = json.loads(secret)
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            result = decoded_binary_secret

    # Your code goes here.
    return result


# Make a connection to the SQL database
def make_db_connection():
    global db_connection
    secret = get_secret()

    secret['dbname'] = 'dashboard'
    connect_string = "dbname={dbname} user={username} port={port} host={host} password={password}".format(**secret)

    db_connection = psycopg2.connect(connect_string)


def get_usage(program: str, columns: str, deployment: str):
    start = time.time_ns()

    sqv2 = SQV2(choosable_columns, temp_view, augment='sum(completions),sum(played_seconds)')
    query, errors = sqv2.parse(columns)
    if errors:
        return str(errors)

    make_db_connection()
    cur = db_connection.cursor()

    # Create a convenience view limited to the data of interest.
    if deployment:
        print('Program filter: "{}" with: {}, {}'.format(view_query_depl, program, deployment))
        cur.execute(view_query_depl, (program, deployment))
    else:
        cur.execute(view_query, (program,))

    # Run the query
    num_rows = 0
    file_like = io.StringIO()

    print('Main query: "{}"'.format(query))
    cur.execute(query)
    num_columns = len(cur.description)
    writer = csv.writer(file_like, quoting=csv.QUOTE_MINIMAL)
    writer.writerow([x.name for x in cur.description])
    for record in cur:
        num_rows += 1
        writer.writerow([str(record[ix]) for ix in range(0, num_columns)])
    end = time.time_ns()
    print('{} rows in {} mSec'.format(num_rows, (end - start) / 1000000))
    if debug and num_rows < 10:
        print('values: {}'.format(file_like.getvalue()))

    return {'column_descriptions': {x.name: x.type_code for x in cur.description},
            'row_count': num_rows, 'msec': (end - start) / 1000000,
            'query': query, 'values': file_like.getvalue()}


@handler
def usage(programid: str = path_param(1), deployment: str = path_param(2),
          cols: QueryStringParam = 'deploymentnumber,startdate,category,sum(completions),sum(played_seconds)') -> Any:
    result = get_usage(program=programid, columns=cols, deployment=deployment)
    # A string is an error message.
    return result, (400 if isinstance(result, str) else 200)


def lambda_handler(event, context):
    the_router = LambdaRouter(event, context)
    action = the_router.path_param(0)
    return the_router.dispatch(action)


if __name__ == '__main__':
    """
    Tests.
    """
    debug = True
    #             claims = event['requestContext']['authorizer'].get('claims', {})
    event = {'requestContext': {'authorizer': {'claims': {'email': 'bill@amplio.org'}}},
             'path': '/usage/LBG-COVID19/4/',
             'pathParameters': {
                 'proxy': 'usage/LBG-COVID19/4'
             },
             'queryStringParameters': {
                 'cols': 'deploymentnumber,district,sum(completions),sum(played_seconds)'}}

    result = lambda_handler(event, None)
    body = json.loads(result['body'])
    status_code = result['statusCode']
    print("\n\nStatus code {}\n     Result {}".format(status_code, body))

    event['queryStringParameters']['cols'] = 'unknown,missing,invalid,caps(nothing)'
    result = lambda_handler(event, None)
    body = json.loads(result['body'])
    status_code = result['statusCode']
    print("\n\nStatus code {}\n     Result {}".format(status_code, body))

    print('Done.')
