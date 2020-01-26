# -*- coding: utf-8 -*-
import base64
import csv
import io
import json
import re
import time

import boto3
import psycopg2
from botocore.exceptions import ClientError
# import requests
from utils.SimpleQueryValidator import SimpleQueryValidator, QueryColumn

db_connection = None
debug = False

class TableAccessChecker:
    def __init__(self, event):
        self._edit = ''
        self._view = 'DEMO'
        if 'requestContext' in event and 'authorizer' in event['requestContext']:
            claims = event['requestContext']['authorizer'].get('claims', {})
            self._edit = claims.get('edit', '')
            self._view = claims.get('view', 'DEMO')

    def can_view(self, project):
        return re.match(self._view, project) or re.match(self._edit, project)

    def can_edit(self, project):
        return re.match(self._edit, project)


accessChecker = TableAccessChecker('')

# The event object includes:
# ['requestContext']['authorizer']['claims'] with values
#    admin: true/false, based on user
#    edit:  regex of projects user can edit
#    view:  regex of projects user can view
#    email: email address of user
#      sub: appears to be the unique cognito user id.


aggregations = {'sum': 'sum(', 'count': 'count(distinct '}

# A map of the columns that the user may request.
choosable_columns = [
    'deploymentnumber',
    'deployment',
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

    'category',
    'playlist',
    'sdg_goals',
    'sdg_targets',
    'contentid',
    'title',
    'format',
    'duration_seconds',
    'position',

    'timestamp',

    'played_seconds',
    'completions',
    'threequarters',
    'half',
    'quarter',
    'started',

    'tbcdid'
]

implied_columns = {
    # 'agent': ('communityname', 'groupname', 'agent'),
    # 'groupname': ('communityname', 'groupname'),
    'recipient': ('communityname', 'groupname', 'agent'),
    'deployment': ('deploymentnumber', 'deployment'),
    'startdate': ('deploymentnumber', 'startdate')
}

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


def get_usage_params(request):
    params = {}
    args = request.get('args', [])
    kwargs = request.get('kwargs', {})

    # Default query if none specified
    params['cols'] = kwargs.get('cols', 'deploymentnumber,startdate,category,sum(completions),sum(played_seconds)')
    params['project'] = args[0] if len(args) > 0 else 'DEMO'
    params['deployment'] = args[1] if len(args) > 1 else None

    sqv = SimpleQueryValidator(choosable_columns)
    # Always include sum(completions) and sum(played_seconds). If these aren't included in the query from the user,
    # we'll add them later.
    cpl = QueryColumn(column='completions', agg='sum')
    seconds = QueryColumn(column='played_seconds', agg='sum')
    # Parse the user's query.
    parsed = sqv.parse(params['cols'])
    # If there was an error parsing, that'll be the result returned. Otherwise build the SQL query.
    if isinstance(parsed, str):
        params['error'] = parsed
    else:
        if cpl not in parsed:
            parsed.append(cpl)
        if seconds not in parsed:
            parsed.append(seconds)
        params['query'] = sqv.make_query(parsed, temp_view)
        params['columns'] = [x.name for x in parsed]

    return params


def get_usage(request):
    start = time.time_ns()
    usage_params = get_usage_params(request)
    if 'error' in usage_params:
        end = time.time_ns()
        return {'error': usage_params['error'], 'msec': (end - start) / 1000000}

    make_db_connection()
    cur = db_connection.cursor()

    # Create a convenience view limited to the data of interest.
    if usage_params['deployment']:
        cur.execute(view_query_depl, (usage_params['project'], usage_params['deployment']))
    else:
        cur.execute(view_query, (usage_params['project'],))

    # Run the query
    num_rows = 0
    file_like = io.StringIO()
    cur.execute(usage_params['query'])
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

    return {'column_descriptions': {x.name: x.type_code for x in cur.description}, 'columns': usage_params['columns'],
            'row_count': num_rows, 'msec': (end - start) / 1000000,
            'query': usage_params['query'], 'values': file_like.getvalue()}


# noinspection SqlResolve
def get_projects():
    """ Retrieve a list of projects the user can see.
    :return: A struct {'msec': query-time, 'values': [proj, proj, ...]}
    """
    start = time.time_ns()
    make_db_connection()
    cur = db_connection.cursor()
    cur.execute('SELECT projectcode FROM projects WHERE id>0;')
    end = time.time_ns()
    projects = [r[0] for r in cur if accessChecker.can_view(r[0])]
    return {'msec': (end - start) / 1000000, 'values': projects}


def request_from_event(event):
    request = {'query': '', 'args': [], 'kwargs': {}}
    if 'path' not in event:
        request['query'] = 'projects'
    else:
        parts = event.get('path').split('/')[1:]
        if len(parts) == 0:
            request['query'] = 'projects'
        else:
            request['query'] = parts[0]
            request['args'] = parts[1:]

    if event.get('queryStringParameters', None):
        request['kwargs'] = event['queryStringParameters']
    request['event'] = event
    return request


def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    # The event object includes:
    # ['requestContext']['authorizer']['claims'] with values
    #    admin: true/false, based on user
    #     edit:  regex of projects user can edit
    #     view:  regex of projects user can view
    #    email: email address of user
    #      sub: appears to be the unique cognito user id.
    # ['path'] the path of the query
    # ['queryStringParameters'] a map of parameter names to values

    global accessChecker
    accessChecker = TableAccessChecker(event)
    request = request_from_event(event)
    result = {}

    # temp
    #     if 'body-json' in request['event']:
    #         request['query'] = 'usage'
    # temp
    if (debug):
        print('Request: ' + str(request))

    if request['query'] == 'projects':
        result = get_projects()
    elif request['query'] == 'usage':
        result = get_usage(request)

    return {
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body":
            json.dumps({"result": result,
                        "event": event
                        })
    }


if __name__ == '__main__':
    debug = True
    #             claims = event['requestContext']['authorizer'].get('claims', {})
    event = {'requestContext': {'authorizer': {'claims': {'edit': '.*', 'view': '.*'}}},
             'path': 'handler/usage/UNICEF-2/5',
             'queryStringParameters': {
                 'cols': 'deploymentnumber,district,count(talkingbookid),sum(played_seconds)/count(talkingbookid)as secs_per_tb'}}

    result = lambda_handler(event, None)
    body = json.loads(result['body'])
    print("\n\nResult:")
    print(body['result'])
    print('Done.')