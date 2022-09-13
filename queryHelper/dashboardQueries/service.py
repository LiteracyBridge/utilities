# -*- coding: utf-8 -*-
import base64
import csv
import io
import time
from typing import List

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

    'timestamp',
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
# noinspection SqlDialectInspectionForFile
# noinspection SqlNoDataSourceInspection
DEPLOYMENT_BY_COMMUNITY = '''
SELECT DISTINCT 
        td.project, 
        td.deployment, 
        d.deploymentnumber,
        td.contentpackage as package,
        td.recipientid, 
        r.communityname, 
        r.groupname,
        r.agent,
        r.language as languagecode,
        d.startdate,
        d.enddate,
       COUNT(DISTINCT td.talkingbookid) AS deployed_tbs
    FROM tbsdeployed td
    JOIN recipients r
      ON td.recipientid = r.recipientid
    LEFT OUTER JOIN deployments d
      ON d.project=td.project AND d.deployment ilike td.deployment
    WHERE td.project = %s
    GROUP BY td.project, 
        td.deployment, 
        package, 
        d.deploymentnumber,
        td.recipientid, 
        r.communityname, 
        r.groupname, 
        r.agent,
        r.language, 
        d.startdate,
        d.enddate
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
def get_db_connection():
    global db_connection
    if db_connection is None:
        secret = get_secret()

        secret['dbname'] = 'dashboard'
        connect_string = "dbname={dbname} user={username} port={port} host={host} password={password}".format(**secret)

        db_connection = psycopg2.connect(connect_string)
    return db_connection


def query_to_csv(query: str, cursor=None, vars: Tuple = None) -> Tuple[str, int]:
    if cursor is None:
        cursor = get_db_connection().cursor()
    file_like = io.StringIO()
    num_rows = 0
    cursor.execute(query, vars=vars)
    num_columns = len(cursor.description)
    writer = csv.writer(file_like, quoting=csv.QUOTE_MINIMAL)
    writer.writerow([x.name for x in cursor.description])
    for record in cursor:
        num_rows += 1
        writer.writerow(record)
    return file_like.getvalue(), num_rows

def query_to_json(query: str, name_map=None, cursor=None, vars: Tuple=None) -> Tuple[List[Any], int]:
    if cursor is None:
        cursor = get_db_connection().cursor()
    cursor.execute(query, vars=vars)
    names = [x.name for x in cursor.description]
    if name_map:
        names = [name_map.get(name, name) for name in names]
    result = []
    for record in cursor:
        result.append(dict(zip(names, record)))
    return result, len(result)

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

@handler
def usage2(programid: QueryStringParam, deployment: QueryStringParam,
          columns: QueryStringParam = 'deploymentnumber,startdate,category,sum(completions),sum(played_seconds)') -> Any:
    start = time.time_ns()

    sqv2 = SQV2(choosable_columns, temp_view, augment='sum(completions),sum(played_seconds)')
    query, errors = sqv2.parse(columns)
    if errors:
        # http error result code
        return str(errors), 400

    get_db_connection()
    cursor = db_connection.cursor()

    # Create a convenience view limited to the data of interest.
    if deployment:
        print('Program filter: "{}" with: {}, {}'.format(view_query_depl, programid, deployment))
        cursor.execute(view_query_depl, (programid, deployment))
    else:
        cursor.execute(view_query, (programid,))

    # Run the query
    usage, num_rows = query_to_csv(query, cursor=cursor)
    end = time.time_ns()
    print('{} rows in {} mSec'.format(num_rows, (end - start) / 1000000))
    if debug and num_rows < 10:
        print('values: {}'.format(usage))

    return usage


# noinspection SqlNoDataSourceInspection
@handler
def recipients(programid: QueryStringParam) -> Any:
    # noinspection SqlResolve
    query = 'SELECT * FROM recipients WHERE project ILIKE %s;'
    vars = (programid,)
    recipients, numrecips = query_to_csv(query, cursor=get_db_connection().cursor(), vars=vars)
    print('{} recipients found for program {}'.format(numrecips, programid))
    return recipients


# noinspection SqlNoDataSourceInspection
@handler
def deployments(programid: QueryStringParam) -> Any:
    # noinspection SqlResolve
    query = 'SELECT * FROM deployments WHERE project ILIKE %s;'
    vars = (programid,)
    deployments, numdepls = query_to_csv(query, cursor=get_db_connection().cursor(), vars=vars)
    print('{} deployments found for program {}'.format(numdepls, programid))
    return deployments


# noinspection SqlNoDataSourceInspection
@handler
def tbsdeployed(programid: QueryStringParam) -> Any:
    # noinspection SqlResolve
    query = '''
    SELECT tbd.talkingbookid,tbd.recipientid,tbd.deployedtimestamp,dep.deploymentnumber,tbd.deployment,
           tbd.contentpackage,tbd.username,tbd.tbcdid,tbd.action,tbd.newsn,tbd.testing
      FROM tbsdeployed tbd
      JOIN deployments dep
        ON tbd.project=dep.project AND tbd.deployment=dep.deployment
     WHERE tbd.project ILIKE %s
     ORDER BY dep.deploymentnumber, tbd.recipientid;
    '''
    vars = (programid,)
    tbsdeployed, numtbs = query_to_csv(query, cursor=get_db_connection().cursor(), vars=vars)
    print('{} tbs deployed found for {}'.format(numtbs, programid))
    return tbsdeployed

# noinspection SqlNoDataSourceInspection
@handler
def depl_by_community(programid: QueryStringParam) -> Any:
    # noinspection SqlResolve
    vars = (programid,)
    tbsdeployed, numdepls = query_to_csv(DEPLOYMENT_BY_COMMUNITY, cursor=get_db_connection().cursor(), vars=vars)
    print('{} deployments-by-community found for {}'.format(numdepls, programid))
    return tbsdeployed

@handler(roles=None)
def supported_languages(programid: QueryStringParam):
    map = {'languagecode':'code', 'languagename':'name', 'comments':'comments'}
    # Only global "supportedlanguages" supported at this point; per-program language support TBD
    supported_languages, numlangs = query_to_json('SELECT * FROM supportedlanguages;', name_map=map, cursor=get_db_connection().cursor())
    print('{} supported languages'.format(numlangs))
    return supported_languages

def lambda_handler(event, context):
    the_router = LambdaRouter(event, context)
    action = the_router.path_param(0)
    print('Action is {}'.format(action))
    return the_router.dispatch(action)


if __name__ == '__main__':
    """
    Tests.
    """
    debug = True
    #             claims = event['requestContext']['authorizer'].get('claims', {})
    event = {'requestContext': {'authorizer': {'claims': {'email': 'bill@amplio.org'}}},
             'pathParameters': {
                 'proxy': 'usage/TEST/1'
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

    event['queryStringParameters']['programid'] = 'TEST'
    event['queryStringParameters']['deployment'] = 1
    del event['queryStringParameters']['cols']
    event['queryStringParameters']['columns'] = 'deploymentnumber,district,sum(completions),sum(played_seconds)'
    event['pathParameters'] = {'proxy': 'usage2'}
    result = lambda_handler(event, None)
    body = json.loads(result['body'])
    status_code = result['statusCode']
    print("\n\nStatus code {}\n     Result {}".format(status_code, body))

    event['pathParameters'] = {'proxy': 'recipients'}
    result = lambda_handler(event, None)
    print(result)

    event['pathParameters'] = {'proxy': 'tbsdeployed'}
    result = lambda_handler(event, None)
    print(result)

    event['pathParameters'] = {'proxy': 'depl_by_community'}
    result = lambda_handler(event, None)
    print(result)

    event['pathParameters'] = {'proxy': 'supported_languages'}
    event['queryStringParameters']['programid']
    result = lambda_handler(event, None)
    print(result)

    print('Done.')
