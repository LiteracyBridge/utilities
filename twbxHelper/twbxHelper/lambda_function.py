import binascii
import datetime
import distutils.util
import json
import os
import time
import traceback

import boto3
from amplio.rolemanager import manager as role_manager

"""
Tableau Workbook helper.

An AWS Lambda function to upload .twbx files and "teaser" .pngs, and to server those
files to a web page (eg, in the dashboard).

Functions available:



"""

WORKBOOK_SUFFIX = '_stats.twbx'
PREVIEW_SUFFIX = '_stats.png'

# CURRENT_PROGSPEC_KEY = 'program_spec.xlsx'
# PENDING_PROGSPEC_KEY = 'pending_spec.xlsx'
# DEPLOYMENT_SPEC_KEY = 'deployment_spec.csv'
# TALKINGBOOK_MAP_KEY = 'talkingbook_map.csv'
# RECIPIENTS_MAP_KEY = 'recipients_map.csv'
# RECIPIENTS_KEY = 'recipients.csv'
# CONTENT_JSON_KEY = 'content.json'
# CONTENT_CSV_KEY = 'content.csv'
#
# PARTS_FILES = [DEPLOYMENT_SPEC_KEY, TALKINGBOOK_MAP_KEY, RECIPIENTS_MAP_KEY,
#                RECIPIENTS_KEY, CONTENT_JSON_KEY, CONTENT_CSV_KEY]
# FILE_ENCODINGS = {DEPLOYMENT_SPEC_KEY: 'utf-8',
#                   TALKINGBOOK_MAP_KEY: 'utf-8',
#                   RECIPIENTS_MAP_KEY: 'utf-8',
#                   RECIPIENTS_KEY: 'utf-8',
#                   CONTENT_JSON_KEY: 'utf-8',
#                   CONTENT_CSV_KEY: 'utf-8-sig'}
#
# LIST_VERSION_KEYS = [CURRENT_PROGSPEC_KEY, PENDING_PROGSPEC_KEY]

STATUS_OK = 'ok'
STATUS_FAILURE = 'failure'
STATUS_EXTRA_PARAMETER = 'Extraneous parameter'
STATUS_ACCESS_DENIED = 'Access denied'
STATUS_MISSING_PARAMETER = 'Missing parameter'

s3 = boto3.client('s3')

bucket = 'dashboard-lb-stats'

role_manager.open_tables()


class Authorizer:
    # noinspection PyUnusedLocal
    @staticmethod
    def is_authorized(claims, action, program: str = None):
        email = claims.get('email')
        roles_str = role_manager.get_roles_for_user_in_program(email, program)
        print('Roles for {} in {}: {}'.format(email, program, roles_str))
        if action == 'approve-progspec':
            return role_manager.Roles.PM_ROLE in roles_str
        return roles_str is not None and len(roles_str) > 0


authorizer: Authorizer = Authorizer()


# Given a value that may be True/False or may be a [sub-]string 'true'/'false', return the truth value.
def _bool_arg(arg, default=False):
    if type(arg) == bool:
        return arg
    elif arg is None:
        return default
    try:
        return bool(distutils.util.strtobool(arg))
    except ValueError:
        return default


def object_key(program: str, file: str):
    return 'twbx/{0}/{0}{1}'.format(program, WORKBOOK_SUFFIX if file == 'workbook' else PREVIEW_SUFFIX)


# Given a program or ACM name, return just the program name part, uppercased. ACM-TEST -> TEST, test -> TEST
def cannonical_acm_project_name(acmdir):
    if acmdir is None:
        return None
    _, acm = os.path.split(acmdir)
    acm = acm.upper()
    if acm.startswith('ACM-'):
        acm = acm[4:]
    return acm


def get_s3_params_and_obj_info(program: str, file: str) -> (object, object):
    params = {'Bucket': bucket}
    key = object_key(program, file)

    params['Key'] = key
    head = s3.head_object(**params)
    obj_info = {'Metadata': head.get('Metadata'),
                'VersionId': head.get('VersionId'),
                'Size': head.get('ContentLength'),
                'LastModified': head.get('LastModified').isoformat(),
                'Key': params.get('Key')}
    return params, obj_info


# Returns a signed link to download a version of the program spec.
def do_getlinks(params, claims):
    program = params.get('program') if 'program' in params else params.get('project')
    if not program:
        return {'status': STATUS_MISSING_PARAMETER, 'output': ['Must specify program.']}
    program = cannonical_acm_project_name(program)
    if not authorizer.is_authorized(claims, 'get-link', program):
        return {'status': STATUS_ACCESS_DENIED, 'output': ['Access denied']}

    params, _ = get_s3_params_and_obj_info(program, 'workbook')
    workbook_url = s3.generate_presigned_url('get_object', Params=params, ExpiresIn=3600)
    params, _ = get_s3_params_and_obj_info(program, 'preview')
    preview_url = s3.generate_presigned_url('get_object', Params=params, ExpiresIn=3600)
    return {'workbook': workbook_url, 'preview': preview_url, 'status': 'ok'}


# Submit a new program spec. If the new spec passes validation, it Becomes the "pending" spec.
def do_submit(data, params, claims):  # program, metadata):
    program = params.get('program') if 'program' in params else params.get('project')
    if not program:
        return {'status': STATUS_MISSING_PARAMETER, 'output': ['Must specify program.']}
    program = cannonical_acm_project_name(program)
    if not authorizer.is_authorized(claims, 'submit-progspec', program):
        return {'status': STATUS_ACCESS_DENIED, 'output': ['Access denied']}
    file = params.get('file')
    if not file:
        return {'status': STATUS_MISSING_PARAMETER, 'file': ['Must specify file="workbook" or file="preview".']}

    metadata = {'submitter-email': claims.get('email'),
                'submitter-comment': params.get('comment', 'No comment provided'),
                'submission-date': datetime.datetime.now().isoformat()}

    key = object_key(program, file)
    put_result = s3.put_object(Body=data, Bucket=bucket, Metadata=metadata, Key=key)

    result = {'status': STATUS_OK, 'ETag': put_result.get('ETag')}

    return result


# noinspection PyUnusedLocal
def lambda_handler(event, context):
    global authorizer
    start = time.time_ns()

    keys = [x for x in event.keys()]
    # info = {'keys': keys}
    #         'resource': event.get('resource', '-no resource'),
    #         'path': event.get('path', '-no path'),
    #         'httpMethod': event.get("httpMethod", '-no httpMethod')}
    # for key in keys:
    #     if key != 'body':
    #         info[key] = event.get(key, '-no ' + key)
    parts = [x for x in event.get('pathParameters', {}).get('proxy', 'validate').split('/') if x != 'data']
    action = parts[0]

    path = event.get('path', {})
    path_parameters = event.get('pathParameters', {})
    multi_value_query_string_parameters = event.get('multiValueQueryStringParameters', {})
    query_string_params = event.get('queryStringParameters', {})

    print('pathParameters: {}, path: {}, action: {}'.format(path_parameters, path, action))
    print('queryStringParameters: {}'.format(query_string_params))
    result = {'output': [],
              'status': ''}

    data = None
    body = event.get('body')
    if body is None:
        print('Body is None')
    else:
        print('Body is {} characters long'.format(len(body)))
    if body:
        try:
            data = binascii.a2b_base64(body)
        except (binascii.Error, binascii.Incomplete):
            data = None

    claims = event.get('requestContext', {}).get('authorizer', {}).get('claims', {})

    try:
        if action == 'submit':
            result = do_submit(data, query_string_params, claims)

        elif action == 'getlinks':
            result = do_getlinks(query_string_params, claims)

        elif action == 'clear':
            pass

        elif action == 'ping':
            result = {'status': 'ok'}

    except Exception as ex:
        traceback.print_exception(type(ex), ex, ex.__traceback__)
        result['status'] = STATUS_FAILURE
        result['exception'] = 'Exception: {}'.format(ex)

    end = time.time_ns()
    return {
        'statusCode': 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        'body': json.dumps({'msg': 'Program Specification Utility',
                            'keys': keys,
                            'result': result,
                            'claims': claims,
                            'action': action,
                            'path': path,
                            'path_parameters': path_parameters,
                            'query_string_params': query_string_params,
                            'multi_value_query_string_parameters': multi_value_query_string_parameters,
                            'msec': (end - start) / 1000000})
    }


if __name__ == '__main__':
    # Test code, runs on a developer desktop.
    # noinspection PyUnusedLocal
    def __test__():
        from os.path import expanduser
        from pathlib import Path

        _PROGRAM = 'LBG-COVID19'

        def test_submit(fn, file, comment='No commment provided.'):
            print('\nSubmit {}:'.format(fn))
            bytes_read = open(expanduser(fn), "rb").read()
            body_data = binascii.b2a_base64(bytes_read)

            submit_event = {'requestContext': {'authorizer': {'claims': claims}},
                            'pathParameters': {'proxy': 'submit'}, 'queryStringParameters': {'program': _PROGRAM,
                                                                                             'file': file,
                                                                                             'comment': comment},
                            'body': body_data}
            result = lambda_handler(submit_event, {})
            submit_result = json.loads(result['body']).get('result', {})
            print(submit_result)
            print('Submit ' + submit_result['status'])
            return submit_result.get('PendingId')

        def test_get():
            print('\nGet\n')
            event = {'requestContext': {'authorizer': {'claims': claims}},
                     'pathParameters': {'proxy': 'getlinks'},
                     'queryStringParameters': {'program': _PROGRAM}
                     }
            result = lambda_handler(event, {})
            return result

        claims = {'email': 'test@example.org'}
        print('Just testing')

        pending_id = test_submit('~/workspace/utilities/twbxHelper/DEMO_stats.twbx',
                                 file='workbook',
                                 comment='First workbook.')

        pending_id = test_submit('~/workspace/utilities/twbxHelper/DEMO_stats.png',
                                 file='preview',
                                 comment='First preview.')

        get_result = test_get()
        result = json.loads(get_result['body'])['result']
        print('{}\n{}'.format(result['workbook'], result['preview']))
        get_result = test_get()
        result = json.loads(get_result['body'])['result']
        print('{}\n{}'.format(result['workbook'], result['preview']))


    __test__()
