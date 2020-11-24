import binascii
import csv
import datetime
import distutils.util
import io
import json
import os
import time
import traceback
import zipfile
from pathlib import Path
from typing import Dict, Union, Tuple

import boto3
from amplio.rolemanager import manager as role_manager

"""
Tableau Workbook helper.

An AWS Lambda function to upload .twbx files and "teaser" .pngs, and to server those
files to a web page (eg, in the dashboard).

Functions available:



"""
# noinspection DuplicatedCode

WORKBOOK_SUFFIX = '.twbx'
TEMPLATE_SUFFIX = '.template.twbx'
PREVIEW_SUFFIX = '.png'
SUFFIX_MAP = {'workbook': WORKBOOK_SUFFIX, 'preview': PREVIEW_SUFFIX, 'template': TEMPLATE_SUFFIX}
PREVIEW_EXTENSIONS = ['.apng', '.bmp', '.gif', '.jpg', '.jpeg', '.jfif', '.pjpeg', '.pjp', '.png', '.svg', '.webp']

WORKBOOK_XML_SUFFIX = '.twb'
WORKBOOK_DATA_FILENAME = 'usage.csv'

STATUS_OK = 'ok'
STATUS_FAILURE = 'failure'
STATUS_EXTRA_PARAMETER = 'Extraneous parameter'
STATUS_ACCESS_DENIED = 'Access denied'
STATUS_MISSING_PARAMETER = 'Missing parameter'
STATUS_BAD_FILE_TYPE = 'Bad file type'

s3 = boto3.client('s3')

stats_bucket = 'dashboard-lb-stats'

role_manager.open_tables()

ADMIN_REQUIRED_ACTIONS = {'refresh-twbx', 'upload-twbx-template', 'remove-previews'}


class Authorizer:
    # noinspection PyUnusedLocal
    @staticmethod
    def is_authorized(claims, action, program: str = None):
        email = claims.get('email')
        roles_str = role_manager.get_roles_for_user_in_program(email, program)
        print('Roles for {} in {}: {}'.format(email, program, roles_str))
        if action in ADMIN_REQUIRED_ACTIONS:
            return role_manager.Roles.PM_ROLE in roles_str and role_manager.Roles.ADMIN_ROLE in roles_str
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


def object_key(program: str, flavor: str, filename=None) -> str:
    if flavor == 'preview' and filename is not None:
        return f'twbx/{program}/{filename}'

    suffix = SUFFIX_MAP.get(flavor, WORKBOOK_SUFFIX)
    return f'twbx/{program}/{program}{suffix}'


# Given a program or ACM name, return just the program name part, uppercased. ACM-TEST -> TEST, test -> TEST
def cannonical_acm_program_name(acmdir: str) -> Union[str, None]:
    if acmdir is None:
        return None
    _, acm = os.path.split(acmdir)
    acm = acm.upper()
    if acm.startswith('ACM-'):
        acm = acm[4:]
    return acm


def get_s3_params_and_obj_info(program: str, file: str) -> (object, object):
    params = {'Bucket': stats_bucket}
    key = object_key(program, file)

    params['Key'] = key
    head = s3.head_object(**params)
    obj_info = {'Metadata': head.get('Metadata'),
                'VersionId': head.get('VersionId'),
                'Size': head.get('ContentLength'),
                'LastModified': head.get('LastModified').isoformat(),
                'Key': params.get('Key')}
    return params, obj_info


def _delete_objects(bucket=stats_bucket, prefix=None, delete=lambda obj: True):
    """
    Deletes object with the given prefix from the given bucket. A callback lets
    the caller control which individual objects are deleted.
    :param bucket The S3 bucket containing the objects.
    :param prefix Prefix of the objects to be deleted. Must not be None or blank.
    :param delete A callback to control individual deletion.
    """
    num_deleted = 0
    if prefix is None or len(prefix) < 2:
        raise ValueError('Must specify a prefix to delete.')

    print(f'Delete objects in {bucket} with prefix {prefix}')

    # We can to delete up to 1000 objects at a time. So accumulate and delete keys per page.
    to_delete = []
    for obj in _list_objects(prefix=prefix):
        if delete(obj):
            delete_obj = {'Key': obj.get('Key')}
            if 'VersionId' in obj:
                delete_obj['VersionId'] = obj.get('VersionId')
            to_delete.append(delete_obj)
        if len(to_delete) == 1000:
            print('Deleting objects: ' + str(to_delete))
            s3.delete_objects(Delete={'Objects': to_delete}, Bucket=bucket)
            num_deleted += len(to_delete)
            to_delete.clear()

    if len(to_delete) > 0:
        s3.delete_objects(Delete={'Objects': to_delete}, Bucket=bucket)
        num_deleted += len(to_delete)

    return num_deleted


# List the objects with the given prefix.
def _list_objects(bucket=stats_bucket, prefix=''):
    paginator = s3.get_paginator("list_objects_v2")
    kwargs = {'Bucket': bucket, 'Prefix': prefix}
    for objects in paginator.paginate(**kwargs):
        for obj in objects.get('Contents', []):
            yield obj


def _get_program_info(program: str) -> Union[Dict[str, str], None]:
    """
    Gets the content of the projects table for the given program.
    :param program: The program of interest.
    :return: A dict with information about the program. Has at least 'description' as entered by the customer.
    """
    params: Dict[str, str] = {'Bucket': stats_bucket, 'Key': f'data/{program}/program_info.csv'}
    try:
        obj = s3.get_object(**params)
        data = obj.get('Body').read().decode('utf-8')
    except Exception:
        return None
    file_like = io.StringIO(data)
    csvreader = csv.DictReader(file_like)
    return next(csvreader)


def _get_twbx_data(program: str) -> Union[str, None]:
    """
    Gets the data that Tableau will display. Retrieved from S3 bucket dashboard-lb-stats.
    :param program: The program of interest.
    :return: The contents of the .csv file as a string.
    """
    params: Dict[str, str] = {'Bucket': stats_bucket, 'Key': f'data/{program}/twbx_info.csv'}
    try:
        obj = s3.get_object(**params)
        data = obj.get('Body').read().decode('utf-8')
    except Exception:
        return None
    return data


def get_template(program: str) -> Union[Tuple[None, None], Tuple[bytes, bool]]:
    """
    Gets the Tableau workbook template for the given program. If there is a program-specific template, that is
    returned, otherwise a generic (universal) template is returned. The template is in .zip format.
    :param program: The program of interest.
    :return: The data, as a bytes object.
    """
    params: Dict[str, str] = {'Bucket': stats_bucket, 'Key': f'twbx/{program}/{program}{TEMPLATE_SUFFIX}'}
    universal = False
    try:
        s3.head_object(**params)
    except Exception:
        params['Key'] = 'twbx/template.twbx'
        try:
            s3.head_object(**params)
            universal = True
        except Exception:
            # Oye.
            return None, None
    obj = s3.get_object(**params)
    data = obj.get('Body').read()
    return data, universal


def _put_workbook(program: str, workbook: bytes, metadata: Dict[str, str]) -> None:
    """
    Writes a Tableau workbook to S3.
    :param program: The program whose Tableau workbook is to be written.
    :param workbook: The data of the workbook to be written.
    :return: None
    """
    params: Dict[str, str] = {'Bucket': stats_bucket, 'Key': f'twbx/{program}/{program}{WORKBOOK_SUFFIX}',
                              'Body': workbook,
                              'Metadata': metadata}
    s3.put_object(**params)


# noinspection PyShadowingNames
def _make_workbook_from_template(template: bytes, is_universal_template: bool, description: str, usage_data: str):
    """
    Updates a template with the current data for some program. If the template is a generic one, also updates
    the title in the template.
    :param template: A Tableau workbook template that needs current data.
    :param is_universal_template: If True, this is a generic template usable by different programs.
    :param description: A description that can be applied to the template, if it is a universal template.
    :param usage_data: The current usage data for the program. This is .csv data in a single string.
    :return: The updated template.
    """
    in_file_like = io.BytesIO(template)
    in_zip = zipfile.ZipFile(in_file_like)

    # Copy the template as the workbook. Replace template elements with real ones.
    out_buffer = io.BytesIO()
    with zipfile.ZipFile(out_buffer, "a", compression=zipfile.ZIP_DEFLATED, allowZip64=False) as out_zip:
        for zfile in in_zip.filelist:
            zfile_path = Path(zfile.filename)
            with in_zip.open(zfile.filename) as zipped_file:
                if zfile_path.suffix.lower() == WORKBOOK_XML_SUFFIX and is_universal_template:
                    # Replace the generic title with the program's description
                    out_data = zipped_file.read().replace(b'TITLE_PLACEHOLDER_TEXT',
                                                          bytes(description.encode('utf-8')))
                elif zfile_path.name.lower() == WORKBOOK_DATA_FILENAME:
                    # Replace the placeholder file with current data from stats database
                    out_data = usage_data  # usage_data.replace('\x0a', '\x0d\x0a')
                else:
                    out_data = zipped_file.read()
                out_zip.writestr(zfile.filename, out_data)

    return out_buffer.getvalue()


def do_refresh(params, claims) -> Dict[str, str]:
    """
    refreshs the Tableau workbook for the given program. Uses a template workbook, and adds the program-
    specific current usage data.
    :param params: Parameters of the query. Must contain 'program'.
    :param claims: Cognito claims. Must contain email.
    :return a dict of status.
    """
    program = params.get('program') if 'program' in params else params.get('project')
    if not program:
        return {'status': STATUS_MISSING_PARAMETER, 'output': ['Must specify program.']}
    program = cannonical_acm_program_name(program)
    if not authorizer.is_authorized(claims, 'refresh-twbx', program):
        return {'status': STATUS_ACCESS_DENIED, 'output': ['Access denied']}

    metadata = {'submitter-email': claims.get('email'),
                'submitter-comment': params.get('comment', 'No comment provided'),
                'submission-date': datetime.datetime.now().isoformat()}

    template, is_universal = get_template(program)
    info = _get_program_info(program)
    description = info.get('description', 'Talking Book Program')
    usage_data = _get_twbx_data(program)
    twbx = _make_workbook_from_template(template, is_universal, description, usage_data)
    _put_workbook(program, twbx, metadata)

    return {'status': STATUS_OK}


def _getlinks_all(program: str):
    """
    Gets signed links to the workbook and any preview images.
    @param program: Program for which links are desired.
    @return: A dict with {'workbook': url, 'preview': [url, url,...], 'status': 'ok'
    """
    twbx_name = object_key(program, 'workbook').lower()
    prefix = f'twbx/{program}/'
    one_week = 7 * 24 * 3600
    workbook_url = None
    preview_urls = []
    for obj in _list_objects(prefix=prefix):
        params = {'Bucket': stats_bucket, 'Key': obj.get('Key')}
        fn = params['Key'].lower()
        if fn == twbx_name:
            workbook_url = s3.generate_presigned_url('get_object', Params=params, ExpiresIn=one_week)
        elif Path(fn).suffix in PREVIEW_EXTENSIONS:
            preview_urls.append(s3.generate_presigned_url('get_object', Params=params, ExpiresIn=one_week))
    return {'workbook': workbook_url, 'preview': preview_urls, 'status': 'ok'}


# Returns a signed link to download a version of the program spec.
def do_getlinks(params, claims):
    program = params.get('program') if 'program' in params else params.get('project')
    if not program:
        return {'status': STATUS_MISSING_PARAMETER, 'output': ['Must specify program.']}
    program = cannonical_acm_program_name(program)
    if not authorizer.is_authorized(claims, 'get-link', program):
        return {'status': STATUS_ACCESS_DENIED, 'output': ['Access denied']}

    if 'all' in params:
        return _getlinks_all(program)

    try:
        params, _ = get_s3_params_and_obj_info(program, 'workbook')
        workbook_url = s3.generate_presigned_url('get_object', Params=params, ExpiresIn=3600)
    except:
        workbook_url = None
    try:
        params, _ = get_s3_params_and_obj_info(program, 'preview')
        preview_url = s3.generate_presigned_url('get_object', Params=params, ExpiresIn=3600)
    except:
        preview_url = None
    return {'workbook': workbook_url, 'preview': preview_url, 'status': 'ok'}


# Submit a new template.
def do_upload(data, params, claims):  # program, metadata):
    program = params.get('program') if 'program' in params else params.get('project')
    if not program:
        return {'status': STATUS_MISSING_PARAMETER, 'output': ['Must specify program.']}
    program = cannonical_acm_program_name(program)
    if not authorizer.is_authorized(claims, 'upload-twbx-template', program):
        return {'status': STATUS_ACCESS_DENIED, 'output': ['Access denied']}
    filename = params.get('filename')
    ext = Path(filename).suffix.lower()

    if ext in PREVIEW_EXTENSIONS:
        key = object_key(program, flavor='preview', filename=filename)
    elif ext == WORKBOOK_SUFFIX:
        key = object_key(program, flavor='template')
    else:
        return {'status': STATUS_BAD_FILE_TYPE,
                'message': f"Must be one of {WORKBOOK_SUFFIX}, {', '.join(PREVIEW_EXTENSIONS)}.",
                'filetype': ext, 'filename': filename}

    metadata = {'submitter-email': claims.get('email'),
                'submitter-comment': params.get('comment', 'No comment provided'),
                'submission-date': datetime.datetime.now().isoformat()}

    put_result = s3.put_object(Body=data, Bucket=stats_bucket, Metadata=metadata, Key=key)
    print('Put {} with result {}'.format(key, put_result))
    result = {'status': STATUS_OK, 'ETag': put_result.get('ETag')}

    return result


def do_remove_previews(params, claims):
    program = params.get('program') if 'program' in params else params.get('project')
    if not program:
        return {'status': STATUS_MISSING_PARAMETER, 'output': ['Must specify program.']}
    program = cannonical_acm_program_name(program)
    if not authorizer.is_authorized(claims, 'remove-preview', program):
        return {'status': STATUS_ACCESS_DENIED, 'output': ['Access denied']}

    prefix = f'twbx/{program}/'
    num_deleted = _delete_objects(prefix=prefix, delete=lambda obj: not obj.get('Key').lower().endswith('.twbx'))

    result = {'status': STATUS_OK, 'numDeleted': num_deleted}
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
        if action == 'upload':
            result = do_upload(data, query_string_params, claims)

        elif action == 'getlinks':
            result = do_getlinks(query_string_params, claims)

        elif action == 'removePreviews':
            result = do_remove_previews(query_string_params, claims)

        elif action == 'refresh':
            result = do_refresh(query_string_params, claims)

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
                            # 'keys': keys,
                            'result': result,
                            # 'claims': claims,
                            # 'action': action,
                            # 'path': path,
                            # 'path_parameters': path_parameters,
                            # 'query_string_params': query_string_params,
                            # 'multi_value_query_string_parameters': multi_value_query_string_parameters,
                            'msec': (end - start) / 1000000})
    }


if __name__ == '__main__':
    # Test code, runs on a developer desktop.
    # noinspection PyUnusedLocal
    def __test__():
        from os.path import expanduser
        from pathlib import Path

        _PROGRAM = 'LBG-COVID19'

        def test_upload(fn, file, comment='No commment provided.'):
            print('\nupload {}:'.format(fn))
            bytes_read = open(expanduser(fn), "rb").read()
            body_data = binascii.b2a_base64(bytes_read)

            upload_event = {'requestContext': {'authorizer': {'claims': claims}},
                            'pathParameters': {'proxy': 'upload'}, 'queryStringParameters': {'program': _PROGRAM,
                                                                                             'file': file,
                                                                                             'comment': comment},
                            'body': body_data}
            result = lambda_handler(upload_event, {})
            upload_result = json.loads(result['body']).get('result', {})
            print(upload_result)
            print('upload ' + upload_result['status'])
            return upload_result.get('PendingId')

        def test_get(program: str = _PROGRAM, all: bool = False):
            print('\nGet\n')
            event = {'requestContext': {'authorizer': {'claims': claims}},
                     'pathParameters': {'proxy': 'getlinks'},
                     'queryStringParameters': {'program': program}
                     }
            if all:
                event['queryStringParameters']['all'] = '1'
            result = lambda_handler(event, {})
            return result

        def test_remove_previews(program: str = 'TEST'):
            print('\nRemove\n')
            event = {'requestContext': {'authorizer': {'claims': claims}},
                     'pathParameters': {'proxy': 'removePreviews'},
                     'queryStringParameters': {'program': program}
                     }
            result = lambda_handler(event, {})
            return result

        def test_refresh(program: str = 'DEMO'):
            print('\nRefresh\n')
            event = {'requestContext': {'authorizer': {'claims': claims}},
                     'pathParameters': {'proxy': 'refresh'},
                     'queryStringParameters': {'program': program}
                     }
            result = lambda_handler(event, {})
            return result

        claims = {'email': 'test@example.org'}
        claims_path = Path('claims.json')
        if claims_path.exists():
            with open(claims_path) as claims_file:
                claims = json.loads(claims_file.read())
            print('exists')
        print('Just testing')

        pending_id = test_upload('~/workspace/utilities/twbxHelper/DEMO_stats.twbx',
                                 file='workbook',
                                 comment='First workbook.')

        pending_id = test_upload('~/workspace/utilities/twbxHelper/DEMO_stats.png',
                                 file='preview',
                                 comment='First preview.')

        get_result = test_get()
        result = json.loads(get_result['body'])['result']
        print('{}\n{}'.format(result['workbook'], result['preview']))
        get_result = test_get()
        result = json.loads(get_result['body'])['result']
        print('{}\n{}'.format(result['workbook'], result['preview']))

        get_result = test_get(program='VSO-TALK')
        result = json.loads(get_result['body'])['result']
        print('{}\n{}'.format(result['workbook'], result['preview']))

        get_result = test_get(program='VSO-TALK', all=True)
        result = json.loads(get_result['body'])['result']
        print('{}\n{}'.format(result['workbook'], result['preview']))

        refresh_result = test_refresh(program='DEMO')
        result = json.loads(get_result['body'])['result']
        print('{}\n'.format(result))

        remove_result = test_remove_previews()
        result = json.loads(remove_result['body'])['result']
        print('{} deleted'.format(result['numDeleted']))


    __test__()
