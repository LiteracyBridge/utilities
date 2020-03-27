import binascii
import datetime
import distutils.util
import json
import os
import time
import traceback
from io import BytesIO

from botocore.exceptions import ClientError
import boto3
from programspec import errors, spreadsheet, programspec, specdiff
from programspec.exporter import Exporter
from programspec.validator import Validator

"""
Program Spec update utility.

Functions to manage the server copy of the program specification.

Program specification is kept in an S3 bucket, with a prefix of the project name. There are several files with that 
prefix:
  program_spec.xlsx         The program spec itself.
  deployment_spec.csv       The projects, their date range, and any filters.
  talkingbook_map.csv       If the project has a Talking Book map, it is here. Only CBCC uses talking book maps,
                            and only for deployments before the program specification was completed.
  recipients.csv            All of the program's recipients.
  recipients_map.csv        Map of recipientid to the directory name in the TB-Loaders/communities directory.
  content.csv               The content calendar, one line per message.
  content.json              The content calendar in a json format.
  
The S3 bucket has versioning turned on. The various .csv files can be derived from the .xlsx, so when new versions
of the .csv files are written, older versions are deleted. Versions are kept of the .xlsx file, so that the history
of the program spec is available.

Functions available:

validate        program_spec data (as base-64 encoded body)
                 - validates the given program spec. Returns a list of issues if any are found.
submit          project, comment, program_spec data
                 - if the given program spec passes validation with no issues, it is saved as the
                   pending program spec. Any previous versions of the pending program spec are deleted.
                 - user must have 'submit program spec' authority.
approve         project, comment, versionId of pending .xlsx, versionId of current
                 - if the version ids match, copies the pending .xlsx as the current .xlsx, and deletes
                   the current.
                 - user must have 'approve program spec' authority. 
list            project
                 - returns a list of program spec versions
diff            project, versionId 1, versionId 2
                 - returns the changes between versionId 1 and versionId 2. 

"""

CURRENT_PROGSPEC_KEY = 'program_spec.xlsx'
PENDING_PROGSPEC_KEY = 'pending_spec.xlsx'
DEPLOYMENT_SPEC_KEY = 'deployment_spec.csv'
TALKINGBOOK_MAP_KEY = 'talkingbook_map.csv'
RECIPIENTS_MAP_KEY = 'recipients_map.csv'
RECIPIENTS_KEY = 'recipients.csv'
CONTENT_JSON_KEY = 'content.json'
CONTENT_CSV_KEY = 'content.csv'

PARTS_FILES = [DEPLOYMENT_SPEC_KEY, TALKINGBOOK_MAP_KEY, RECIPIENTS_MAP_KEY,
                     RECIPIENTS_KEY, CONTENT_JSON_KEY, CONTENT_CSV_KEY]
FILE_ENCODINGS = {DEPLOYMENT_SPEC_KEY:'utf-8',
                  TALKINGBOOK_MAP_KEY:'utf-8',
                  RECIPIENTS_MAP_KEY:'utf-8',
                  RECIPIENTS_KEY:'utf-8',
                  CONTENT_JSON_KEY:'utf-8',
                  CONTENT_CSV_KEY: 'utf-8-sig'}


LIST_VERSION_KEYS = [CURRENT_PROGSPEC_KEY, PENDING_PROGSPEC_KEY]

STATUS_OK = 'ok'
STATUS_FAILURE = 'failure'
STATUS_EXTRA_PARAMETER = 'Extraneous parameter'
STATUS_ACCESS_DENIED = 'Access denied'
STATUS_MISSING_PARAMETER = 'Missing parameter'

s3 = boto3.client('s3')

bucket = 'amplio-progspecs'


class Authorizer:
    # noinspection PyUnusedLocal
    @staticmethod
    def is_authorized(claims, action, project: str = None):
        return True


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


def project_key(project: str, obj: str = ''):
    return '{}/{}'.format(project, obj)


# Given a project or ACM name, return just the project name part, uppercased. ACM-TEST -> TEST, test -> TEST
def cannonical_acm_project_name(acmdir):
    if acmdir is None:
        return None
    _, acm = os.path.split(acmdir)
    acm = acm.upper()
    if acm.startswith('ACM-'):
        acm = acm[4:]
    return acm


# Retrieve any errors (& warnings, info, ...) with optional starting point. Format into a list of
# Severity-Name
#   issue ...
def _get_errors(from_mark=None, min_severity=errors.ISSUE):
    result = []
    previous_severity = -1
    for error in errors.get_errors(mark=from_mark):
        if error[0] > min_severity: # A higher numerical value is a lower severity
            continue
        if error[0] != previous_severity:
            previous_severity = error[0]
            result.append('{}:'.format(errors.severity[error[0]]))
        result.append('  {}: {}'.format(error[1], error[2]))
    return result


# Tries to load the given data as a program spec. Returns any resulting spec, and a boolean 'error free'.
def _load_and_validate(data, project: str = None, **kwargs):
    ps = None
    file = BytesIO(data)
    ss = spreadsheet.load(file)
    # If the spreadsheet is structually OK, validate the program spec.
    if ss.ok:
        ps = programspec.get_program_spec_from_spreadsheet(ss, cannonical_acm_project_name(project))
        validator = Validator(ps, **kwargs)
        validator.validate()

    # Note that simply getting a programspec does not mean there are no errors.
    return ps, not errors.has_error()


# List the versions of objects with the given prefix.
# noinspection PyPep8Naming
def _list_versions(Bucket=bucket, Prefix=''):
    paginator = s3.get_paginator("list_object_versions")
    kwargs = {'Bucket': Bucket, 'Prefix': Prefix}
    for versions in paginator.paginate(**kwargs):
        for version in versions.get('Versions', []):
            yield version


# List the objects with the given prefix.
# noinspection PyPep8Naming
def _list_objects(Bucket=bucket, Prefix=''):
    paginator = s3.get_paginator("list_objects_v2")
    kwargs = {'Bucket': Bucket, 'Prefix': Prefix}
    for objects in paginator.paginate(**kwargs):
        for obj in objects.get('Contents', []):
            yield obj


# Deletes versions that match the given prefix. Optional string or iterable list of versions to not delete.
def _delete_versions(prefix: str, versions_to_keep=None):
    if prefix is None or len(prefix) < 2:
        raise ValueError('Must specify a prefix to delete.')
    if versions_to_keep is None:
        versions_to_keep = []
    elif type(versions_to_keep) == str:
        versions_to_keep = [versions_to_keep]
    versions_to_keep = set(versions_to_keep)
    # We can to delete up to 1000 objects at a time. So accumulate and delete keys per page.
    to_delete = []
    for version in _list_versions(Prefix=prefix):
        if not version.get('VersionId') in versions_to_keep:
            to_delete.append({'Key': prefix, 'VersionId': version.get('VersionId')})
        if len(to_delete) == 1000:
            s3.delete_objects(Delete={'Objects': to_delete}, Bucket=bucket)
            to_delete.clear()

    if len(to_delete) > 0:
        s3.delete_objects(Delete={'Objects': to_delete}, Bucket=bucket)


# Extracts the standalone .csv files from the progspec.
def _extract_parts(project: str, versionid: str):
    # Read data from S3
    key = project_key(project, CURRENT_PROGSPEC_KEY)
    obj = s3.get_object(Bucket=bucket, Key=key, VersionId=versionid)
    data = obj.get('Body').read()
    progspec, ok = _load_and_validate(data, project)
    metadata = {'programspec_versionid': versionid}
    exporter = Exporter(progspec)

    # Write the individual files to S3
    key = project_key(project, CONTENT_CSV_KEY)
    encoding = FILE_ENCODINGS.get(CONTENT_CSV_KEY, 'utf-8')
    objdata = exporter.get_content_csv().getvalue().encode(encoding)
    put_result = s3.put_object(Body=objdata, Bucket=bucket, Metadata=metadata, Key=key)
    _delete_versions(key, versions_to_keep=put_result.get('VersionId'))

    key = project_key(project, CONTENT_JSON_KEY)
    encoding = FILE_ENCODINGS.get(CONTENT_JSON_KEY, 'utf-8')
    objdata = exporter.get_content_json().encode(encoding)
    put_result = s3.put_object(Body=objdata, Bucket=bucket, Metadata=metadata, Key=key)
    _delete_versions(key, versions_to_keep=put_result.get('VersionId'))

    key = project_key(project, RECIPIENTS_KEY)
    encoding = FILE_ENCODINGS.get(RECIPIENTS_KEY, 'utf-8')
    objdata = exporter.get_recipients_csv().getvalue().encode(encoding)
    put_result = s3.put_object(Body=objdata, Bucket=bucket, Metadata=metadata, Key=key)
    _delete_versions(key, versions_to_keep=put_result.get('VersionId'))

    key = project_key(project, RECIPIENTS_MAP_KEY)
    encoding = FILE_ENCODINGS.get(RECIPIENTS_MAP_KEY, 'utf-8')
    objdata = exporter.get_recipients_map_csv().getvalue().encode(encoding)
    # Recipients map may be empty, in which case do not store it.
    if len(objdata) > 0:
        put_result = s3.put_object(Body=objdata, Bucket=bucket, Metadata=metadata, Key=key)
        _delete_versions(key, versions_to_keep=put_result.get('VersionId'))
    else:
        _delete_versions(key)

    key = project_key(project, TALKINGBOOK_MAP_KEY)
    encoding = FILE_ENCODINGS.get(TALKINGBOOK_MAP_KEY, 'utf-8')
    objdata = exporter.get_talkingbook_map_csv().getvalue().encode(encoding)
    # Talkingbook map may be empty...
    if len(objdata) > 0:
        put_result = s3.put_object(Body=objdata, Bucket=bucket, Metadata=metadata, Key=key)
        _delete_versions(key, versions_to_keep=put_result.get('VersionId'))
    else:
        _delete_versions(key)

    key = project_key(project, DEPLOYMENT_SPEC_KEY)
    encoding = FILE_ENCODINGS.get(DEPLOYMENT_SPEC_KEY, 'utf-8')
    objdata = exporter.get_deployments_csv().getvalue().encode(encoding)
    put_result = s3.put_object(Body=objdata, Bucket=bucket, Metadata=metadata, Key=key)
    _delete_versions(key, versions_to_keep=put_result.get('VersionId'))


# Approve the "pending" program spec. Move it to "current" and extract the .csv files.
# The caller must provide the version of the pending spec and of the current spec (or
# 'None' if there is no current spec), which must match the actual pending and current.
def do_approve(params, claims):
    result = {'status': STATUS_OK, 'output': []}
    project = params.get('project')
    if not project:
        return {'status': STATUS_MISSING_PARAMETER, 'output': ['Must specify project.']}
    project = cannonical_acm_project_name(project)
    if not authorizer.is_authorized(claims, 'approve-progspec', project):
        return {'status': STATUS_ACCESS_DENIED, 'output': ['Access denied']}
    # Make sure the versions we have are the versions the user expected.
    current_version = params.get('current')
    current_key = project_key(project, CURRENT_PROGSPEC_KEY)
    try:
        current_metadata = s3.head_object(Bucket=bucket, Key=current_key)
    except ClientError:
        current_metadata = {'VersionId': 'None'}

    pending_version = params.get('pending')
    pending_key = project_key(project, PENDING_PROGSPEC_KEY)
    pending_metadata = s3.head_object(Bucket=bucket, Key=pending_key)

    if current_version != current_metadata.get('VersionId'):
        result['output'].append('Version mismatch on current programspec.')
        result['status'] = STATUS_FAILURE
    if pending_version != pending_metadata.get('VersionId'):
        result['output'].append('Version mismatch on pending programspec.')
        result['status'] = STATUS_FAILURE

    if result['status'] == STATUS_OK:
        metadata = pending_metadata.get('Metadata', {})
        metadata['approver-email'] = claims.get('email')
        metadata['approver-comment'] = params.get('comment', 'No comment provided')
        metadata['approval-date'] = datetime.datetime.now().isoformat()

        copy_result = s3.copy_object(Bucket=bucket,
                                     CopySource={'Bucket': bucket, 'Key': pending_key, 'VersionId': pending_version},
                                     Key=current_key,
                                     Metadata=metadata, MetadataDirective='REPLACE')

        _extract_parts(project, copy_result.get('VersionId'))
        _delete_versions(pending_key)

        result['CurrentId'] = copy_result.get('VersionId')
        result['ETag'] = copy_result.get('CopyObjectResult', {}).get('ETag')
        result['LastModified'] = copy_result.get('CopyObjectResult', {}).get('LastModified').isoformat()

    return result


def get_s3_params_and_obj_info(project: str, version: str) -> (object, object):
    params = {'Bucket': bucket}
    if version == 'current':
        key = project_key(project, CURRENT_PROGSPEC_KEY)
    elif version == 'pending':
        key = project_key(project, PENDING_PROGSPEC_KEY)
    elif version in PARTS_FILES:
        key = project_key(project, version)
    else:
        key = project_key(project, CURRENT_PROGSPEC_KEY)
        params['VersionId'] = version

    params['Key'] = key
    head = s3.head_object(**params)
    obj_info = {'Metadata': head.get('Metadata'),
                'VersionId': head.get('VersionId'),
                'Size': head.get('ContentLength'),
                'LastModified': head.get('LastModified').isoformat(),
                'Key': params.get('Key')}
    return params, obj_info


# Returns a signed link to download a version of the program spec.
def do_getlink(params, claims):
    project = params.get('project')
    if not project:
        return {'status': STATUS_MISSING_PARAMETER, 'output': ['Must specify project.']}
    project = cannonical_acm_project_name(project)
    if not authorizer.is_authorized(claims, 'get-link', project):
        return {'status': STATUS_ACCESS_DENIED, 'output': ['Access denied']}
    version = params.get('version')
    if not version:
        return {'status': STATUS_MISSING_PARAMETER, 'version': ['Must specify version.']}

    params, obj_info = get_s3_params_and_obj_info(project, version)

    signed_url = s3.generate_presigned_url('get_object', Params=params, ExpiresIn=600)
    return {'url': signed_url, 'status': 'ok', 'object': obj_info, 'version': version}


# Returns the data for some version of the program spec.
def do_getfile(params, claims):
    project = params.get('project')
    if not project:
        return {'status': STATUS_MISSING_PARAMETER, 'output': ['Must specify project.']}
    project = cannonical_acm_project_name(project)
    if not authorizer.is_authorized(claims, 'get-file', project):
        return {'status': STATUS_ACCESS_DENIED, 'output': ['Access denied']}
    version = params.get('version')
    if not version:
        return {'status': STATUS_MISSING_PARAMETER, 'version': ['Must specify version.']}

    params, obj_info = get_s3_params_and_obj_info(project, version)

    obj = s3.get_object(**params)
    if version in FILE_ENCODINGS:
        data = obj.get('Body').read().decode(FILE_ENCODINGS[version])
    else:
        bin_data = obj.get('Body').read()
        data = binascii.b2a_base64(bin_data).decode('ascii')
    return {'status': STATUS_OK, 'data': str(data), 'object': obj_info, 'version': version}


# Submit a new program spec. If the new spec passes validation, it Becomes the "pending" spec.
def do_submit(data, params, claims):  # project, metadata):
    project = params.get('project')
    if not project:
        return {'status': STATUS_MISSING_PARAMETER, 'output': ['Must specify project.']}
    project = cannonical_acm_project_name(project)
    if not authorizer.is_authorized(claims, 'submit-progspec', project):
        return {'status': STATUS_ACCESS_DENIED, 'output': ['Access denied']}

    metadata = {'submitter-email': claims.get('email'),
                'submitter-comment': params.get('comment', 'No comment provided'),
                'submission-date': datetime.datetime.now().isoformat()}

    # Validate the program spec.
    kwargs = {'fix_recips': _bool_arg(params.get('fix_recips'))}
    kwargs['save_changes'] = kwargs.get('fix_recips')
    ps, ok = _load_and_validate(data, project, **kwargs)
    result = {'output': _get_errors()}

    if ok:
        # If we (may have) tweaked the program spec (eg, assigned recipientids), get the updated data.
        if kwargs.get('save_changes'):
            xls_data = BytesIO()
            ps.save_changes(xls_data)
            data = xls_data.getvalue()

        key = project_key(project, PENDING_PROGSPEC_KEY)
        put_result = s3.put_object(Body=data, Bucket=bucket, Metadata=metadata, Key=key)
        _delete_versions(key, versions_to_keep=put_result.get('VersionId'))

        result['status'] = STATUS_OK
        result['PendingId'] = put_result.get('VersionId')
        result['ETag'] = put_result.get('ETag')

    else:
        result['status'] = STATUS_FAILURE

    return result


# Does the work of comparing two versions of programspec.
def _diff2(data1, data2, project: str = None, name1='version 1', name2='version 2', **kwargs):
    result = ["Examining '{}' program specification.".format(name1)]

    ps1, ok1 = _load_and_validate(data1, project, **kwargs)
    result.extend(_get_errors())
    errors.reset()

    result.append("Examining '{}' program specification.".format(name2))
    ps2, ok2 = _load_and_validate(data2, project, **kwargs)
    result.extend(_get_errors())

    if not ps1 or not ps2:
        result.append("Error(s) loading program specification(s), can't continue.")
        return result

    result.append("Differences from '{}' to '{}' program specification.".format(name1, name2))
    differ = specdiff.SpecDiff(ps1, ps2)
    diffs = differ.diff()
    if len(diffs) == 0:
        result.append(' -- no differences found.')
    else:
        result.extend(diffs)
    return result


def do_diff(data, params, claims):
    def get_version(v):
        try:
            name = {'name': v}
            if v == 'pending':
                key = project_key(project, PENDING_PROGSPEC_KEY)
                obj = s3.get_object(Bucket=bucket, Key=key)
            elif v == 'current':
                key = project_key(project, CURRENT_PROGSPEC_KEY)
                obj = s3.get_object(Bucket=bucket, Key=key)
            else:
                key = project_key(project, CURRENT_PROGSPEC_KEY)
                obj = s3.get_object(Bucket=bucket, Key=key, VersionId=v)
                name['name'] = 'version ' + v
            obj['Key'] = key
            obj_data = obj.get('Body').read()
            name['VersionId'] = obj.get('VersionId')
            name['LastModified'] = obj.get('LastModified').isoformat()
            name['Metadata'] = obj.get('Metadata')
        except ClientError as ex:
            obj_data = None
            name = {'name': 'not found', 'ex': str(ex)}
        return obj_data, name

    kwargs = {'fix_recips': _bool_arg(params.get('fix_recips'))}
    project = params.get('project')
    if not project:
        return {'status': STATUS_MISSING_PARAMETER, 'output': ['Must specify project.']}
    project = cannonical_acm_project_name(project)
    if not authorizer.is_authorized(claims, 'list-progspec', project):
        return {'status': STATUS_ACCESS_DENIED, 'output': ['Access denied.']}

    v1 = params.get('v1')
    v2 = params.get('v2')
    if not v1 or (not v2 and not data):
        return {'status': STATUS_MISSING_PARAMETER, 'output': ['Must specify two program specs to compare.']}
    if v2 and data:
        return {'status': STATUS_EXTRA_PARAMETER, 'output': ['Must specify two program specs to compare.']}
    result = {'output': []}
    d1, n1 = get_version(v1)
    if v2:
        d2, n2 = get_version(v2)
    else:
        n2 = {'name': 'uploaded file'}
        d2 = data
    result['v1'] = n1
    result['v2'] = n2

    if d1 and d2:
        diff = _diff2(d1, d2, project, name1=n1['name'], name2=n2['name'], **kwargs)
        result['output'] = diff
        result['status'] = STATUS_OK
    else:
        if not d1:
            result['output'].append('Missing spec-1')
        if not d2:
            result['output'].append('Missing spec-2')
        result['status'] = STATUS_FAILURE

    return result


# Obtain a list of the current objects for the project. If requested, return the list of versions of program_spec.xlsx.
def do_list(params, claims):
    project = params.get('project')
    if not project:
        return {'status': STATUS_MISSING_PARAMETER, 'output': ['Must specify project.']}
    project = cannonical_acm_project_name(project)
    if not authorizer.is_authorized(claims, 'list-progspec', project):
        return {'status': STATUS_ACCESS_DENIED, 'output': ['Access denied']}
    prefix = project_key(project)
    xls_key = project_key(project, CURRENT_PROGSPEC_KEY)
    result = {'output': [], 'status': STATUS_OK, 'objects': {}, 'versions': []}

    for obj in _list_objects(Prefix=prefix):
        fn = obj.get('Key')[len(prefix):]
        details = {'Key': obj.get('Key'),
                   'Size': obj.get('Size'),
                   'LastModified': obj.get('LastModified').isoformat(),
                   'ETag': obj.get('ETag')}
        if fn in LIST_VERSION_KEYS:
            metadata = s3.head_object(Bucket=bucket, Key=obj.get('Key'))
            details['VersionId'] = metadata.get('VersionId')
            details['Metadata'] = metadata.get('Metadata')
        result['objects'][fn] = details

    if _bool_arg(params.get('versions')):
        for version in _list_versions(Prefix=xls_key):
            details = {'VersionId': version.get('VersionId'),
                       'LastModified': version.get('LastModified').isoformat(),
                       'ETag': version.get('ETag'),
                       'IsLatest': version.get('IsLatest')}
            result['versions'].append(details)
    # convenience variables
    result['CurrentId'] = result['objects'].get(CURRENT_PROGSPEC_KEY, {}).get('VersionId', 'None')
    result['PendingId'] = result['objects'].get(PENDING_PROGSPEC_KEY, {}).get('VersionId', 'None')
    return result


# Validate a program spec
# noinspection PyUnusedLocal
def do_validate(data, params, claims):
    project = params.get('project')
    kwargs: dict = {'fix_recips': _bool_arg(params.get('fix_recips'))}
    kwargs['save_changes'] = kwargs.get('fix_recips')
    _load_and_validate(data, project, **kwargs)
    return {'status': STATUS_OK,
            'severity': errors.severity.get(errors.get_severity()) or 'No issues',
            'output': _get_errors()}


# noinspection PyUnusedLocal
def lambda_handler(event, context):
    global authorizer
    start = time.time_ns()
    errors.reset()

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
        if action == 'validate':
            result = do_validate(data, query_string_params, claims)

        elif action == 'list':
            result = do_list(query_string_params, claims)

        elif action == 'diff':
            result = do_diff(data, query_string_params, claims)

        elif action == 'submit':
            result = do_submit(data, query_string_params, claims)

        elif action == 'approve':
            result = do_approve(query_string_params, claims)

        elif action == 'getlink':
            result = do_getlink(query_string_params, claims)

        elif action == 'getfile':
            result = do_getfile(query_string_params, claims)

        elif action == 'regenerate':
            pass

        elif action == 'expunge':
            pass

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

        def test_submit(fn, comment='No commment provided.'):
            print('\nSubmit {}:'.format(fn))
            bytes_read = open(expanduser(fn), "rb").read()
            body_data = binascii.b2a_base64(bytes_read)

            submit_event = {'requestContext': {'authorizer': {'claims': claims}},
                            'pathParameters': {'proxy': 'submit'}, 'queryStringParameters': {'project': 'TEST',
                                                                                             'fix_recips': True,
                                                                                             'comment': comment},
                            'body': body_data}
            result = lambda_handler(submit_event, {})
            submit_result = json.loads(result['body']).get('result', {})
            print(submit_result)
            print('Submit ' + submit_result['status'])
            return submit_result.get('PendingId')

        def test_approve(current, pending, comment='No comment provided.'):
            print('\nApprove {} replacing {}:'.format(pending, current))
            approve_event = {'requestContext': {'authorizer': {'claims': claims}},
                             'pathParameters': {'proxy': 'approve'}, 'queryStringParameters': {'project': 'TEST',
                                                                                               'comment': comment,
                                                                                               'current': current,
                                                                                               'pending': pending}}
            result = lambda_handler(approve_event, {})
            approve_result = json.loads(result['body']).get('result', {})
            print(approve_result)
            print('Approve ' + approve_result['status'])
            return approve_result.get('CurrentId')

        def test_diff(v1, v2):
            print('\nDiff {} => {}:'.format(v1, v2))
            diff_event = {'requestContext': {'authorizer': {'claims': claims}},
                          'pathParameters': {'proxy': 'diff'}, 'queryStringParameters': {'project': 'TEST',
                                                                                         'fix_recips': True,
                                                                                         'v1': v1}}
            p2 = Path(expanduser(v2))
            if p2.exists():
                bytes_read = open(p2, "rb").read()
                body = binascii.b2a_base64(bytes_read)
                diff_event['body'] = body
            else:
                diff_event['queryStringParameters']['v2'] = v2
            result = lambda_handler(diff_event, {})
            diff_result = json.loads(result['body']).get('result')
            print('\n'.join(diff_result['output']))
            print({k: v for k, v in diff_result.items() if k != 'output'})

        def test_list():
            print('\nList:')
            event = {'requestContext': {'authorizer': {'claims': claims}},
                     'pathParameters': {'proxy': 'list'}, 'queryStringParameters': {'project': 'TEST'}
                     }
            result = lambda_handler(event, {})
            list_result = json.loads(result['body']).get('result')
            for f, st in list_result['objects'].items():
                print('{}: {}'.format(f, st))
            return list_result

        def test_get(version='current'):
            print('\nGet {}\n'.format(version))
            event = {'requestContext': {'authorizer': {'claims': claims}},
                     'pathParameters': {'proxy': 'getfile'}, 'queryStringParameters': {'project': 'TEST', 'version': version}
                     }
            result = lambda_handler(event, {})
            return result



        claims = {'edit': '.*', 'view': '.*', 'email': 'bill@amplio.org'}
        print('Just testing')

        obj_list = test_list()

        # pending_id = test_submit('~/Dropbox/ACM-TEST/programspec/TEST-ProgramSpecification.xlsx',
        #                          comment='First test program spec.')
        # obj_list = test_list()
        #
        # current_id = test_approve(current=obj_list.get('CurrentId'), pending=pending_id,
        #                           comment='First approved program specification')
        # obj_list = test_list()
        #
        # pending_id = test_submit('~/Dropbox/ACM-TEST/programspec/TEST-ProgramSpecification-updated.xlsx',
        #                          comment='Second submitted program spec.')
        # obj_list = test_list()
        # test_diff('current', 'pending')
        # test_diff('current', '~/Dropbox/ACM-TEST/programspec/TEST-ProgramSpecification.xlsx')
        # test_diff('pending', 'current')
        #
        # current_id = test_approve(current=current_id, pending=pending_id, comment='Second approved program spec.')
        # obj_list = test_list()
        #
        # pending_id = test_submit('~/Dropbox/ACM-TEST/programspec/TEST-ProgramSpecification.xlsx',
        #                          comment='Third submitted program spec.')
        # obj_list = test_list()

        get_result = test_get('content.csv')
        get_result = test_get('recipients.csv')


    __test__()
