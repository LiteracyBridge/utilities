import binascii
import json
import os
import time
from io import BytesIO
from os.path import expanduser

import boto3
from programspec import errors, exporter, spreadsheet, programspec, specdiff

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
review          project
                 - returns the changes between 'current' and 'pending' versions of the .xlsx
list            project
                 - returns a list of program spec versions
diff            project, versionId 1, versionId 2
                 - returns the changes between versionId 1 and versionId 2. 

"""

PROGSPEC_KEY = 'program_spec.xlsx'
DEPLOYMENT_SPEC_KEY = 'deployment_spec.csv'
TALKINGBOOK_MAP_KEY = 'talkingbook_map.csv'
RECIPIENTS_MAP_KEY = 'recipients_map.csv'
RECIPIENTS_KEY = 'recipients.csv'
CONTENT_JSON_KEY = 'content.json'
CONTENT_CSV_KEY = 'content.csv'

s3 = boto3.client('s3')

bucket = 'amplio-progspecs'

def project_key(project: str, obj: str = ''):
    return '{}/{}'.format(project, obj)


def cannonical_acm_project_name(acmdir):
    if acmdir is None:
        return None
    _, acm = os.path.split(acmdir)
    acm = acm.upper()
    if acm.startswith('ACM-'):
        acm = acm[4:]
    return acm


def _get_errors(from_mark=None):
    result = []
    previous_severity = -1
    for error in errors.get_errors(mark=from_mark):
        if error[0] != previous_severity:
            previous_severity = error[0]
            result.append('{}:'.format(errors.severity[error[0]]))
        result.append('  {}: {}'.format(error[1], error[2]))
    return result


def _get_versions(Bucket=bucket, Prefix=''):
    paginator = s3.get_paginator("list_object_versions")
    kwargs = {'Bucket': Bucket, 'Prefix':Prefix}
    for versions in paginator.paginate(**kwargs):
        for version in versions.get('Versions', []):
            yield version

def _delete_older_versions(version_to_keep: str, prefix: str):
    found_date = None
    # We're allowed to delete up to 1000 objects, a page worth. So accumulate and delete keys per page.
    to_delete = []
    for version in _get_versions(Prefix=prefix):
        if found_date and not version.get('IsLatest') and version.get('LastModified') < found_date:
            to_delete.append({'Key': prefix, 'VersionId': version.get('VersionId')})
        elif version.get('VersionId') == version_to_keep:
            found_date = version.get('LastModified')
        if len(to_delete) == 1000:
            s3.delete_objects(Delete={'Objects': to_delete}, Bucket=bucket)
            to_delete.clear()

    if len(to_delete) > 0:
        s3.delete_objects(Delete={'Objects': to_delete}, Bucket=bucket)


def _upload_parts(data, acm_name, progspec: programspec, metadata):
    project = cannonical_acm_project_name(acm_name)
    expiration = 60

    # Write id to S3
    objdata = exporter.get_content_csv(progspec).getvalue().encode('utf-8-sig')
    key = project_key(project, CONTENT_CSV_KEY)
    put_result = s3.put_object(Body=objdata, Bucket=bucket, Metadata=metadata, Key=key)
    _delete_older_versions(put_result.get('VersionId'), key)

    objdata = exporter.get_content_json(progspec).encode('utf-8')
    key = project_key(project, CONTENT_JSON_KEY)
    put_result = s3.put_object(Body=objdata, Bucket=bucket, Metadata=metadata, Key=key)
    _delete_older_versions(put_result.get('VersionId'), key)

    objdata = exporter.get_recipients_csv(progspec).getvalue().encode('utf-8')
    key = project_key(project, RECIPIENTS_KEY)
    put_result = s3.put_object(Body=objdata, Bucket=bucket, Metadata=metadata, Key=key)
    _delete_older_versions(put_result.get('VersionId'), key)

    objdata = exporter.get_recipients_map_csv(progspec).getvalue().encode('utf-8')
    if len(objdata) > 0:
        key = project_key(project, RECIPIENTS_MAP_KEY)
        put_result = s3.put_object(Body=objdata, Bucket=bucket, Metadata=metadata, Key=key)
        _delete_older_versions(put_result.get('VersionId'), key)

    objdata = exporter.get_talkingbook_map_csv(progspec).getvalue().encode('utf-8')
    if len(objdata) > 0:
        key = project_key(project, TALKINGBOOK_MAP_KEY)
        put_result = s3.put_object(Body=objdata, Bucket=bucket, Metadata=metadata, Key=key)
        _delete_older_versions(put_result.get('VersionId'), key)

    objdata = exporter.get_deployments_csv(progspec).getvalue().encode('utf-8')
    key = project_key(project, DEPLOYMENT_SPEC_KEY)
    put_result = s3.put_object(Body=objdata, Bucket=bucket, Metadata=metadata, Key=key)
    _delete_older_versions(put_result.get('VersionId'), key)

    key = project_key(project, PROGSPEC_KEY)
    put_result = s3.put_object(Body=data, Bucket=bucket, Metadata=metadata, Key=key)
    # _delete_older_versions(put_result.get('VersionId'), key)

    # Read id from S3
    data = s3.get_object(Bucket=bucket, Key=project_key(project, CONTENT_JSON_KEY))
    objid = data.get('Body').read().decode('utf-8')
    print("Id:" + objid)

    params = {'Bucket': bucket, 'Key': project_key(project, CONTENT_JSON_KEY)}
    signed_url = s3.generate_presigned_url('get_object', Params=params, ExpiresIn=expiration)
    print(signed_url)


def _upload(data, project, metadata):
    file = BytesIO(data)
    ss_data = spreadsheet.load(file)
    if not errors.has_error():
        progspec = programspec.get_program_spec_from_spreadsheet(ss_data, cannonical_acm_project_name(project))
        _upload_parts(data, project, progspec, metadata)


def _diff2(project, data1, data2):
    result = []
    file1 = BytesIO(data1)
    ss_data1 = spreadsheet.load(file1)
    progspec1 = None
    if not errors.has_error():
        progspec1 = programspec.get_program_spec_from_spreadsheet(ss_data1, project)
    result.append(_get_errors())
    errors.reset()

    file2 = BytesIO(data2)
    ss_data2 = spreadsheet.load(file2)
    progspec2 = None
    if not errors.has_error():
        progspec2 = programspec.get_program_spec_from_spreadsheet(ss_data2, project)
    result.append(_get_errors())

    if not progspec1 or not progspec2:
        result.append("Error(s) loading program specification(s), can't continue")
        return result

    differ = specdiff.SpecDiff(progspec1, progspec2)
    diffs = differ.diff()
    return result.append(diffs)



def _diff(data, acm_name, params):
    project = cannonical_acm_project_name(acm_name)
    file1 = BytesIO(data)
    ss_data1 = spreadsheet.load(file1)
    if not errors.has_error():
        progspec1 = programspec.get_program_spec_from_spreadsheet(ss_data1, project)
        obj2 = s3.get_object(Bucket=bucket, Key=project_key(project, PROGSPEC_KEY))
        data2 = obj2.get('Body').read()
        file2 = BytesIO(data2)
        ss_data2 = spreadsheet.load(file2)
        progspec2 = programspec.get_program_spec_from_spreadsheet(ss_data2, project)

        differ = specdiff.SpecDiff(progspec1, progspec2)
        diffs = differ.diff()
        return diffs


def _list(acm_name, params):
    project = cannonical_acm_project_name(acm_name)
    xls_key = project_key(project, PROGSPEC_KEY)
    result = []
    for version in _get_versions(Prefix=project_key(project)):
        details = {'Key': version.get('Key'), 'VersionId': version.get('VersionId'),
                       'LastModified': str(version.get('LastModified'))}
        if details['Key'] == xls_key and version.get('IsLatest'):
            metadata = s3.head_object(Bucket=bucket, Key=details['Key'], VersionId=details['VersionId'])
            details['Metadata'] = metadata.get('Metadata')
        result.append(details)
    return result


def _validate(data, acm_name):
    file = BytesIO(data)
    ss_data = spreadsheet.load(file)
    progspec = None
    # Get the program spec. Even if we don't want the spec, we want the side effect of finding any duplicates, etc.
    if not errors.has_error():
        progspec = programspec.get_program_spec_from_spreadsheet(ss_data, cannonical_acm_project_name(acm_name))

    return _get_errors(), progspec


def lambda_handler(event, context):
    start = time.time_ns()
    errors.reset()

    keys = [x for x in event.keys()]
    info = {'keys': keys}
    #         'resource': event.get('resource', '-no resource'),
    #         'path': event.get('path', '-no path'),
    #         'httpMethod': event.get("httpMethod", '-no httpMethod')}
    for key in keys:
        if key != 'body':
            info[key] = event.get(key, '-no ' + key)
    parts = event.get('pathParameters').split('/')
    function = parts[2] if len(parts) > 2 else 'validate'
    query_params = {'path': event.get('pathParameters'), 'query': event.get('queryStringParameters'),
              'vars': event.get('stageVariables')}

    # print('Lambda function invoked with {}: {}'.format(type(event), len(event.get('body'))))

    queryStringParams = event.get('queryStringParameters', {})
    project = queryStringParams.get('project', '')
    comment = queryStringParams.get('comment', None)

    data = binascii.a2b_base64(event.get('body')) if 'body' in event else None
    claims = event.get('requestContext', {}).get('authorizer', {}).get('claims', {})
    params = {'uploader_email': claims.get('email', None)}
    if function == 'validate':
        issues, _ = _validate(data, project)
    elif function == 'diff':
        issues = _diff(data, project, params)
    elif function == 'upload':
        if comment:
            params['uploader_comment'] = comment
        issues = _upload(data, project, params)
    elif function == 'list':
        issues = _list(project, params)

    end = time.time_ns()
    return {
        'statusCode': 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        'body': json.dumps({'msg': 'Hello from Lambda!',
                            'keys': keys,
                            'info': info,
                            'issues': issues,
                            'params': params,
                            'query_params': query_params,
                            'msec': (end - start) / 1000000})
    }


if __name__ == '__main__':
    print('Just testing')
    bytes_read = open("../TEST-ProgramSpecification.xlsx", "rb").read()
    print('Got {} bytes'.format(len(bytes_read)))
    _validate(bytes_read, 'TEST')

    body = binascii.b2a_base64(bytes_read)
    event = {'requestContext': {'authorizer': {'claims': {'edit': '.*', 'view': '.*', 'email': 'bill@amplio.org'}}},
             'pathParameters': '/data/upload', 'queryStringParameters': {'project': 'TEST',
                                                                         'comment': 'Revised content calendar for deployment #3.'},
             'body': body}
    lambda_handler(event, {})

    bytes_read = open(expanduser("~/Dropbox/ACM-TEST/programspec/TEST-ProgramSpecification-updated.xlsx"), "rb").read()
    body = binascii.b2a_base64(bytes_read)
    event = {'requestContext': {'authorizer': {'claims': {'edit': '.*', 'view': '.*', 'email': 'bill@amplio.org'}}},
             'pathParameters': '/data/diff', 'queryStringParameters': {'project': 'TEST',
                                                                       'comment': 'Revised content calendar for deployment #3.'},
             'body': body}
    lambda_handler(event, {})

    event = {'requestContext': {'authorizer': {'claims': {'edit': '.*', 'view': '.*', 'email': 'bill@amplio.org'}}},
             'pathParameters': '/data/list', 'queryStringParameters': {'project': 'TEST',
                                                                       'comment': 'Revised content calendar for deployment #3.'}
             }
    result = lambda_handler(event, {})
    print(result)
