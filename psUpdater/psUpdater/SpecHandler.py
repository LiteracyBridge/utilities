import datetime
from typing import Optional, List

import boto3
from amplio.utils.AmplioLambda import *

from sqlalchemy.engine import Engine
import Spec
import SpecCompare
from Spec import Program
from ImportProcessor import ImportProcessor
from XlsExporter import Exporter

print("Importing db, then XlsImporter")
import db
import XlsImporter
import XlsExporter

print('SpecHandler loading.')

STATUS_OK = 'ok'
STATUS_FAILURE = 'failure'

OK_RESPONSE = 200
FAILURE_RESPONSE_400 = 400

PENDING_PROGSPEC_KEY = 'pending_progspec.xlsx'
PUBLISHED_PROGSPEC_KEY = 'pub_progspec.xlsx'
UNPUBLISHED_PROGSPEC_KEY = 'unpub_progspec.xlsx'
PROGSPEC_BUCKET = 'amplio-progspecs'

GET_METADATA_FOR = [PENDING_PROGSPEC_KEY, PUBLISHED_PROGSPEC_KEY]

PUBLISHED_ARTIFACT_NAME = 'published'
UNPUBLISHED_ARTIFACT_NAME = 'unpublished'
PENDING_ARTIFACT_NAME = 'pending'
artifact_map = {
    PUBLISHED_ARTIFACT_NAME: PUBLISHED_PROGSPEC_KEY,
    UNPUBLISHED_ARTIFACT_NAME: UNPUBLISHED_PROGSPEC_KEY,
    PENDING_ARTIFACT_NAME: PENDING_PROGSPEC_KEY,
    'recipients': 'pub_recipients.csv',
    'content': 'pub_content.csv',
    'deployments': 'pub_deployments.csv'
}
BINARY_ARTIFACTS = [PUBLISHED_ARTIFACT_NAME, UNPUBLISHED_ARTIFACT_NAME, PENDING_ARTIFACT_NAME]

s3 = boto3.client('s3')


def _program_key(program: str, obj: str = ''):
    return '{}/{}'.format(program, obj)


def _s3_key_for(programid: str, artifact: str) -> Optional[str]:
    if artifact in artifact_map:
        return _program_key(programid, artifact_map[artifact])
    return None


# noinspection PyPep8Naming
def _list_objects(Bucket=PROGSPEC_BUCKET, Prefix=''):
    paginator = s3.get_paginator("list_objects_v2")
    kwargs = {'Bucket': Bucket, 'Prefix': Prefix}
    for objects in paginator.paginate(**kwargs):
        for obj in objects.get('Contents', []):
            yield obj


# List the versions of objects with the given prefix.
# noinspection PyPep8Naming
def _list_versions(Bucket=PROGSPEC_BUCKET, Prefix=''):
    paginator = s3.get_paginator("list_object_versions")
    kwargs = {'Bucket': Bucket, 'Prefix': Prefix}
    for versions in paginator.paginate(**kwargs):
        for version in versions.get('Versions', []):
            yield version


# Deletes versions that match the given prefix. Optional string or iterable list of versions to not delete.
def _delete_versions(prefix: str, versions_to_keep=None):
    if prefix is None or len(prefix) < 2:
        raise ValueError('Must specify a prefix to delete.')
    if versions_to_keep is None:
        versions_to_keep = []
    elif type(versions_to_keep) == str:
        versions_to_keep = [versions_to_keep]
    versions_to_keep = set(versions_to_keep)
    # We can delete up to 1000 objects at a time, so accumulate and delete keys in batches.
    to_delete = []
    for version in _list_versions(Prefix=prefix):
        if not version.get('VersionId') in versions_to_keep:
            to_delete.append({'Key': prefix, 'VersionId': version.get('VersionId')})
        if len(to_delete) == 1000:
            s3.delete_objects(Delete={'Objects': to_delete}, Bucket=PROGSPEC_BUCKET)
            to_delete.clear()

    if len(to_delete) > 0:
        s3.delete_objects(Delete={'Objects': to_delete}, Bucket=PROGSPEC_BUCKET)


def _bool_arg(arg, default=False):
    """
    Given a value that may be True/False or may be a [sub-]string 'true'/'false', return the truth value.
    :param arg: May contain a truth value.
    :param default: If the argument can't be interpreted as a boolean, use this default.
    :return: the True or False
    """
    if type(arg) == bool:
        return arg
    elif arg is None:
        return default
    try:
        val = str(arg).lower()
        if val in ('y', 'yes', 't', 'true', 'on', '1'):
            return 1
        elif val in ('n', 'no', 'f', 'false', 'off', '0'):
            return 0
        else:
            return default
    except ValueError:
        return default


def _get_s3_params_and_obj_info(key: str) -> (object, object):
    params = {'Bucket': PROGSPEC_BUCKET, 'Key': key}
    head = s3.head_object(**params)
    obj_info = {'Metadata': head.get('Metadata'),
                'VersionId': head.get('VersionId'),
                'Size': head.get('ContentLength'),
                'LastModified': head.get('LastModified').isoformat(),
                'Key': params.get('Key')}
    return params, obj_info


def _open_program_spec_from_s3(programid: str, name: str = None, key: str = None) -> Tuple[
    bool, List[str], XlsImporter.Importer]:
    if (name is None) == (key is None):
        raise Exception('Exactly one of "key" and "name" must be provided')
    importer: XlsImporter.Importer = XlsImporter.Importer(program_id=programid)
    key = key if key is not None else _program_key(programid, name)
    obj = s3.get_object(Bucket=PROGSPEC_BUCKET, Key=key)
    obj_data = obj.get('Body').read()
    ok, errors = importer.do_open(data=obj_data)
    return ok, errors, importer


# noinspection PyUnusedLocal
def echo_handler(event, context):
    """
    Echos the parameters back. For testing.
    :param event: The Lambda event.
    :param context: The Lambda context.
    :return: Various extracts from the event.
    """
    keys = [x for x in event.keys()]

    path = event.get('path', {})
    path_parameters = event.get('pathParameters', {})
    multi_value_query_string_parameters = event.get('multiValueQueryStringParameters', {})
    query_string_params = event.get('queryStringParameters', {})

    bodyless = {k: v for k, v in event.items() if k != 'body'}
    if 'body' in event:
        bodyless['body'] = '...'
    if 'Authorization' in bodyless.get('headers', {}):
        bodyless['headers'] = {k: (v if k != 'Authorization' else '...') for k, v in bodyless['headers'].items()}
    if 'Authorization' in bodyless.get('multiValueHeaders', {}):
        bodyless['multiValueHeaders'] = {k: (v if k != 'Authorization' else '...') for k, v in
                                         bodyless['multiValueHeaders'].items()}

    print(f'event: {bodyless}\n')
    print(f'context: {context}')
    print(f'keys: {keys}')
    print(f'path: {path}')
    print(f'pathParameters: {path_parameters}')
    print(f'queryStringParameters: {query_string_params}')
    print(f'multiValueQueryStringParameters: {multi_value_query_string_parameters}')

    data = None
    body = event.get('body')
    body_len = 0
    data_len = 0
    if body is None:
        print('Body is None')
    else:
        body_len = len(body)
        print(f'Body is {body_len} characters long')
    if body:
        try:
            data = binascii.a2b_base64(body)
            data_len = len(data)
            print(f'Body decodes as {data_len} bytes')
        except (binascii.Error, binascii.Incomplete):
            data = None

    claims = event.get('requestContext', {}).get('authorizer', {}).get('claims', {})
    print(f'claims: {claims}')

    return {
        'statusCode': 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        'body': json.dumps({
            'claims': claims,
            'keys': keys,
            'path': path,
            'body_len': body_len,
            'data_len': data_len,
            'path_parameters': path_parameters,
            'query_string_params': query_string_params,
            'multi_value_query_string_parameters': multi_value_query_string_parameters,
        })
    }


@NeedsRole('AD,PM')
def get_content(programid: QueryStringParam):
    engine: Engine = db.get_db_engine()
    exporter = XlsExporter.Exporter(programid, engine)
    exporter.read_from_database()
    content = Spec.progspec_to_json(exporter.program_spec)
    return content

@NeedsRole('AD,PM')
def put_content(programid: QueryStringParam, data: JsonBody, return_diff: QueryStringParam = False):
    """
    Update one or more sections of the program spec.
    :param programid: The program whose spec is to be updated.
    :param data: The data.
    :param return_diff: If true, return a diff of the program spec.
    :return: the update status, and the diff if requested.
    """
    print(f'put content for {programid}, data: {data}')
    engine: Engine = db.get_db_engine()
    return_diff: bool = _bool_arg(return_diff)
    new_spec: Program = Spec.progspec_from_json(programid, data)

    diffs = None
    if return_diff:
        # Get existing content.
        exporter: Exporter = XlsExporter.Exporter(programid, engine)
        exporter.read_from_database()

        differ: SpecCompare = SpecCompare.SpecCompare(exporter.program_spec, new_spec)
        diffs = differ.diff(content_only=True)
        print(diffs)

    importer: ImportProcessor = ImportProcessor(programid, new_spec)
    with db.get_db_connection(engine=engine) as conn:
        importer.update_db_program_spec(conn, content_only=True)
        result = {'status': 'ok'}
        if return_diff:
            result['updates'] = diffs
        return result

# @handler(roles='AD,PM')
@NeedsRole('AD,PM')
def upload_handler(data: BinBody, programid: QueryStringParam, email: Claim, return_diff: QueryStringParam = False,
                   comment: QueryStringParam = 'No comment provided'):
    return_diff = _bool_arg(return_diff)
    print(
        f'Upload {len(data)} bytes in program {programid} for {email}. Return diff: {return_diff}, comment: {comment}')
    # Check that it's a valid program spec.
    importer = XlsImporter.Importer(programid)
    ok, errors = importer.do_open(data=data)
    if ok:
        # Looks good, save to S3.
        metadata = {'submitter-email': email,
                    'submitter-comment': comment,
                    'submission-date': datetime.now().isoformat()}

        key = _program_key(programid, PENDING_PROGSPEC_KEY)
        put_result = s3.put_object(Body=data, Bucket=PROGSPEC_BUCKET, Metadata=metadata, Key=key)
        _delete_versions(key, versions_to_keep=put_result.get('VersionId'))
        result = {'status': STATUS_OK}
        if return_diff:
            diff_result = compare_handler(programid, 'published', 'pending')
            if diff_result.get('status') == STATUS_OK:
                result['diff'] = diff_result.get('diff')
            else:
                result['status'] = diff_result.get('status')
        return result
    else:
        return {'status': STATUS_FAILURE, 'errors': errors}, FAILURE_RESPONSE_400


# @handler()
def compare_handler(programid: QueryStringParam, v1: QueryStringParam, v2: QueryStringParam):
    def get_pending() -> Tuple[bool, List[str], Spec.Program]:
        # Get the pending program spec
        ok, errors, importer = _open_program_spec_from_s3(programid=programid, name=PENDING_PROGSPEC_KEY)
        try:
            ps = importer.program_spec
        except Exception as ex:
            ps = None
        return ok, errors, ps

    def get_published() -> Tuple[bool, List[str], Spec.Program]:
        # Get the published program spec
        ok, errors, importer = _open_program_spec_from_s3(programid=programid, name=PUBLISHED_PROGSPEC_KEY)
        try:
            ps = importer.program_spec
        except Exception as ex:
            ps = None
        return ok, errors, ps

    def get_unpublished() -> Spec.Program:
        # Get the db-staged program spec.
        engine: Engine = db.get_db_engine()
        exporter: XlsExporter.Exporter = XlsExporter.Exporter(program_id=programid, engine=engine)
        unpublished_spec: Spec.Program = exporter.read_from_database()
        return unpublished_spec

    if v1 == 'published':
        ok1, errors1, ps1 = get_published()
    elif v1 == 'unpublished':
        ps1 = get_unpublished()
        ok1 = True
    else:
        ok1, errors1, ps1 = get_pending()

    if v2 == 'published':
        ok2, errors2, ps2 = get_published()
    elif v2 == 'unpublished':
        ps2 = get_unpublished()
        ok2 = True
    else:
        ok2, errors2, ps2 = get_pending()

    comparator = SpecCompare.SpecCompare(ps1, ps2)
    diffs = comparator.diff()

    return {'status': 'ok', 'diff': diffs}


# @handler(roles='AD,PM')
@NeedsRole('AD,PM')
def accept_handler(programid: QueryStringParam, email: Claim, comment: QueryStringParam = 'No comment provided',
                   publish: QueryStringParam = False):
    original_publish = publish
    publish = _bool_arg(publish)
    print(
        f'Accept pending program spec for program {programid} by {email}. Publish: {publish} (orig: {original_publish}).')
    pending_key = _program_key(programid, PENDING_PROGSPEC_KEY)
    ok, errors, importer = _open_program_spec_from_s3(programid=programid, key=pending_key)
    engine: Engine = db.get_db_engine()
    importer.update_database(engine=engine, commit=True)

    result = {'status': STATUS_OK}
    if publish:
        publish_result, publish_rc = publish_handler(programid, email, comment)
        if publish_result.get('status') != STATUS_OK:
            result['status'] = STATUS_FAILURE
            result['errors'] = publish_result.get('errors')

    if result.get('status') == STATUS_OK:
        _delete_versions(pending_key)
    return result


# @handler(roles='AD,PM')
@NeedsRole('AD,PM')
def publish_handler(programid: QueryStringParam, email: Claim, comment: QueryStringParam = 'No comment provided'):
    """
    Publish the program specification from the database. Creates pub_general.csv, pub_deployments.csv,
    pub_content.csv, and pub_recipients.csv objects in S3.
    :param programid: The program for which to publish the program specification.
    :param comment: A comment about the publish operation. May be automatically generated by publishing application.
    :param email: The email address of the user initiating the publish operation.
    :return: Any detected errors (These would be access errors; the data in the database is, by definition,
            good data.
    """
    print(f'Publish for program {programid} by {email}')
    comment = comment or 'No comment provided'
    metadata = {'submitter-email': email,
                'submitter-comment': comment,
                'submission-date': datetime.now().isoformat()}
    print(metadata)
    # Get the db-staged program spec.
    engine: Engine = db.get_db_engine()
    exporter: XlsExporter.Exporter = XlsExporter.Exporter(program_id=programid, engine=engine)
    ok, errors = exporter.do_export(bucket=PROGSPEC_BUCKET, metadata=metadata)
    print(f'Publish result: {ok}, {errors}')
    if ok:
        result = {'status': STATUS_OK}
        response_code = OK_RESPONSE
    else:
        result = {'status': STATUS_FAILURE, 'errors': errors}
        response_code = FAILURE_RESPONSE_400
    return result, response_code


# @handler(roles='AD,PM,CO')
@NeedsRole('AD,PM,CO')
def download_handler(programid: QueryStringParam, artifact: QueryStringParam, aslink: QueryStringParam, email: Claim):
    """
    Returns a program spec artifact as the bytes of a file or a link to a file in S3.
    :param programid: The program for which the artifact is desired.
    :param artifact: Which artifact, one of 'unpublished', 'published', 'general', 'deployments', 'content',
                or 'recipients'. 'unpublished' is the unpublished program spec (what one sees in the Amplio
                Suite as a .xlsx file, 'published' is the published .xlsx spreadsheet, the others are the
                corresponding .csv files.
    :param aslink: Boolean. If true, returns a link to the file.
    :param email: The user's email
    :return: {'status': status, 'data': bytes-or-link, 'object': metadata-about-object}
    """
    aslink = _bool_arg(aslink)
    artifact = artifact.lower()
    key = _s3_key_for(programid, artifact)
    # If this is a request for the un-published program specification, we need to extract it from the database first.
    if artifact == UNPUBLISHED_ARTIFACT_NAME:
        metadata = {'submitter-email': email,
                    'submitter-comment': 'Unpublished created for download.',
                    'submission-date': datetime.now().isoformat()}
        engine: Engine = db.get_db_engine()
        exporter: XlsExporter.Exporter = XlsExporter.Exporter(program_id=programid, engine=engine)
        exporter.read_from_database()
        ok, errors = exporter.save_unpublished(bucket=PROGSPEC_BUCKET, metadata=metadata)
    if key:
        params, obj_info = _get_s3_params_and_obj_info(key)
        obj_info['artifact'] = artifact
        if aslink:
            # set the save-as name.
            params['ResponseContentDisposition'] = f'filename="{programid}-{artifact_map[artifact]}"'
            obj_info['filename'] = f'{programid}-{artifact_map[artifact]}'
            signed_url = s3.generate_presigned_url('get_object', Params=params, ExpiresIn=600)
            result = {'status': STATUS_OK, 'url': signed_url, 'object': obj_info}
        else:
            obj = s3.get_object(**params)
            if artifact in BINARY_ARTIFACTS:
                bin_data = obj.get('Body').read()
                data = binascii.b2a_base64(bin_data).decode('ascii')
            else:
                data = obj.get('Body').read().decode('utf-8')

            result = {'status': STATUS_OK, 'data': str(data), 'object': obj_info}
        return result


# @handler(roles='AD,PM,CO,FO')
@NeedsRole('AD,PM,CO,FO')
def list_objects(programid: QueryStringParam):
    """
    Lists the objects in the program's directory in the amplio-progspecs bucket.
    :param programid: Program for which objects are desired.
    :return: A list of the objects and metadata about them.
    """
    print(f'Listing objects for program {programid}')
    prefix = _program_key(programid)
    result = {'status': STATUS_OK, 'objects': {}}
    for obj in _list_objects(Prefix=prefix):
        fn = obj.get('Key')[len(prefix):]
        details = {'Key': obj.get('Key'),
                   'Size': obj.get('Size'),
                   'LastModified': obj.get('LastModified').isoformat(),
                   'ETag': obj.get('ETag')}
        if fn in GET_METADATA_FOR:
            metadata = s3.head_object(Bucket=PROGSPEC_BUCKET, Key=obj.get('Key'))
            details['VersionId'] = metadata.get('VersionId')
            details['Metadata'] = metadata.get('Metadata')
        result['objects'][fn] = details
    return result


# @handler()
def validation_handler(data: BinBody, programid: QueryStringParam, email: Claim):
    print(f'Validate program spec for {programid}, user {email}.')
    # Check that it's a valid program spec.
    importer = XlsImporter.Importer('TEST')  # Only used for save.
    ok, issues = importer.do_open(data=data)
    if len(issues) == 0:
        issues.append('No issues found.')
    return {'status': 'ok', 'output': issues}


PROGSPEC_HANDLERS = {'upload': upload_handler,
                     'compare': compare_handler,
                     'accept': accept_handler,
                     'publish': publish_handler,
                     'download': download_handler,
                     'list': list_objects,
                     'validate': validation_handler,
                     'echo': echo_handler,
                     'get_content': get_content,
                     'put_content': put_content,
                     }


def lambda_router(event, context):
    the_router = LambdaRouter(event, context, handlers=PROGSPEC_HANDLERS)
    action = the_router.pathParam(0)
    return the_router.dispatch(action)


# def lambda_router(event, context, action: str = path_param(0)):
#     if action == 'upload':
#         return upload_handler(event, context)
#     elif action == 'review' or action == 'compare':
#         return compare_handler(event, context)
#     elif action == 'accept':
#         return accept_handler(event, context)
#     elif action == 'publish':
#         return publish_handler(event, context)
#     elif action == 'download':
#         return download_handler(event, context)
#     elif action == 'list':
#         return list_objects(event, context)
#     elif action == 'validate':
#         return validation_handler(event, context)
#     elif action == 'echo':
#         return echo_handler(event, context)
