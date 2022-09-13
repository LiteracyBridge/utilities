import traceback
from datetime import datetime
from typing import Tuple, List, Dict

import boto3
from botocore.client import BaseClient

from ps import ProgramSpec
from ps.XlsxReaderWriter import write_to_csv, write_to_xlsx, ARTIFACTS as CSV_ARTIFACTS

_s3 = None

_PROGSPEC_BUCKET = 'amplio-progspecs'

_PENDING_PROGSPEC_KEY = 'pending_progspec.xlsx'
_PUBLISHED_PROGSPEC_KEY = 'pub_progspec.xlsx'
_UNPUBLISHED_PROGSPEC_KEY = 'unpub_progspec.xlsx'

PENDING_ARTIFACT = 'pending'
PUBLISHED_ARTIFACT = 'published'
UNPUBLISHED_ARTIFACT = 'unpublished'

_ARTIFACT_KEYS = {
    PENDING_ARTIFACT: _PENDING_PROGSPEC_KEY,
    PUBLISHED_ARTIFACT: _PUBLISHED_PROGSPEC_KEY,
    UNPUBLISHED_ARTIFACT: _UNPUBLISHED_PROGSPEC_KEY
}

_VERSIONED_ARTIFACTS = [PUBLISHED_ARTIFACT]
_PUBLISHABLE_ARTIFACTS = [*CSV_ARTIFACTS, 'published']


def get_s3() -> any:
    global _s3
    if _s3 is None:
        _s3 = boto3.client('s3')
    return _s3


def read_from_s3(programid: str, artifact: str = None, bucket: str = None, versionid: str = None) -> \
        Tuple[ProgramSpec, List[str]]:
    """
    Reads a program specification from an .xlsx file in S3.

    There can be up to three program spec .xlsx files in S3 for a given program, the "published"
    spec, an "unpublished" spec (an export from the SQL database, but not exported for the ACM,
    useful to see what *would* be given to the ACM), and "pending" (a file uploaded and checked
    but not inserted into the database)

    :param programid: The program for which the program spec is desired.
    :param bucket: The S3 bucket containing program specs. Default 'amplio-progspecs'
    :param artifact: One of 'published', 'pending', or 'unpublished'. Default 'published'
    :param versionid: If specified, the S3 version id desired. There is currently no
            support for querying the published artifact version ids.

    :return: A ProgramSpec object, or a List[str] of errors.
    """
    bucket = bucket or _PROGSPEC_BUCKET
    artifact = artifact or PUBLISHED_ARTIFACT
    if artifact not in _ARTIFACT_KEYS:
        raise ValueError(f'"artifact" must be one of {_ARTIFACT_KEYS.keys()}')
    key = f'{programid}/{_ARTIFACT_KEYS[artifact]}'
    s3 = get_s3()
    params = {
        'Bucket': bucket,
        'Key': key
    }
    if versionid:
        params['Versionid'] = versionid
    obj = s3.get_object(**params)
    obj_data = obj.get('Body').read()
    from .XlsxReaderWriter import read_from_xlsx
    ok, errors = read_from_xlsx(programid, obj_data)
    return ok, errors


def _key_for_artifact(programid: str, artifact: str) -> str:
    if artifact in CSV_ARTIFACTS:
        return f'{programid}/pub_{artifact}.csv'
    return f'{programid}/{_ARTIFACT_KEYS[artifact]}'


def _list_versions(Bucket=_PROGSPEC_BUCKET, Prefix=''):
    paginator = get_s3().get_paginator("list_object_versions")
    kwargs = {'Bucket': Bucket, 'Prefix': Prefix}
    for versions in paginator.paginate(**kwargs):
        for version in versions.get('Versions', []):
            yield version

    # Delete versions that match the given prefix. Optional string or iterable list of versions to not delete.


def _delete_versions(prefix: str, versions_to_keep=None, bucket=_PROGSPEC_BUCKET):
    s3 = get_s3()
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
            s3.delete_objects(Delete={'Objects': to_delete}, Bucket=bucket)
            to_delete.clear()

    if len(to_delete) > 0:
        s3.delete_objects(Delete={'Objects': to_delete}, Bucket=bucket)


def _write_to_bucket(programid: str, artifact: str, data: any, bucket: str, errors: List[str],
                     metadata: Dict[str, str] = None):
    s3 = get_s3()
    result = True
    key = _key_for_artifact(programid, artifact)
    try:
        s3_put_result = s3.put_object(Body=data, Bucket=bucket, Key=key, Metadata=metadata)
        if s3_put_result.get('ResponseMetadata', {}).get('HTTPStatusCode') != 200:
            result = False
            errors.append(f"Couldn't publish '{key}' for {programid} to s3 in bucket '{bucket}'.")
        elif artifact not in _VERSIONED_ARTIFACTS:
            _delete_versions(key, versions_to_keep=s3_put_result.get('VersionId'))
    except Exception:
        result = False
        errors.append(
            f"Couldn't publish '{key}' for {programid} to s3 in bucket " +
            f"{bucket}': {traceback.format_exc()}.")
    return result


def _write_artifacts_to_bucket(program_spec, artifacts: List[str], bucket: str = _PROGSPEC_BUCKET, metadata = None) -> \
        Tuple[bool, List[str]]:
    data = {}
    for artifact in artifacts:
        if artifact in CSV_ARTIFACTS:
            data[artifact] = write_to_csv(program_spec, artifact)
        else:
            data[artifact] = write_to_xlsx(program_spec)

    programid = program_spec.program_id
    result = True
    errors: List[str] = []
    for artifact in _PUBLISHABLE_ARTIFACTS:
        result = result and _write_to_bucket(programid, artifact, data[artifact], bucket, errors, metadata)

    return result, errors


def write_to_s3(program_spec, artifact: str, bucket: str = _PROGSPEC_BUCKET, metadata: Dict[str,str] = None) -> Tuple[bool, List[str]]:
    if artifact not in _ARTIFACT_KEYS:
        raise ValueError(f'"artifacts" must contain one or more of {_ARTIFACT_KEYS.keys()}')
    if metadata is None:
        metadata = {'submission-date': datetime.now().isoformat()}
    return _write_artifacts_to_bucket(program_spec, [artifact], bucket, metadata)


def publish_to_s3(program_spec, bucket: str = _PROGSPEC_BUCKET, metadata:Dict[str,str] = None) -> Tuple[bool, List[str]]:
    if metadata is None:
        metadata = {'submission-date': datetime.now().isoformat()}
    return _write_artifacts_to_bucket(program_spec, [PUBLISHED_ARTIFACT, *CSV_ARTIFACTS], bucket, metadata)
