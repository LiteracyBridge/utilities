#!/usr/bin/env zsh
"exec" "./acmEnv/bin/python3" "newAcm.py" "$@"
import argparse
import shutil
import subprocess
import sys
from os.path import expanduser
from pathlib import Path

import boto3

from sqlUtils import initialize_postgres, check_for_postgresql_project, populate_postgresql
from dropboxUtils import initialize_dbx, check_for_existing_dbx_content, check_for_existing_dbx_folder, \
    create_and_share_dbx
from dynamoUtils import check_for_checkout, create_checkout_record, check_for_program_record, create_program_record, \
    check_for_organization_record, create_organization_record

from utils import set_dropbox_directory, canonical_acm_dir_name, canonical_acm_path_name, canonical_acm_project_name, \
    StorePathAction

# s3 and projspec, dashboard buckets
s3_client = boto3.client('s3')
projspec_bucket: str = 'amplio-progspecs'
content_bucket: str = 'amplio-program-content'
project_list_bucket: str = 'dashboard-lb-stats'
project_list_key: str = 'data/project_list.csv'


# Properties of the two dropbox users: a maintaining user and the processing user.

args = {}
dropbox_directory:Path = None

# Get the user name and password that we need to sign into the SQL database. Configured through AWS console.


# Make a connection to the SQL database

# List the objects with the given prefix.
# noinspection PyPep8Naming
def _list_objects(Bucket, Prefix='', **kwargs):
    paginator = s3_client.get_paginator("list_objects_v2")
    kwargs = {'Bucket': Bucket, 'Prefix': Prefix, **kwargs}
    for objects in paginator.paginate(**kwargs):
        for obj in objects.get('Contents', []):
            yield obj


def fetch_template_progspec() -> bytes:
    """
    Download the template program specification to the desired file.
    :param dest_path: to receive the program specification.
    :return:
    """
    import urllib.request
    url = 'https://s3-us-west-2.amazonaws.com/dashboard-lb-stats/' + \
          'ProgramSpecificationTemplate/Template-ProgramSpecification.xlsx'
    # Download the file from `url` and save it locally under `file_name`:
    with urllib.request.urlopen(url) as response:
        data = response.read()  # a `bytes` object
        return data


def write_template_progspec(dest_path='~/template.xlsx'):
    """
    Download the template program specification to the desired file.
    :param dest_path: to receive the program specification.
    :return:
    """
    file_name = expanduser(dest_path)
    # Download the file from `url` and save it locally under `file_name`:
    data = fetch_template_progspec()
    with open(file_name, 'wb') as out_file:
        out_file.write(data)


# def find_acm_in_dbx(acm: str):
#     """
#     Given an acm name, see if there is a top-level folder of that name, even if
#     deleted, for the maintenance or processing user. If so, those will need to
#     be manually obliterated for the sharing to work correctly (otherwise Drobpox
#     will screw up the name, appending ' (1)' or something similar.
#     :param acm: Name of the ACM.
#     :return: (path, deleted) or None, for each of the files.
#     """
#     def entry_info(entries):
#         if len(entries) == 1:
#             return entries[0].name, isinstance(entries[0], dropbox.files.DeletedMetadata)
#         return None
#
#     acm_dir = canonical_acm_dir_name(acm)
#     folder_list = dbx_maint.files_list_folder(path="", include_deleted=True)
#     file1 = entry_info([x for x in folder_list.entries if x.name == acm_dir])
#     folder_list = dbx_process.files_list_folder(path="", include_deleted=True)
#     file2 = entry_info([x for x in folder_list.entries if x.name == acm_dir])
#     return file1, file2


def check_for_existing_s3_content(program_id: str) -> bool:
    """
    Checks to see if there is program content in S3 for the given acm.
    :param program_id: to be checked.
    :return: True if there is no existing program content, False if there is
    """
    print(f"Looking for program '{program_id}' content objects in s3...", end='')
    prefix = f'{program_id}/'
    paginator = s3_client.get_paginator("list_objects_v2")
    kwargs = {'Bucket': content_bucket, 'Prefix': prefix}
    objs = []
    for objects in paginator.paginate(**kwargs):
        objs.extend(objects.get('Contents', []))
    if objs:
        print(f"\n  Found program content objects for '{program_id}'.")
        return False
    print('ok')
    return True


def check_for_existing_content(program_id: str, acm_dir: str) -> bool:
    global args
    if args.s3:
        return check_for_existing_s3_content(program_id)
    else:
        return check_for_existing_dbx_content(acm_dir)


def check_for_programspec(program_id) -> bool:
    """
    Checks to see if there is a program spec in S3 for the given acm.
    :param program_id: to be checked.
    :return: True if there is no existing program spec, False if there is one
    """
    print(f"Looking for program spec '{program_id}' objects in s3...", end='')
    prefix = f'{program_id}/'
    paginator = s3_client.get_paginator("list_objects_v2")
    kwargs = {'Bucket': projspec_bucket, 'Prefix': prefix}
    objs = []
    for objects in paginator.paginate(**kwargs):
        objs.extend(objects.get('Contents', []))
    if objs:
        print('\n  Found program spec objects for {}'.format(program_id))
        return False
    print('ok')
    return True


# noinspection SqlResolve,SqlNoDataSourceInspection,SqlDialectInspection


def project_list_path():
    global dropbox_directory
    return Path(dropbox_directory, 'DashboardReports', 'project_list.csv')


def check_for_project_list_entry(program_id) -> bool:
    """
    Checks for an existing entry in the projects_list.csv file in the DashboardReports directory.
    :param program_id: to be checked.
    :return: True if there is no row for the acm, False if there already is one.
    """
    global dropbox_directory
    print(f"Looking for '{program_id}' entry in {dropbox_directory}/DashboardReports/project_list.csv...", end='')
    with open(project_list_path(), 'r') as pl:
        lines = [x.strip().split(',')[0] for x in pl.readlines()]
    if program_id in lines:
        print('\n  {} exists in project_list'.format(program_id))
        return False

    print('ok')
    return True


def create_and_populate_acm_directory(acm_dir):
    """
    Copies ACM-template as the new project.
    :param program_name: to be created
    :return: True
    """
    acm_path = canonical_acm_path_name(acm_dir)
    template_path = canonical_acm_path_name('template', upper=False)
    print(f'Creating and populating acm directory for {acm_dir}...', end='')

    try:
        shutil.copytree(template_path, acm_path)
        print('ok')
        return True
    except Exception as ex:
        print('exception copying template acm: {}'.format(ex))
        return False


def create_and_populate_s3_object(program_id):
    """
    Copy content from ${content_bucket}/template to ${content_bucket}/${program_name}
    """
    print(f'Creating and populating s3 folder for {program_id}...', end='')
    try:
        for s3_obj in _list_objects(Bucket=content_bucket, Prefix='template/'):
            source_key = s3_obj['Key']
            dest_key = program_id + source_key[8:]
            response = s3_client.copy({'Bucket': content_bucket, 'Key': source_key},
                                      content_bucket, dest_key)
        print('ok')
        return True
    except Exception as ex:
        print(f'Exception copying template acm: {ex}')
        return False


def create_and_populate_content(program_id: str, acm_dir: str) -> bool:
    global args
    if args.s3:
        return create_and_populate_s3_object(program_id)
    else:
        return create_and_populate_acm_directory(acm_dir)


def initialize_programspec(program_id: str, is_s3: bool) -> bool:
    """
    Downloads the program spec template to the projspec directory. Names it as
    PROJECT-programspec.xlsx to avoid conflicts with the real new program spec.
    Opens the file in Excel to be edited
    :param program_id: to get the program spec
    :return: True
    """
    print(f'Creating program spec for {program_id}...', end='')
    if is_s3:
        data:bytes = fetch_template_progspec()
        key = f'{program_id}/program_spec.xlsx'
        print(f"writing program spec to 's3://{projspec_bucket}/{key}'...", end='')
        kwargs = {'Bucket': projspec_bucket, 'Key': key, 'Body': data}
        put_result = s3_client.put_object(**kwargs)
        if put_result.get('ResponseMetadata', {}).get('HTTPStatusCode', 0) == 200:
            print('ok\n  -- Download the spec, edit appropriately, and use the dashboard to submit')
            return True
    else:
        acm_path = canonical_acm_path_name(program_id)
        progspec_dir = Path(acm_path, 'programspec')
        progspec_dir.mkdir(parents=True, exist_ok=True)
        progspec = Path(progspec_dir, f'{program_id}-programspec.xlsx')
        write_template_progspec(progspec)
        print('opening program spec...', end='')
        subprocess.run(['open', str(progspec)], check=True)
        print(f'ok\n  -- Edit the spec and use the Dashboard to submit for {program_id}')
        return True
    return False

# noinspection SqlResolve,SqlNoDataSourceInspection


def populate_project_list(program_id: str) -> bool:
    """
    Adds the project name to projects_list.csv in DashboardReports, then uploads the files to
        s3://dashboard-lb-stats/data/project_list.csv
    :param program_id: to be added
    :return: True
    """
    print(f"Adding '{program_id}' to projects_list...", end='')
    path = project_list_path()
    with open(path, 'a') as pl:
        print(f'{program_id},{program_id}/', file=pl)
    s3_client.upload_file(Filename=str(path), Bucket=project_list_bucket, Key=project_list_key)
    print('ok')
    return True


# noinspection PyUnresolvedReferences
def new_acm():
    global args
    ok = True
    acm = args.acm
    program_id = canonical_acm_project_name(acm)
    acm_dir = program_id if args.s3 else canonical_acm_dir_name(acm)

    if args.do_content != 'none':
        ok = check_for_existing_content(program_id, acm_dir) and ok
    if args.do_dropbox != 'none':
        ok = check_for_existing_dbx_folder(acm_dir) and ok

    if args.do_checkout != 'none':
        ok = check_for_checkout(acm_dir) and ok

    if args.do_program != 'none':
        ok = check_for_program_record(program_id) and ok
    if args.do_organization != 'none':
        ok = check_for_organization_record(args.organization, args.parent_organization) and ok

    if args.do_progspec != 'none':
        ok = check_for_programspec(program_id) and ok
    if args.do_sql != 'none':
        ok = check_for_postgresql_project(program_id) and ok

    if args.do_dashboard != 'none':
        ok = check_for_project_list_entry(program_id) and ok

    if ok and not args.dry_run:
        print(f'\nCreating entries for {program_id}.\n')

        if args.do_content == 'both':
            ok = create_and_populate_content(program_id, acm_dir) and ok
        if args.do_dropbox == 'both':
            ok = create_and_share_dbx(program_id, args.admin, args.comment) and ok

        if args.do_checkout == 'both':
            ok = create_checkout_record(acm_dir) and ok
        if args.do_program == 'both':
            ok = create_program_record(program_id, args.organization, args.admin, args.s3, args.comment)

        if args.do_organization == 'both':
            ok = create_organization_record(args.organization, args.parent_organization)

        if args.do_progspec == 'both':
            ok = initialize_programspec(program_id, args.s3) and ok
        if args.do_sql == 'both':
            ok = populate_postgresql(program_id, args.comment) and ok

        if args.do_dashboard == 'both':
            ok = populate_project_list(program_id) and ok

        if not ok:
            print(f'Errors encountered creating or sharing acm {program_id}')
    else:
        print()
        if not ok:
            print('Issues detected; ', end='')
        if args.dry_run:
            print('Dry run; ', end='')
        print('no action attempted.')

    return ok


def _initialize_postgres():
    global args
    parms = {}
    if args.db_host:
        parms['host'] = args.db_host
    if args.db_port:
        parms['port'] = int(args.db_port)
    if args.db_user:
        parms['user'] = args.db_user
    if args.db_password:
        parms['password'] = args.db_password
    if args.db_name:
        parms['database'] = args.db_name
    initialize_postgres(parms)


def main():
    global args, dropbox_directory
    arg_parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    arg_parser.add_argument('--dropbox', action=StorePathAction, default=expanduser('~/Dropbox'),
                            help='Dropbox directory (default is ~/Dropbox).')
    arg_parser.add_argument('acm', metavar='ACM', help='The new ACM name')
    arg_parser.add_argument('--s3', action='store_true', help='The program\'s content storage is in S3, not Drobpox.')
    arg_parser.add_argument('--comment', required=True, help='Program comment.')
    arg_parser.add_argument('--organization', '--org', help='The program\'s organization.')
    arg_parser.add_argument('--parent-organization', '--parent_org', '--parent',
                            help='The program\'s organization\'s parent.',
                            default='Amplio')
    arg_parser.add_argument('--admin', help='Email address of the program administrator.')
    arg_parser.add_argument('--dry-run', '--dryrun', '-n', action='store_true', help='Don\'t update anything.')

    arg_parser.add_argument('--do-content', '--do-acm', choices=['none', 'check', 'both'], default='both',
                            help='Do or don\'t create ACM directory.')
    arg_parser.add_argument('--do-dropbox', choices=['none', 'check', 'both'], default='both',
                            help='Do or don\'t create content folders.')
    arg_parser.add_argument('--do-sql', choices=['none', 'check', 'both'], default='both',
                            help='Do or don\'t check or update projects table in PostgreSQL.')
    arg_parser.add_argument('--do-dashboard', choices=['none', 'check', 'both'], default='both',
                            help='Do or don\'t check or update projects_list in dashboard.')
    arg_parser.add_argument('--do-checkout', choices=['none', 'check', 'both'], default='both',
                            help='Do or don\'t create a checkout record.')
    arg_parser.add_argument('--do-progspec', choices=['none', 'check', 'both'], default='both',
                            help='Do or don\'t check or create program specification.')
    arg_parser.add_argument('--do-program', choices=['none', 'check', 'both'], default='both',
                            help='Do or don\'t check or create a program record.')
    arg_parser.add_argument('--do-organization', choices=['none', 'check', 'both'], default='both',
                            help='Do or don\'t check or create an organization record.')

    arg_parser.add_argument('--feedback', action='store_true', help='Create an ACM suitable for User Feedback.')

    arg_parser.add_argument('--db-host', default=None, metavar='HOST',
                            help='Optional host name, default from secrets store.')
    arg_parser.add_argument('--db-port', default=None, metavar='PORT',
                            help='Optional host port, default from secrets store.')
    arg_parser.add_argument('--db-user', default=None, metavar='USER',
                            help='Optional user name, default from secrets store.')
    arg_parser.add_argument('--db-password', default=None, metavar='PWD',
                            help='Optional password, default from secrets store.')
    arg_parser.add_argument('--db-name', default='dashboard', metavar='DB',
                            help='Optional database name, default "dashboard".')

    args = arg_parser.parse_args()
    if args.s3 and args.do_dropbox == 'both':
        print('Options \'s3\' and \'do_dropbox both\' are incompatible. Setting \'do_dropbox check\'.')
        args.do_dropbox = 'check'
    dropbox_directory = args.dropbox
    set_dropbox_directory(dropbox_directory)

    _initialize_postgres()

    if args.feedback:
        args.no_sql = True
        args.no_dashboard = True
        args.no_progspec = True

    if not args.s3 or args.do_dropbox != 'none':
        initialize_dbx()
    new_acm()


if __name__ == '__main__':
    sys.exit(main())
