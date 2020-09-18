import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import time, datetime
from os.path import expanduser
from pathlib import Path

import boto3
import dropbox
import pg8000
from botocore.exceptions import ClientError

import amplio.rolemanager.manager as roles_manager
from amplio.rolemanager.Roles import *

REGION_NAME = 'us-west-2'

# specify dynamoDB table for checkout records
dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
CHECKOUT_TABLE_NAME = 'acm_check_out'
PROGRAM_TABLE_NAME = 'programs'
ORGANIZATION_TABLE_NAME = 'organizations'
checkout_table = dynamodb.Table(CHECKOUT_TABLE_NAME)
program_table = dynamodb.Table(PROGRAM_TABLE_NAME)
organization_table = dynamodb.Table(ORGANIZATION_TABLE_NAME)

# s3 and projspec, dashboard buckets
s3_client = boto3.client('s3')
projspec_bucket: str = 'amplio-progspecs'
project_list_bucket: str = 'dashboard-lb-stats'
project_list_key: str = 'data/project_list.csv'

# This will be a connection to the PostgreSQL database
db_connection = None

dropbox_directory = ''

# Properties of the two dropbox users: a maintaining user and the processing user.
dbx_maint = None
dbx_process = None
user_maint = None
user_process = None
email_process = None

args = {}


def initialize_dbx():
    global dbx_maint, dbx_process, user_maint, user_process, email_process
    secret_admin, secret_process = get_dbx_secret()

    dbx_maint = dropbox.Dropbox(secret_admin)
    result = dbx_maint.users_get_current_account()
    user_maint = '{}, {}'.format(result.name.display_name, result.email)

    dbx_process = dropbox.Dropbox(secret_process)
    result = dbx_process.users_get_current_account()
    user_process = '{}, {}'.format(result.name.display_name, result.email)
    email_process = result.email


def get_dbx_secret():
    secret_name = "NewAcmProject_token"
    region_name = "us-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    get_secret_value_response = None

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

    # Decrypts secret using the associated KMS CMK.
    secret_json_str = get_secret_value_response['SecretString']
    secret_json = json.loads(secret_json_str)
    secret = secret_json.get('token')
    secret2 = secret_json.get('processing')

    return secret, secret2


# Get the user name and password that we need to sign into the SQL database. Configured through AWS console.
def get_postgresql_secret() -> dict:
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

    # Your code goes here.
    return result


def canonical_acm_dir_name(acm, upper=True):
    if upper:
        acm = acm.upper()
    if not acm.startswith('ACM-'):
        acm = 'ACM-' + acm
    return acm


def canonical_acm_path_name(acm, upper=True):
    global dropbox_directory
    acm_path = dropbox_directory + '/' + canonical_acm_dir_name(acm, upper=upper)
    return Path(acm_path)


def canonical_acm_project_name(acmdir):
    if acmdir is None:
        return None
    _, acm = os.path.split(acmdir)
    acm = acm.upper()
    if acm.startswith('ACM-'):
        acm = acm[4:]
    return acm


ENSURE_TRAILING_SLASH = ['--outdir']


class StorePath(argparse.Action):
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        if nargs is not None:
            raise ValueError("nargs not allowed")
        super(StorePath, self).__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        values = expanduser(values)
        if option_string in ENSURE_TRAILING_SLASH and values[-1:] != '/':
            values += '/'
        setattr(namespace, self.dest, values)


# Make a connection to the SQL database
def get_db_connection():
    global args, db_connection
    if db_connection is None:
        secret = get_postgresql_secret()

        parms = {'database': 'dashboard', 'user': secret['username'], 'password': secret['password'],
                 'host': secret['host'], 'port': secret['port']}
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

        db_connection = pg8000.connect(**parms)

    return db_connection


def fetch_template_progspec(dest_path='~/template.xlsx'):
    """
    Download the template program specification to the desired file.
    :param dest_path: to receive the program specification.
    :return:
    """
    import urllib.request
    url = 'https://s3-us-west-2.amazonaws.com/dashboard-lb-stats/' + \
          'ProgramSpecificationTemplate/Template-ProgramSpecification.xlsx'
    file_name = expanduser(dest_path)
    # Download the file from `url` and save it locally under `file_name`:
    with urllib.request.urlopen(url) as response, open(file_name, 'wb') as out_file:
        data = response.read()  # a `bytes` object
        out_file.write(data)


# def list_dbx_acms():
#     folder_list = dbx_maint.files_list_folder(path="", include_deleted=True)
#     # isinstance(folder_list.entries[0], dropbox.files.DeletedMetadata)
#     folder_list2 = dbx_process.files_list_folder(path="", include_deleted=True)
#     files = [y for y in [x.name for x in folder_list.entries] if y[0:4] == 'ACM-']
#     files2 = [y for y in [x.name for x in folder_list2.entries] if y[0:4] == 'ACM-']
#     return files, files2


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


def create_shared_folder(folder_path, access_level, email, message):
    """
    Creates and shares a dropbox folder. It's OK if the folder already exists on disk.
    :param folder_path: to be created / shared.
    :param access_level: of the user to whom it is shared
    :param email: of the user to whom it is shared
    :param message: for the email to the user
    :return: True if successful, False if not
    """
    dbx_owner = dbx_maint

    def retry_sharing_job(job_id):
        sharing_job = dbx_owner.sharing_check_share_job_status(job_id)
        if sharing_job.is_complete():
            print("Async sharing job completed.", end='')
        else:
            print("waiting...", end='')
            time.sleep(3)
            retry_sharing_job(job_id)

    try:
        dbx_owner.files_create_folder_v2(folder_path, autorename=False)
        # sharing_folder = dbx_maint.sharing_share_folder(folder_path, force_async=True)
        sharing_folder = dbx_owner.sharing_share_folder(folder_path)
        if sharing_folder.is_complete():
            sharing_folder_data = sharing_folder.get_complete()
        if sharing_folder.is_async_job_id():
            async_job_id = sharing_folder.get_async_job_id()
            # helper function will block until async sharing job completes
            retry_sharing_job(async_job_id)
            sharing_folder_job = dbx_owner.sharing_check_share_job_status(async_job_id)
            sharing_folder_data = sharing_folder_job.get_complete()

        member = dropbox.sharing.MemberSelector.email(email)
        add_member = dropbox.sharing.AddMember(member, access_level)
        members = [add_member]
        # noinspection PyUnboundLocalVariable
        dbx_maint.sharing_add_folder_member(sharing_folder_data.shared_folder_id, members, custom_message=message)

        return True
    except Exception as ex:
        print('Exception creating or sharing folder: {}'.format(ex))
    return False


def check_for_existing_directory(acm_name):
    """
    Checks whether the directory exists in Dropbox. This utility requires that it not exist.
    :param acm_name: for which to check.
    :return: True if there is no directory, False if there is one.
    """
    acm_path = canonical_acm_path_name(acm_name)
    acm_dir = canonical_acm_dir_name(acm_name)
    print('Looking for existing directory in local dropbox...', end='')
    if acm_path.exists():
        print("\n  '{}' already exists in Dropbox directory.".format(acm_dir))
        return False
    print('ok.')
    return True


def check_for_existing_dbx_folder(acm_name):
    """
    Given an acm name, see if there is a top-level folder of that name, even if
    deleted, for the maintenance or processing user. If so, those will need to
    be manually obliterated for the sharing to work correctly (otherwise Drobpox
    will screw up the name, appending ' (1)' or something similar.
    :param acm_name: Name of the ACM.
    :return:
    """

    def entry_info(entries):
        if len(entries) == 1:
            return entries[0].name, isinstance(entries[0], dropbox.files.DeletedMetadata)
        return None

    print('Looking for folder in dropbox cloud...', end='')
    result = True
    acm_dir = canonical_acm_dir_name(acm_name)
    acm_upper = acm_dir.upper()
    folder_list = dbx_maint.files_list_folder(path="", include_deleted=True)
    file_admin = entry_info([x for x in folder_list.entries if x.name.upper() == acm_upper])
    folder_list = dbx_process.files_list_folder(path="", include_deleted=True)
    file_process = entry_info([x for x in folder_list.entries if x.name.upper() == acm_upper])
    if file_admin:
        print("\n  '{}' exists{} for admin user '{}'".format(file_admin[0], ' (deleted)' if file_admin[1] else '',
                                                             user_maint), end='')
        result = False
    if file_process:
        print("\n  '{}' exists{} for process user '{}'".format(file_process[0], ' (deleted)' if file_process[1] else '',
                                                               user_process), end='')
        result = False
    print('ok.' if result else '')
    return result


def check_for_checkout(acm_name):
    """
    Checks whether there is an existing checkout record for the ACM in DynamoDB.
    :param acm_name: to be checked
    :return: True if there is no record, False if thera already is one.
    """
    print('Looking for checkout record in dynamoDb...', end='')
    # legacy artifact -- the checkout db prepends acm names with 'ACM-'
    acm_dir = canonical_acm_dir_name(acm_name)
    query = checkout_table.get_item(Key={'acm_name': acm_dir})
    checkout_row = query.get('Item')
    if checkout_row:
        print('\n  Checkout record exists for {}'.format(acm_dir))
        return False
    print('ok')
    return True


def check_for_program_record(program_name):
    print('Looking for program record in dynamoDb...', end='')
    query = program_table.get_item(Key={'program': program_name})
    program_row = query.get('Item')
    if program_row:
        print('\n  Program record exists for {}'.format(program_name))
        return False
    print('ok')
    return True


def check_for_organization_record(organization, parent_organization):
    print('Checking for conflicting organization record in dynamoDb...', end='')
    query = organization_table.get_item(Key={'organization': organization})
    organization_row = query.get('Item')
    if organization_row:
        existing_parent = organization_row.get('parent')
        if existing_parent != parent_organization:
            print('\n  Organization record exists for {}, but with different parent. {} exists, {} desired'
                  .format(organization, existing_parent, parent_organization))
            return False
    else:
        parent_row = organization_table.get_item(Key={'organization': parent_organization})
        if not parent_row.get('Item'):
            print('\n  Missing parent {} for new organization {}'.format(parent_organization, organization))
            return False
    print('ok')
    return True


def check_for_programspec(acm_name):
    """
    Checks to see if there is a program spec in S3 for the given acm.
    :param acm_name: to be checked.
    :return: True if there is no existing program spec, False if there is one
    """
    print('Looking for program spec objects in s3...', end='')
    prefix = '{}/'.format(acm_name)
    paginator = s3_client.get_paginator("list_objects_v2")
    kwargs = {'Bucket': projspec_bucket, 'Prefix': prefix}
    objs = []
    for objects in paginator.paginate(**kwargs):
        objs.extend(objects.get('Contents', []))
    if objs:
        print('\n  Found program spec objects for {}'.format(acm_name))
        return False
    print('ok')
    return True


# noinspection SqlResolve,SqlNoDataSourceInspection
def check_for_postgresql_project(acm_name):
    """
    Checks to see if the project exists in the PostgerSQL projects table.
    :param acm_name: to be checked.
    :return: True if there is no existing project record, False if there already is one.
    """
    print('Looking for project in PostgreSQL...', end='')

    connection = get_db_connection()
    cur = connection.cursor()
    cur.execute('SELECT projectcode FROM projects;')
    rows = [x[0] for x in cur]

    if acm_name in rows:
        print('\n  {} exists in PostgreSQL projects table'.format(acm_name))
        return False
    print('ok')
    return True


def project_list_path():
    global dropbox_directory
    return Path(dropbox_directory, 'DashboardReports', 'project_list.csv')


def check_for_project_list_entry(acm_name):
    """
    Checks for an existing entry in the projects_list.csv file in the DashboardReports directory.
    :param acm_name: to be checked.
    :return: True if there is no row for the acm, False if there already is one.
    """
    print('Looking for entry in {}/DashboardReports/project_list.csv...'.format(dropbox_directory), end='')
    with open(project_list_path(), 'r') as pl:
        lines = [x.strip().split(',')[0] for x in pl.readlines()]
    if acm_name in lines:
        print('\n  {} exists in project_list'.format(acm_name))
        return False

    print('ok')
    return True


def create_and_populate_acm_directory(acm_name):
    """
    Copies ACM-template as the new project.
    :param acm_name: to be created
    :return: True
    """
    acm_dir = canonical_acm_dir_name(acm_name)
    acm_path = canonical_acm_path_name(acm_name)
    template_path = canonical_acm_path_name('template', upper=False)
    print('Creating and populating acm directory for {}...'.format(acm_dir), end='')

    try:
        shutil.copytree(template_path, acm_path)
        print('ok')
        return True
    except Exception as ex:
        print('exception copying template acm: {}'.format(ex))
        return False


def create_and_share_dbx(acm_name):
    """
    Creates and shares Dropbox folder.
    :param acm_name: acm name
    :return: True if successful, False if errors.
    """
    acm_dir = '/' + canonical_acm_dir_name(acm_name)
    print('Creating and sharing Dropbox folder {}...'.format(acm_dir), end='')

    ok = create_shared_folder(acm_dir, dropbox.sharing.AccessLevel.editor, email_process,
                              'Folder for {}'.format(acm_name))

    if ok:
        print('ok')
    return ok


def create_checkout_record(acm_name):
    """
    Creates the initial checkout record for the project in DyanamoDB
    :param acm_name: to be created
    :return: True if successful, False if not
    """
    acm_dir = canonical_acm_dir_name(acm_name)
    print('Creating checkout record for {}...'.format(acm_dir), end='')
    update_expr = 'SET acm_state = :s, last_in_file_name = :f, last_in_name = :n, last_in_contact = :c, \
                last_in_date = :d, last_in_version = :v,\
                acm_comment = :z'
    condition_expr = 'attribute_not_exists(last_in_name)'  # only perform if no check-in entry exists
    expr_values = {
        ':s': 'CHECKED_IN',
        ':f': 'db1.zip',
        ':n': 'bill',
        ':c': 'techsupport@amplio.org',
        ':d': str(datetime.now()),
        ':v': 'c202002160',
        ':z': 'Created ACM'
    }

    try:
        checkout_table.update_item(
            Key={'acm_name': acm_dir},
            UpdateExpression=update_expr,
            ConditionExpression=condition_expr,
            ExpressionAttributeValues=expr_values
        )
    except Exception as err:
        print('exception creating record: {}'.format(err))
        return False

    print('ok')
    return True


def create_program_record(program_name, organization, admin_email):
    print('Creating program record for {} in organization {}'.format(program_name, organization), end='')
    update_expr = 'SET organization = :o'
    expr_values = {
        ':o': organization
    }
    item = {'program': program_name, 'organization': organization}
    if admin_email:
        item['roles'] = {admin_email: ADMIN_ROLES}
        update_expr += ', :rn = :r'
        expr_values[':rn'] = 'roles'
        expr_values[':r'] = 'M: {:e : ' + ADMIN_ROLES + ' }'
        expr_values[':e'] = admin_email

    try:
        program_table.put_item(Item=item)
        # program_table.update_item(
        #     Key={'program': program_name},
        #     UpdateExpression=update_expr,
        #     ExpressionAttributeValues=expr_values
        # )
    except Exception as err:
        print('exception creating record: {}'.format(err))
        return False

    print('ok')
    return True


def create_organization_record(organization, parent_organization):
    query = organization_table.get_item(Key={'organization': organization})
    organization_row = query.get('Item')
    if organization_row:
        print('Organization record already exists for {}'.format(organization))
        return True

    print('Creating organization record for {} with parent {}'.format(organization, parent_organization), end='')
    update_expr = 'SET parent = :p'
    expr_values = {
        ':p': parent_organization
    }

    try:
        organization_table.update_item(
            Key={'organization': organization},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_values
        )
    except Exception as err:
        print('exception creating record: {}'.format(err))
        return False

    print('ok')
    return True


def initialize_programspec(acm_name):
    """
    Downloads the program spec template to the projspec directory. Names it as
    PROJECT-programspec.xlsx to avoid conflicts with the real new program spec.
    Opens the file in Excel to be edited
    :param acm_name: to get the program spec
    :return: True
    """
    print('Creating program spec for {}...'.format(acm_name), end='')
    acm_path = canonical_acm_path_name(acm_name)
    progspec_dir = Path(acm_path, 'programspec')
    progspec_dir.mkdir(parents=True, exist_ok=True)
    progspec = Path(progspec_dir, '{}-programspec.xlsx'.format(acm_name))
    fetch_template_progspec(progspec)
    print('opening program spec...', end='')
    subprocess.run(['open', str(progspec)], check=True)
    print('ok\n  -- Edit the spec and use the Dashboard to submit for {}'.format(acm_name))
    return True


# noinspection SqlResolve,SqlNoDataSourceInspection
def populate_postgresql(acm_name):
    """
    Adds the project's row to the projects table in PostgreSQL
    :param acm_name: to be added
    :return: True if successful, False if not
    """
    global args
    print('Adding {} to PostgreSQL projects table...'.format(acm_name), end='')
    connection = get_db_connection()
    cur = connection.cursor()
    cur.execute('SELECT MAX(id) FROM projects;')
    rows = [x[0] for x in cur]
    new_id = max(rows) + 1
    comment = str(args.comment)

    cur.execute("INSERT INTO projects (id, projectcode, project, active) VALUES  (%s, %s, %s, True);",
                (new_id, acm_name, comment))
    num = cur.rowcount
    connection.commit()

    if num == 1:
        print('ok')
        return True

    print('Unexpected row count: {}'.format(num))
    return False


def populate_project_list(acm_name):
    """
    Adds the project name to projects_list.csv in DashboardReports, then uploads the files to
        s3://dashboard-lb-stats/data/project_list.csv
    :param acm_name: to be added
    :return: True
    """
    print('Adding {} to projects_list...'.format(acm_name), end='')
    path = project_list_path()
    with open(path, 'a') as pl:
        print('{0},{0}/'.format(acm_name), file=pl)
    s3_client.upload_file(Filename=str(path), Bucket=project_list_bucket, Key=project_list_key)
    print('ok')
    return True


# noinspection PyUnresolvedReferences
def new_acm():
    global args
    ok = True
    acm = args.acm
    acm_name = canonical_acm_project_name(acm)

    if args.do_acm != 'none':
        ok = check_for_existing_directory(acm_name) and ok
    if args.do_dropbox != 'none':
        ok = check_for_existing_dbx_folder(acm_name) and ok
    if args.do_checkout != 'none':
        ok = check_for_checkout(acm_name) and ok
    if args.do_program != 'none':
        ok = check_for_program_record(acm_name) and ok
    if args.do_organization != 'none':
        ok = check_for_organization_record(args.organization, args.parent_organization) and ok
    if args.do_progspec != 'none':
        ok = check_for_programspec(acm_name) and ok
    if args.do_sql != 'none':
        ok = check_for_postgresql_project(acm_name) and ok
    if args.do_dashboard != 'none':
        ok = check_for_project_list_entry(acm_name) and ok

    if ok and not args.dry_run:
        print('\nCreating entries for {}.\n'.format(acm_name))
        if args.do_acm == 'both':
            ok = create_and_populate_acm_directory(acm_name) and ok
        if args.do_dropbox == 'both':
            ok = create_and_share_dbx(acm_name) and ok
        if args.do_checkout == 'both':
            ok = create_checkout_record(acm_name) and ok
        if args.do_program == 'both':
            ok = create_program_record(acm_name, args.organization, args.admin)
        if args.do_organization == 'both':
            ok = create_organization_record(args.organization, args.parent_organization)
        if args.do_progspec == 'both':
            ok = initialize_programspec(acm_name) and ok
        if args.do_sql == 'both':
            ok = populate_postgresql(acm_name) and ok
        if args.do_dashboard == 'both':
            ok = populate_project_list(acm_name) and ok
        if not ok:
            print('Errors encountered creating or sharing acm {}'.format(acm_name))
    else:
        print()
        if not ok:
            print('Issues detected; ', end='')
        if args.dry_run:
            print('Dry run; ', end='')
        print('no action attempted.')

    return ok


def main():
    global args, dropbox_directory
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--dropbox', action=StorePath, default=expanduser('~/Dropbox'),
                            help='Dropbox directory (default is ~/Dropbox).')
    arg_parser.add_argument('acm', metavar='ACM', help='The new ACM name')
    arg_parser.add_argument('--comment', required=True, help='Program comment.')
    arg_parser.add_argument('--organization', '--org', help='The program\'s organization.')
    arg_parser.add_argument('--parent-organization', '--parent_org', '--parent',
                            help='The program\'s organization\'s parent.',
                            default='Amplio')
    arg_parser.add_argument('--admin', help='Email address of the program administrator.')
    arg_parser.add_argument('--dry-run', '-n', action='store_true', help='Don\'t update anything.')

    arg_parser.add_argument('--do-acm', choices=['none', 'check', 'both'], default='both',
                            help='Don\'t create ACM directory.')
    arg_parser.add_argument('--do-dropbox', choices=['none', 'check', 'both'], default='both',
                            help='Don\'t create dropbox folders.')
    arg_parser.add_argument('--do-sql', choices=['none', 'check', 'both'], default='both',
                            help='Don\'t check or update projects table in PostgreSQL.')
    arg_parser.add_argument('--do-dashboard', choices=['none', 'check', 'both'], default='both',
                            help='Don\'t check or update projects_list in dashboard.')
    arg_parser.add_argument('--do-checkout', choices=['none', 'check', 'both'], default='both',
                            help='Don\'t create a checkout record.')
    arg_parser.add_argument('--do-progspec', choices=['none', 'check', 'both'], default='both',
                            help='Don\'t check or create program specification.')
    arg_parser.add_argument('--do-program', choices=['none', 'check', 'both'], default='both',
                            help='Don\'t check or create a program record.')
    arg_parser.add_argument('--do-organization', choices=['none', 'check', 'both'], default='both',
                            help='Don\'t check or create an organization record.')

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
    dropbox_directory = args.dropbox

    if args.feedback:
        args.no_sql = True
        args.no_dashboard = True
        args.no_progspec = True

    initialize_dbx()
    new_acm()


if __name__ == '__main__':
    sys.exit(main())
