import json
import time
from typing import Union, List

import boto3
import dropbox
from botocore.exceptions import ClientError

from utils import canonical_acm_path_name, canonical_acm_dir_name

dbx_admin: Union[None, dropbox.Dropbox] = None
dbx_processing: Union[None, dropbox.Dropbox] = None
admin_user = None
processing_user = None
processing_email: str = None


def initialize_dbx():
    global dbx_admin, dbx_processing, admin_user, processing_user, processing_email
    admin_secret, processing_secret = get_dbx_secret()

    dbx_admin = dropbox.Dropbox(admin_secret)
    result = dbx_admin.users_get_current_account()
    admin_user = '{}, {}'.format(result.name.display_name, result.email)

    dbx_processing = dropbox.Dropbox(processing_secret)
    result = dbx_processing.users_get_current_account()
    processing_user = '{}, {}'.format(result.name.display_name, result.email)
    processing_email = result.email

    list_dbx_acms()


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


def list_dbx_acms():
    folder_list = dbx_admin.files_list_folder(path="", include_deleted=True)
    # isinstance(folder_list.entries[0], dropbox.files.DeletedMetadata)
    folder_list2 = dbx_processing.files_list_folder(path="", include_deleted=True)
    files = [y for y in [x.name for x in folder_list.entries] if y[0:4] == 'ACM-']
    files2 = [y for y in [x.name for x in folder_list2.entries] if y[0:4] == 'ACM-']
    return files, files2


def check_for_existing_dbx_content(acm_dir) -> bool:
    """
    Checks whether the directory exists in Dropbox. This utility requires that it not exist.
    :param acm_dir: for which to check.
    :return: True if there is no directory, False if there is one.
    """
    acm_path = canonical_acm_path_name(acm_dir)
    print(f"Looking for existing directory '{acm_dir}' in local dropbox...", end='')
    if acm_path.exists():
        print(f"\n  '{acm_dir}' already exists in Dropbox directory.")
        return False
    print('ok.')
    return True


def check_for_existing_dbx_folder(acm_dir) -> bool:
    """
    Given an acm name, see if there is a top-level folder of that name, even if
    deleted, for the maintenance or processing user. If so, those will need to
    be manually obliterated for the sharing to work correctly (otherwise Drobpox
    will screw up the name, appending ' (1)' or something similar.
    :param program_name: Name of the ACM.
    :return:
    """

    def entry_info(entries):
        if len(entries) == 1:
            return entries[0].name, isinstance(entries[0], dropbox.files.DeletedMetadata)
        return None

    result = True
    acm_upper = acm_dir.upper()
    print(f"Looking for folder '{acm_upper}' in dropbox cloud...", end='')
    folder_list = dbx_admin.files_list_folder(path="", include_deleted=True)
    file_admin = entry_info([x for x in folder_list.entries if x.name.upper() == acm_upper])
    folder_list = dbx_processing.files_list_folder(path="", include_deleted=True)
    file_process = entry_info([x for x in folder_list.entries if x.name.upper() == acm_upper])
    if file_admin:
        print("\n  '{}' exists{} for admin user '{}'".format(file_admin[0], ' (deleted)' if file_admin[1] else '',
                                                             admin_user), end='')
        result = False
    if file_process:
        print("\n  '{}' exists{} for process user '{}'".format(file_process[0], ' (deleted)' if file_process[1] else '',
                                                               processing_user), end='')
        result = False
    print('ok.' if result else '')
    return result


def create_and_share_dbx(program_name: str, admin: str, comment: str) -> bool:
    """
    Creates and shares Dropbox folder.
    :param program_name: acm name
    :return: True if successful, False if errors.
    """
    acm_dir = '/' + canonical_acm_dir_name(program_name)
    print(f'Creating and sharing Dropbox folder {acm_dir}...', end='')
    users = [processing_email]
    if admin:
        users.append(admin)

    ok = create_shared_folder(acm_dir, dropbox.sharing.AccessLevel.editor, users,
                              f"Dropbox share of content database for '{comment}' ({program_name}).")

    if ok and admin:
        ok = share_existing_folder('/LB-software', dropbox.sharing.AccessLevel.viewer, [admin],
                                   f'Dropbox share of Amplio software.')
    if ok:
        print('ok')
    return ok


def share_existing_folder(folder_path: str, access_level, emails: List[str], message: str) -> bool:
    """
    Shares an existing dropbox folder with one or more users.
    :param folder_path: path to the folder to be shared
    :param access_level: "viewer", "editor", "owner"
    :param emails: list of emails of the users to whom it is to be shared
    :param message: the message to give to the user in the dropbxo share
    """
    try:
        sw_metadata:dropbox.files.Metadata = dbx_admin.files_get_metadata(folder_path)

        members = []
        for email in emails:
            member = dropbox.sharing.MemberSelector.email(email)
            add_member = dropbox.sharing.AddMember(member, access_level)
            members.append(add_member)
        # noinspection PyUnboundLocalVariable
        dbx_admin.sharing_add_folder_member(sw_metadata.shared_folder_id, members, custom_message=message)
        return True
    except Exception as ex:
        print('Exception sharing folder: {}'.format(ex))
    return False


def create_shared_folder(folder_path: str, access_level, emails: List[str], message: str):
    """
    Creates and shares a dropbox folder. It's OK if the folder already exists on disk.
    :param folder_path: to be created / shared.
    :param access_level: of the user to whom it is shared
    :param emails: list of emails of the users to whom it is to be shared
    :param message: for the email to the user
    :return: True if successful, False if not
    """

    def retry_sharing_job(job_id):
        sharing_job = dbx_admin.sharing_check_share_job_status(job_id)
        if sharing_job.is_complete():
            print("Async sharing job completed.", end='')
        else:
            print("waiting...", end='')
            time.sleep(3)
            retry_sharing_job(job_id)

    try:
        dbx_admin.files_create_folder_v2(folder_path, autorename=False)
        # sharing_folder = dbx_maint.sharing_share_folder(folder_path, force_async=True)
        sharing_folder = dbx_admin.sharing_share_folder(folder_path)
        if sharing_folder.is_complete():
            sharing_folder_data = sharing_folder.get_complete()
        if sharing_folder.is_async_job_id():
            async_job_id = sharing_folder.get_async_job_id()
            # helper function will block until async sharing job completes
            retry_sharing_job(async_job_id)
            sharing_folder_job = dbx_admin.sharing_check_share_job_status(async_job_id)
            sharing_folder_data = sharing_folder_job.get_complete()

        if not hasattr(emails, '__iter__') or isinstance(emails, (str, bytes)):
            emails = [emails]

        members = []
        for email in emails:
            member = dropbox.sharing.MemberSelector.email(email)
            add_member = dropbox.sharing.AddMember(member, access_level)
            members.append(add_member)
        # noinspection PyUnboundLocalVariable
        dbx_admin.sharing_add_folder_member(sharing_folder_data.shared_folder_id, members, custom_message=message)

        return True
    except Exception as ex:
        print('Exception creating or sharing folder: {}'.format(ex))
    return False