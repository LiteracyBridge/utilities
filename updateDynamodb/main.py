"""
Update dynamodb tables from PostgreSQL.
"""
import argparse
import json
from typing import Optional, Dict

import boto3
import pg8000
from botocore.exceptions import ClientError
from pg8000 import Connection

REGION_NAME = 'us-west-2'
dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
PROGRAMS_TABLE_NAME = 'programs'
programs_table = dynamodb.Table(PROGRAMS_TABLE_NAME)

db_connection: Optional[Connection] = None
connction_overrides = {}
connection_params = {}

args: Optional[argparse.Namespace] = None


def get_postgresql_secret() -> dict:
    """
    Retrieve PostgreSQL log in information from secrets manager.
    :return: a dictionary of login parameters
    """
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


def get_db_connection() -> Connection:
    """
    Lazily make a connection to the PostgreSQL database.
    :return: the db connection.
    """
    global db_connection
    if db_connection is None:
        secret = get_postgresql_secret()

        parms = {'database': 'dashboard', 'user': secret['username'], 'password': secret['password'],
                 'host': secret['host'], 'port': secret['port']}
        for o in connction_overrides:
            if o in connection_params:
                parms[o] = connection_params[o]

        db_connection = pg8000.connect(**parms)

    return db_connection


def get_programs_from_postgres() -> Dict[str, str]:
    """
    Gets the list of program-ids and friendly names from postgres. The programid is
    called "projectcode" and the friendly name is "project".
    :return: A dictionary of {programid: friendly-name}
    """
    connection = get_db_connection()
    cur = connection.cursor()
    cur.execute('SELECT projectcode, project FROM projects WHERE active AND id>=0;')
    result: dict = {row[0]: row[1] for row in cur}
    return result


def clean_program_name(programid: str) -> bool:
    """
    Cleans up the program_name (by deleting the obsolete "description").
    :param programid: to be cleaned
    :return: True if successful, False if an exception occurred
    """
    print(f'Cleaning "description" from {programid}...', end='')
    update_expr = 'REMOVE description'

    try:
        programs_table.update_item(
            Key={'program': programid},
            UpdateExpression=update_expr
        )
    except Exception as err:
        print(f'exception cleaning record: {err}')
        return False

    print('ok')
    return True


def update_program_name(programid: str, name: str) -> bool:
    """
    Updates the program_name for the given programid.
    :param programid: the program to be updated
    :param name: the name to be set
    :return: True if successful, False if not
    """
    print(f'Creating or updating name for {programid} as "{name}"...', end='')
    if not args.dry_run:
        update_expr = 'SET program_name = :n'
        expr_values = {
            ':n': name
        }

        try:
            programs_table.update_item(
                Key={'program': programid},
                UpdateExpression=update_expr,
                ExpressionAttributeValues=expr_values
            )
        except Exception as err:
            print(f'exception creating or updating record: {err}')
            return False

    print('ok')
    return True


def delete_program_record(programid: str) -> bool:
    """
    Deletes from dynamodb a program record for programs that no longer exist in PostgreSQL
    :param programid: to be deleted
    :return: True if the delete succeeded, False if an exception occurred.
    """
    print(f'Deleting program record for {programid}')
    if not args.dry_run:
        try:
            programs_table.delete_item(Key={'program': programid})
        except Exception as err:
            print(f'exception deleting record: {err}')
            return False

    print('ok')
    return True


def reconcile_programs() -> None:
    """
    Makes the dynamodb "programs" table match the PostgreSQL "projects" table.
    :return: None
    """
    # Actual from PostgreSQL.
    actual_programs = get_programs_from_postgres()
    print(actual_programs)
    n_updates = 0
    n_deletes = 0
    n_adds = 0

    # Track the programs we find, to know what needs to be added.
    cached_programs = []
    # Iterate the programs table, to find updates and deletions...
    for program_item in programs_table.scan()['Items']:
        programid = program_item.get('program')
        if programid not in actual_programs:
            print(f'Delete program id {programid}')
            delete_program_record(programid)
            n_deletes += 1
        else:
            cached_programs.append(programid)
            cached_name = program_item.get('program_name')
            has_obsolete_name = 'description' in program_item
            actual_name = actual_programs[programid]
            if cached_name != actual_name:
                print(f'Update name of {programid} from "{cached_name}" to "{actual_name}".')
                update_program_name(programid, actual_name)
                n_updates += 1
            if has_obsolete_name:
                clean_program_name(programid)

    # Additions.
    for actual_program in [p for p in actual_programs.keys() if p not in cached_programs]:
        print(f'Add {actual_program} with name "{actual_programs[actual_program]}".')
        # TODO: Implement it (Need the organization to do that.)
        n_adds += 1

    print(f'Add {n_adds}, delete {n_deletes}, update {n_updates}')


def main():
    global args
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--dry-run', '--dryrun', '-n', action='store_true', help='Don\'t update anything.')

    args = arg_parser.parse_args()

    reconcile_programs()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
