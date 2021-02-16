import json
from typing import Dict

import boto3
import pg8000
from botocore.exceptions import ClientError

connction_overrides = ['host', 'port', 'user', 'password', 'database']
connection_params = {}

# This will be a connection to the PostgreSQL database
db_connection = None


def initialize_postgres(_params: Dict[str, str]) -> None:
    global connection_params
    connection_params = _params


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


def get_db_connection():
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


def check_for_postgresql_project(program_id) -> bool:
    """
    Checks to see if the project exists in the PostgerSQL projects table.
    :param program_id: to be checked.
    :return: True if there is no existing project record, False if there already is one.
    """
    print(f"Looking for program '{program_id}' in PostgreSQL...", end='')

    connection = get_db_connection()
    cur = connection.cursor()
    cur.execute('SELECT projectcode FROM projects;')
    rows = [x[0] for x in cur]

    if program_id in rows:
        print('\n  {} exists in PostgreSQL projects table'.format(program_id))
        return False
    print('ok')
    return True


def populate_postgresql(program_id: str, comment: str) -> bool:
    """
    Adds the project's row to the projects table in PostgreSQL
    :param program_id: to be added
    :return: True if successful, False if not
    """
    print(f"Adding '{program_id}' to PostgreSQL projects table...", end='')
    connection = get_db_connection()
    cur = connection.cursor()
    cur.execute('SELECT MAX(id) FROM projects;')
    rows = [x[0] for x in cur]
    new_id = max(rows) + 1

    cur.execute("INSERT INTO projects (id, projectcode, project, active) VALUES  (%s, %s, %s, True);",
                (new_id, program_id, comment))
    num = cur.rowcount
    connection.commit()

    if num == 1:
        print('ok')
        return True

    print(f'Unexpected row count: {num}')
    return False
