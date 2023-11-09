import base64
import csv
import io
import json
from contextlib import contextmanager
from typing import Tuple, List, Any, Optional, Dict

import boto3
import pg8000
from botocore.exceptions import ClientError

from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.engine import Engine
from sqlalchemy.sql import TableClause

_engine: Optional[Engine] = None
_args: Optional[Any] = None


db_connection = None


# Get the user name and password that we need to sign into the SQL database. Configured through AWS console.
def _get_secret():
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
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            result = decoded_binary_secret

    # Your code goes here.
    return result


@contextmanager
def get_db_connection(*, close_with_result=None, engine=None):
    """
    A helper to get a db connection and re-establish the 'content' view after a commit or abort.
    :param close_with_result: If present, passed through to the engine.connect() call.
    :param engine: Optional engine to use for the call. Default is _engine.
    :return: Provides a Connection through a context manager.
    """
    if engine is None:
        engine = get_db_engine()
    kwargs = {}
    if close_with_result is not None:
        kwargs['close_with_result'] = close_with_result
    try:
        with engine.connect(**kwargs) as conn:
            yield conn
    finally:
        pass


def set_db_args(args):
    global _args
    _args = args


# lazy initialized db connection

# Make a connection to the SQL database
def get_db_engine(args=None) -> Engine:
    global _engine, _args

    if _engine is not None:
        print('Reusing db engine.')
    elif _engine is None:
        if _args is None:
            _args = args
        secret = _get_secret()

        parms = {'database': 'dashboard', 'user': secret['username'], 'password': secret['password'],
                 'host': secret['host'], 'port': secret['port']}
        for prop in ['host', 'port', 'user', 'password', 'database']:
            if hasattr(_args, f'db_{prop}'):
                if (val := getattr(_args, f'db_{prop}')) is not None:
                    parms[prop] = val

        # dialect + driver: // username: password @ host:port / database
        # postgresql+pg8000://dbuser:kx%25jj5%2Fg@pghost10/appdb
        engine_connection_string = 'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'.format(**parms)
        _engine = create_engine(engine_connection_string, echo=False)

    return _engine


def query_to_csv(query: str, params: Dict = None) -> Tuple[str, int]:
    file_like = io.StringIO()
    writer = csv.writer(file_like, quoting=csv.QUOTE_MINIMAL)
    num_rows = 0

    with get_db_connection() as conn:
        db_result = conn.execute(text(query), params)
        columns = list(db_result.keys())
        writer.writerow(columns)
        for record in db_result:
            num_rows += 1
            writer.writerow(record)
    return file_like.getvalue(), num_rows


def query_to_json(query: str, name_map=None, params: Dict = None) -> Tuple[List[Any], int]:
    with get_db_connection() as conn:
        db_result = conn.execute(text(query), params)
        columns = list(db_result.keys())
        if name_map:
            columns = [name_map.get(column, column) for column in columns]
        result = []
        for record in db_result:
            result.append(dict(zip(columns, record)))
    return result, len(result)
