import argparse
import base64
import json
from contextlib import contextmanager
import time

from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.engine import Engine
from sqlalchemy.sql import TableClause
# noinspection PyUnresolvedReferences
import pg8000

import boto3
from botocore.exceptions import ClientError

# noinspection PyTypeChecker
_args: argparse.Namespace = None
_engine = None


def _get_secret() -> dict:
    secret_name = "lb_stats_access2"
    region_name = "us-west-2"

    if _args and _args.verbose >= 2:
        print('    Getting credentials for database connection. v2.')
    start = time.time()

    # Create a Secrets Manager client
    try:
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )
    except Exception as e:
        print('    Exception getting session client: {}, elapsed: {}'.format(str(e), time.time() - start))
        raise e

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        if _args and _args.verbose >= 2:
            print('    Exception getting credentials: {}, elapsed: {}'.format(e.response['Error']['code'],
                                                                              time.time() - start))
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
        engine = _engine
    kwargs = {}
    if close_with_result is not None:
        kwargs['close_with_result'] = close_with_result
    try:
        with engine.connect(**kwargs) as conn:
            yield conn
    finally:
        pass
    #     _ensure_content_view(engine)


# lazy initialized db connection

# Make a connection to the SQL database
def get_db_engine(args: argparse.Namespace = None) -> Engine:
    global _engine, _args

    if args is not None and _args is None:
        _args = args
    if _engine is not None:
        print('Reusing db engine.')
    elif _engine is None:
        secret = _get_secret()

        parms = {'database': 'dashboard', 'user': secret['username'], 'password': secret['password'],
                 'host': secret['host'], 'port': secret['port']}
        for prop in ['host', 'port', 'user', 'password', 'database']:
            if hasattr(_args, f'db_{prop}'):
                if (val := getattr(_args, f'db_{prop}')) is not None:
                    parms[prop] = val

        # dialect + driver: // username: password @ host:port / database
        # postgresql+pg8000://dbuser:kx%25jj5%2Fg@pghost10/appdb
        engine_connection_string = 'postgresql+pg8000://{user}:{password}@{host}:{port}/{database}'.format(**parms)
        _engine = create_engine(engine_connection_string, echo=False)

    return _engine

def get_table_metadata(table: str):
    # noinspection PyTypeChecker
    table_def = None
    try:
        engine = get_db_engine()
        table_meta = MetaData(engine)
        table_def: TableClause = Table(table, table_meta, autoload=True)
    except Exception as ex:
        print(ex)

    #     "tbdeployments_pkey" PRIMARY KEY, btree (talkingbookid, deployedtimestamp)
    return table_def
