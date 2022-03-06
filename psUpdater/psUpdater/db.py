import base64
import json
import time
from contextlib import contextmanager
from typing import Optional, Any

import boto3 as boto3
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.engine import Engine

_engine: Optional[Engine] = None
_args: Optional[Any] = None


# Get the user name and password that we need to sign into the SQL database. Configured through AWS console.
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
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if _args and _args.verbose >= 2:
            print('    Exception getting credentials: {}, elapsed: {}'.format(e.response['Error']['code'],
                                                                              time.time() - start))

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


def _ensure_content_view(engine=None):
    v1 = '''
        create temp view sd_goals as 
            select section as sdg_id, section::text as label, label as description from sustainable_development_goals;
        '''
    v2 = '''
        create temp view sd_targets as 
            select sdg.id as sdg_id, subsection as target_id, sdg.section::text || '.' || target.subsection::text as label, target.label as description
                from sustainable_development_goals sdg
                join sustainable_development_targets target
                  on target.goal_id = sdg.id ;
    '''
    v3 = '''
        create temp view content as 
            select pl.program_id as project
                    ,depl.id as deployment_id
                    ,depl.deploymentnumber::integer as deployment_num
                    ,pl.title as playlist_title
                    ,pl.position as playlist_pos
                    ,pl.id as playlist_id
                    ,msg.title as message_title
                    ,msg.position as message_pos
                    ,msg.id as message_id
                    ,msg.key_points
                    ,scat.fullname as default_category
                    ,msg.format
                    ,pl.audience
                    ,msg.variant
                    ,sdg.sdg_goal_id as sdg_goals
                    ,sdt.sdg_target_id as sdg_targets
                    ,STRING_AGG(distinct ml.language_code, ',') as languagecode

            from playlists pl
            join deployments depl
              on pl.deployment_id = depl.id
            left outer join messages msg
              on pl.program_id = msg.program_id AND msg.playlist_id = pl.id
            left outer join sdg_goals sdg
              on msg.sdg_goal_id = sdg.sdg_goal_id
            left outer join sdg_targets sdt
              on msg.sdg_target = sdt.sdg_target and msg.sdg_goal_id = sdt.sdg_goal_id
            left outer join message_languages ml
              on ml.message_id = msg.id
            left outer join supportedcategories scat
              on msg.default_category_code = scat.categorycode

            group by pl.program_id
                    ,depl.deploymentnumber
                    ,depl.id
                    ,pl.position
                    ,pl.title
                    ,pl.id
                    ,msg.position
                    ,msg.title
                    ,msg.id
                    ,msg.key_points
                    ,scat.fullname
                    ,msg.format
                    ,pl.audience
                    ,msg.variant
                    ,sdg.sdg_goal_id
                    ,sdt.sdg_target_id


            order by pl.program_id, depl.deploymentnumber, pl.position, msg.position;
    '''
    v3_new = '''
        create temp view content as 
            select pl.program_id as project
                    ,depl.deploymentnumber::integer as deployment_num
                    ,pl.title as playlist_title
                    ,pl.position as playlist_pos
                    ,pl.id as playlist_id
                    ,msg.title as message_title
                    ,msg.position as message_pos
                    ,msg.id as message_id
                    ,msg.key_points
                    ,scat.fullname as default_category
                    ,msg.format
                    ,msg.audience
                    ,msg.variant
                    ,sdg.sdg_goal_id as sdg_goals
                    ,sdt.sdg_target_id as sdg_targets
                    ,msg.languages as languagecode

            from playlists pl
            join deployments depl
              on pl.deploymentnumber = depl.deploymentnumber AND pl.program_id=depl.project
            left outer join messages msg
              on pl.program_id = msg.program_id AND msg.playlist_id = pl.id
            left outer join sdg_goals sdg
              on msg.sdg_goal_id = sdg.sdg_goal_id
            left outer join sdg_targets sdt
              on msg.sdg_target = sdt.sdg_target and msg.sdg_goal_id = sdt.sdg_goal_id
            left outer join supportedcategories scat
              on msg.default_category_code = scat.categorycode

            group by pl.program_id
                    ,depl.deploymentnumber
                    ,pl.position
                    ,pl.title
                    ,pl.id
                    ,msg.position
                    ,msg.title
                    ,msg.id
                    ,msg.key_points
                    ,scat.fullname
                    ,msg.format
                    ,msg.audience
                    ,msg.variant
                    ,sdg.sdg_goal_id
                    ,sdt.sdg_target_id
                    ,msg.languages

            order by pl.program_id, depl.deploymentnumber, pl.position, msg.position;
    '''
    global _engine
    if engine is None:
        engine = _engine;
    is_new = table_has_column('messages', 'audience', engine=engine)
    content_view = v3_new if is_new else v3
    try:
        with engine.connect() as conn:
            result = conn.execute(text(content_view))
        print(f'(Re)established content view, is_new: {is_new}.')
    except:
        pass

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
        _ensure_content_view(engine)

# lazy initialized db connection

# Make a connection to the SQL database
def get_db_engine(args=None) -> Engine:
    global _engine, _args

    if _engine is not None:
        print('Reusing db engine.')
    elif _engine is None:
        _args = args
        secret = _get_secret()

        parms = {'database': 'dashboard', 'user': secret['username'], 'password': secret['password'],
                 'host': secret['host'], 'port': secret['port']}
        if args:
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

        # dialect + driver: // username: password @ host:port / database
        # postgresql+pg8000://dbuser:kx%25jj5%2Fg@pghost10/appdb
        engine_connection_string = 'postgresql+pg8000://{user}:{password}@{host}:{port}/{database}'.format(**parms)
        _engine = create_engine(engine_connection_string, echo=False)

        _ensure_content_view()

    return _engine


_column_cache = {}


def table_has_column(table: str, column: str, engine=None) -> bool:
    global _column_cache, _engine
    if engine is None:
        engine = _engine
    if table in _column_cache:
        columns = _column_cache.get(table)
    else:
        columns = []
        try:
            table_meta = MetaData(engine)
            table_def = Table(table, table_meta, autoload=True)
            columns = [c.name for c in table_def.columns]
        except:
            pass
        _column_cache[table] = columns
    return column in columns
