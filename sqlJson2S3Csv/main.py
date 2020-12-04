import argparse
import io
import json
from typing import List, Dict, Any

import boto3
# Get the user name and password that we need to sign into the SQL database. Configured through AWS console.
import pg8000
from botocore.exceptions import ClientError

# s3 and projspec, dashboard buckets
s3_client = boto3.client('s3')
projspec_bucket: str = 'amplio-progspecs'

args = {}
db_connection = None


def get_postgresql_secret() -> dict:
    """
    Retrieve the connection parameters for the dashboard database.
    :return: a dict of the parameters.
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


# Make a connection to the SQL database
def get_db_connection():
    """
    Opens a connection to teh database. Caches the connection for future calls.
    :return: the connection
    """
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


def read_postgresql_contents(program: str) -> Dict[int, List]:
    """
    Reads rows from the 'content' table, for the given program.
    :param program: the program of interest.
    :return: a dict { int : list } where the key is deployment and the list is playlists in the deployment
    """
    connection = get_db_connection()
    cur = connection.cursor()
    # noinspection SqlDialectInspection,SqlNoDataSourceInspection
    cur.execute("SELECT * FROM content WHERE projectcode=%s;", args=[program])
    col_names = [k[0] for k in cur.description]
    deployment_ix = col_names.index('deployment')
    content_ix = col_names.index('content')

    contents = {}
    for row in cur:
        deployment = int(row[deployment_ix])
        content = row[content_ix]
        contents[deployment] = content
        print(contents)

    return contents


def build_csv(contents: Dict[int, List]) -> str:
    """
    Given the values from the content table in the dashboard database, create the corresponding .csv file.
    :param contents: from the database
    :return: the .csv content as a single string.
    """
    # for each column of the .csv, what is it called, and where does it come from.
    column_source = [('deployment_num', 'deployment', 'deployment_num'),
                     ('playlist_title', 'playlist', 'title'),
                     ('message_title', 'message', 'title'),
                     ('key_points', 'message', 'key_point'),
                     ('languagecode', 'message', lambda msg: ','.join(msg.get('languages', []))),
                     ('variant', 'message', 'variant'),
                     ('default_category', 'message', 'default_category'),
                     ('sdg_goals', 'message', 'sdg_goal'),
                     ('sdg_targets', 'message', 'sdg_target')
                     ]

    def print_row(output, **kwargs) -> None:
        def enquote(val: Any) -> str:
            val = str(val)
            if ',' in val or "'" in val:
                return f'"{val}"'
            return val

        def get_value(t) -> str:
            obj = kwargs.get(t[1], {})
            val = obj.get(t[2], None)
            if callable(val):
                value = val(obj)
            elif val is not None:
                value = str(val)
            else:
                value = ''
            return enquote(val)

        row_data = []
        for col in column_source:
            row_data.append(get_value(col))
        print(','.join(row_data), file=output)

    def print_heading(output):
        print(','.join([c[0] for c in column_source]), file=output)

    with io.StringIO() as output:
        deployments = sorted(list(contents.keys()))
        print_heading(output)
        for deployment in deployments:
            depl_contents = contents[deployment]
            for playlist in depl_contents:
                for message in playlist.get('messages', []):
                    print_row(output, deployment={'deployment_num': deployment}, playlist=playlist, message=message)
        data = output.getvalue()
    return data


def write_s3_contents(program: str, contents: Dict[int, List]) -> None:
    data = build_csv(contents)
    key = f'{program}/content.csv'
    put_result = s3_client.put_object(Body=data, Bucket=projspec_bucket, Key=key)


def main():
    global args
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('program', help='The program for which to copy content.')
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

    contents = read_postgresql_contents(args.program)

    write_s3_contents(args.program, contents)


if __name__ == '__main__':
    main()
