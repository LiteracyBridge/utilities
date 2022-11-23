#!/usr/bin/env bash
"exec" "/home/ubuntu/bin/csvInsertEnv/bin/python3" "/home/ubuntu/bin/csvInsert.py" "$@"
import argparse
import base64
import csv
import json
import re
import sys
import time
from contextlib import contextmanager
from os.path import expanduser
from pathlib import Path
from typing import List, Tuple, Dict, Optional

import boto3 as boto3
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.engine import Engine
from sqlalchemy.sql import TableClause

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
def get_db_engine() -> Engine:
    global _engine

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
    return table_def


# noinspection SqlDialectInspection
def make_insert(metadata):
    columns = [x.name for x in metadata.columns]
    # noinspection SqlNoDataSourceInspection
    command = text(f'INSERT INTO {metadata.name} ({",".join(columns)}) '
                   f'VALUES (:{",:".join(columns)}) '
                   'ON CONFLICT DO NOTHING;')
    return command


# Recognizes (+1.234,-56.789)
COORD_RE = re.compile(r'"?\((?P<lat>[+-]?[0-9.]+),(?P<lon>[+-]?[0-9.]+)\)"?')




def insert_file(csv_path: Path, command, columns:List[str], connection, c2ll: Optional[List[Tuple]]):
    def tr_c2ll(row: Dict) -> Dict:
        if c2ll:
            for coord, lat, lon in c2ll:
                if c_val := row.get(coord):
                    if match := COORD_RE.match(c_val):
                        row[lat] = match['lat']
                        row[lon] = match['lon']
                else:
                    # Empty 'coordinates' field; must be inserted as None to be parseable.
                    row[coord] = None
        row = row | {c:None for c in columns if c not in row}
        for c in columns:
            if c not in row:
                row[c] = None
        return row

    with csv_path.open(newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = [tr_c2ll(row) for row in reader]

    if len(rows) > 0:
        result = connection.execute(command, rows)
        print(f'{result.rowcount} rows inserted from {str(csv_path)}.')
    else:
        print(f'File {str(csv_path)} is empty.')


def insert_files(paths: List[Path], table: str, separate: bool = False, verbose: int = 0, dry_run: bool = False,
                 c2ll: List[str] = None):
    """
    Insert the contents of one or more .csv files into the given table.
    :param paths: a list of Path() objects identifying the .csv files to be in serted.
    :param table: the name of the table into which the contents should be inserted.
    :param separate: if True, insert each file as a separate transaction.
    :param verbose: how verbose to be. Higher number means more verbose.
    :param dry_run: if True, rollback the transaction rather than committing it. Allows
        checking the contents for proper form.
    :param c2ll: "Coordinates-To-Latitude-Longitude": a list of tuples where each
        tuple consists of three names. The first name is a "coordinate" field
        ("(1.2354,-10.0987)"), and the next two are the names of latitude
        and longitude fields. The coordinate field is parsed into latitude and
        longitude. What gets inserted depends on the table schema, but this
        allows inserting a coordinate field as latitude and longitude.
        If present, empty coordinate fields are transformed into None, which inserts
        as null. Otherwise the sql alchemy & pg8000 won't be able to insert an
        empty coordinate.
    :return: None
    """
    if (metadata := get_table_metadata(table)) is None:
        raise Exception(f'Table {table} does not seem to exist.')
    columns:List[str] = [x.name for x in metadata.columns]
    print(metadata)
    commit = not dry_run
    command = make_insert(metadata)

    remaining = [x for x in paths]
    while len(remaining) > 0:
        with get_db_connection() as conn:
            transaction = conn.begin()
            while len(remaining) > 0:
                path = remaining.pop(0)
                insert_file(path, command, columns, conn, c2ll)
                if separate:
                    break
            if commit:
                transaction.commit()
                print(f'Changes commited for {table}')
            else:
                transaction.rollback()
                print(f'Changes rolled back for {table}')


def parse_c2ll(args) -> Optional[List[Tuple]]:
    c2ll = []
    if args.c2ll is not None:
        if len(args.c2ll) % 3 != 0:
            print('--c2ll must provide sets of three names: from->to,to', file=sys.stderr)
            sys.exit(1)
        if len(args.c2ll) == 0:
            c2ll = [('coordinates', 'latitude', 'longitude')]
        else:
            c2ll = [(args.c2ll[x], args.c2ll[x + 1], args.c2ll[x + 2]) for x in range(0, len(args.c2ll), 3)]
    return c2ll


def go(args):
    global _args
    _args = args

    get_db_engine()
    c2ll = parse_c2ll(_args)
    insert_files(_args.files, table=_args.table, verbose=_args.verbose, separate=args.separate, dry_run=_args.dry_run, c2ll=c2ll)


class StorePathAction(argparse.Action):
    """
    An argparse.Action to store a Path object. A leading ~ is expanded to the user's home directory.
    """

    @staticmethod
    def _expand(v: str) -> Optional[str]:
        """
        Does the work of expanding.
        :param v: A string, possibly with a leading ~ to be expanded ot user's home directory.
        :return: A Path object that encapsulates the given path. Note that there is no guarantee of
            any actual file system object at that path.
        """
        return v and Path(expanduser(v))  # 'None' if v is None, otherwise Path(expanduser(v))

    def __init__(self, option_strings, dest, default=None, **kwargs):
        if 'glob' in kwargs: self._glob = kwargs.get('glob', False); del kwargs['glob']
        super(StorePathAction, self).__init__(option_strings, dest, default=self._expand(default), **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        if self._glob and not isinstance(values, list): raise TypeError("The 'glob' option requires that the value is a list; did you use 'nargs'?")
        if not isinstance(values, list):
            values = self._expand(values)
        else:
            result = []
            for value in values:
                path=self._expand(value)
                path_str = str(path)
                if self._glob and ('*' in path_str or '?' in path_str):
                    import glob
                    result.extend([Path(x) for x in glob.glob(path_str)])
                else:
                    result.append(path)
            values = result
        setattr(namespace, self.dest, values)


def main():
    global args

    arg_parser = argparse.ArgumentParser(description="Import CSV into SQL, with column matching.")
    arg_parser.add_argument('--verbose', action='count', default=0, help="More verbose output.")

    arg_parser.add_argument('--table', action='store', help='Table into which to insert.')
    arg_parser.add_argument('--separate', '-s', action='store_true', help='Insert each file independently.')

    arg_parser.add_argument('--c2ll', action='store', nargs='*',
                            help='Convert a coordinate field to latitude,longitude.')

    arg_parser.add_argument('--dry-run', '--dryrun', '-n', action='store_true',
                            help='Dry run, do not update (abort transaction at end).')

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

    arg_parser.add_argument('--files', nargs='*', action=StorePathAction, glob=True, help='Files(s) to insert.')

    arglist = None
    ######################################################
    #
    #
    #arglist = sys.argv[1:] + ['--db-host', 'localhost']
    #
    #
    ######################################################

    args = arg_parser.parse_args(arglist)

    go(args)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
