import argparse
import sys
from typing import List, Dict

from sqlalchemy import text

import acmMdHelper
from db import get_db_engine, get_db_connection

args = None

# List latest revisions:
# ~/s3/amplio-program-content % ls -1 */TB-Loaders/published/*.rev
# CARE-GH-COCOA/TB-Loaders/published/CARE-GH-COCOA-21-1-c.rev
# CARE-GH-W4C/TB-Loaders/published/CARE-GH-W4C-22-2-a.rev
# ...
# Then replace
#       # ([\w-]+)/TB-Loaders/published/([\w-]+)-(\w).rev
# with
#       ('$1', '$2', ['$3']),
program_deployments = [
    ('CARE-GH-COCOA', 'CARE-GH-COCOA-21-1', ['c']),
    ('CARE-GH-W4C', 'CARE-GH-W4C-22-2', ['a']),
    # ('CBCC-ANZ', 'CBCC-ANZ-18-3', ['a']),
    ('CBCC-AT', 'CBCC-AT-19-5', ['h']),
    ('CBCC-ATAY', 'CBCC-ATAY-19-1', ['h']),
    # ('CBCC-DEMO', 'CBCC-BUSARADEMO-19-1', ['a']),
    ('CBCC-RTI', 'CBCC-RTI-20-2', ['e']),
    ('CRS-GH-ICO', 'CRS-GH-ICO-22-1', ['b']),
    # ('DEMO-DL', 'DEMO-DL-21-4', ['t']),
    ('FAO-UG', 'FAO-UG-22-1', ['m']),
    ('LBG-APME_2A', 'LBG-APME_2A-20-1', ['c']),
    ('LBG-COVID19', 'LBG-COVID19-22-5', ['c']),
    # ('LBG-DEMO', 'LBG-DEMO-22-6', ['a']),
    ('LBG-ESOKO', 'LBG-ESOKO-20-1', ['d']),
    ('NANDOM-NAP-GH', 'NANDOM-NAP-GH-22-2', ['b']),
    # ('NG-SAHEL-DEMO', 'NG-SAHEL-DEMO-22-1', ['d']),
    ('SSA-ETH', 'SSA-ETH-22-1', ['d']),
    # ('TEST', 'TEST-22-4', ['m']),
    # ('UNICEF-2', 'UNICEF-2-2019-7', ['a']),
    ('UNICEF-CHPS', 'UNICEF-CHPS-19-3', ['a']),
    ('UNICEF-GH-CHPS', 'UNICEF-GH-CHPS-22-6', ['e']),
]

def read_sql(metadata_filename: str, programid: str) -> List[Dict]:
    """
    Loads from SQL the values corresponding to the given metadata file, for the
    given programid.
    :param metadata_filename: The metadata file for which SQL data is desired.
    :param programid: The programid of interest.
    :return: a list of dicts, one dict per SQL row.
    """
    load_params: Dict = acmMdHelper.LOAD_DATA_PARAMS.get(metadata_filename)
    command = text(
        f'SELECT * FROM {load_params.get("table", metadata_filename)} WHERE {load_params.get("programid")}=:programid;')
    with get_db_connection() as conn:
        result = conn.execute(command, {'programid': programid})
        data: List[Dict] = [dict(row) for row in result]
        return data


def make_csv_dict(load_params, csv_list: List[Dict]):
    """
    Given a list of dicts of CSV data, return a dict-of-dicts of CSV data.
    The key is a concatenation of the values of the primary key columns (because
    that must be unique). The sub-dict keys are translated to the corresponding
    SQL names.
    :param load_params: From acmMdHelper.LOAD_DATA_PARAMS, describing the metadata.
    :param csv_list: List of dicts of CSV data.
    :return: dict-of-dicts of CSV data
    """
    csv_to_sql = load_params['csv_to_sql']
    sql_to_csv = {v: k for k, v in csv_to_sql.items()}
    pk_cols = [sql_to_csv[x] for x in load_params['pk_columns']]
    # Make a key from the concatenation of the primary key columns, then {key: csv_record}. Skip fields
    # that aren't in csv_to_sql.
    csv_dict = {'::'.join(str(rec[c]) for c in pk_cols): {csv_to_sql[k]: str(v) for k, v in rec.items() if k in csv_to_sql} for rec in
                csv_list}
    return csv_dict


def make_sql_dict(load_params, sql_list: List[Dict]):
    """
    Given a list of dicts of SQL data, return a dict-of-dicts of CSV data.
    The key is a concatenation of the values of the primary key columns (because
    that must be unique).
    :param load_params: From acmMdHelper.LOAD_DATA_PARAMS, describing the metadata.
    :param csv_list: List of dicts of SQL data.
    :return: dict-of-dicts of SQL data
    """
    pk_cols = load_params['pk_columns']
    # Make a key from the concatenation of the primary key columns, then {key: sql_record}
    sql_dict = {'::'.join(str(sql_rec[c]) for c in pk_cols): {k: str(v) for k, v in sql_rec.items()} for sql_rec in
                sql_list}
    return sql_dict


def compare_data(metadata_filename: str, csv_list: List[Dict], sql_list: List[Dict]):
    """
    Compares data from a CSV with data from SQL. The data is cumulative in
    SQL, so after a CSV is loaded, it should all appear in SQL. The reverse
    is not true.
    :param metadata_filename: The metadata file for which data is being compared.
    :param csv_list: List of dicts of CSV data.
    :param sql_list: List of dicts of SQL data
    :return: True if the CSV data is all in the SQL data.
    """
    load_params: Dict = acmMdHelper.LOAD_DATA_PARAMS.get(metadata_filename)

    # Get the values from the CSV and from SQL in a compare-friendly format.
    csv_dict = make_csv_dict(load_params, csv_list)
    sql_dict = make_sql_dict(load_params, sql_list)

    # All of the records from the CSV should be in SQL. There may legitimately be other records.
    OK = True
    for csv_k, csv_v in csv_dict.items():
        sql_v = sql_dict.get(csv_k)
        if sql_v is None:
            OK = False
            print(f'Missing from SQL for {csv_k} in {metadata_filename}')
        elif sql_v != csv_v:
            OK = False
            print(f'Difference between CSV and SQL for {csv_k} in {metadata_filename}')

    return OK


def test_md_file(metadata_filename: str, programid: str, deployment: str, revisions: list[str]) -> bool:
    bucket = 'amplio-program-content'
    ok = True
    kwargs = {'programid': programid, 'deployment': deployment}
    for rev in revisions:
        key = f'{programid}/TB-Loaders/published/{deployment}-{rev}/metadata/{metadata_filename}.csv'
        event = {'Records': [{'s3': {'bucket': {'name': bucket},
                                     'object': {'key': key}}}]}
        kwargs['revision'] = rev
        csv_data = acmMdHelper.read_csv(bucket, key, metadata_filename, **kwargs)
        acmMdHelper.lambda_handler(event, None)
        sql_data = read_sql(metadata_filename, programid)
        ok = ok and compare_data(metadata_filename, csv_data, sql_data)

    return ok


def clean_tables(programid: str):
    with get_db_connection() as conn:
        for metadata_filename, load_params in acmMdHelper.LOAD_DATA_PARAMS.items():
            table = load_params.get("table", metadata_filename)
            command = text(f'DELETE FROM {table} WHERE {load_params.get("programid")}= :programid;')
            result = conn.execute(command, {'programid': programid})
            print(f'{result.rowcount} rows deleted from {table}')


def test2():
    all_ok = True
    for programid, deployment, revisions in program_deployments:
        print(f'Program {programid}...')
        program_ok = True
        clean_tables(programid=programid)
        for mdf in acmMdHelper.LOAD_DATA_PARAMS.keys():
            ok = test_md_file(mdf, programid=programid, deployment=deployment, revisions=revisions)
            program_ok = program_ok and ok
        print(f'{programid} compares {"OK" if program_ok else "WITH ERRORS"}')
        all_ok = all_ok and program_ok
    print(f'All programs compare {"OK" if all_ok else "WITH ERRORS"}')

def test3():
    bucket = 'amplio-program-content'
    all_ok = True
    for programid, deployment, revisions in program_deployments:
        print(f'Program {programid}...')
        program_ok = True
        for metadata_filename in acmMdHelper.LOAD_DATA_PARAMS.keys():
            key = f'{programid}/TB-Loaders/published/{deployment}-{revisions[0]}/metadata/{metadata_filename}.csv'
            kwargs = {'programid': programid, 'deployment': deployment, 'revision': revisions[0]}
            csv_data = acmMdHelper.read_csv(bucket, key, metadata_filename, **kwargs)
            sql_data = read_sql(metadata_filename, programid)
            ok = compare_data(metadata_filename, csv_data, sql_data)
            program_ok = program_ok and ok
        print(f'{programid} compares {"OK" if program_ok else "WITH ERRORS"}')
        all_ok = all_ok and program_ok
    print(f'All programs compare {"OK" if all_ok else "WITH ERRORS"}')

def test4():
    bucket = 'amplio-program-content'
    programid = 'TEST'
    deployment = 'TEST-22-4'
    revisions = ['p']

    print(f'Program {programid}...')
    program_ok = True
    for metadata_filename in acmMdHelper.LOAD_DATA_PARAMS.keys():
        key = f'{programid}/TB-Loaders/published/{deployment}-{revisions[0]}/metadata/{metadata_filename}.csv'
        kwargs = {'programid': programid, 'deployment': deployment, 'revision': revisions[0]}
        csv_data = acmMdHelper.read_csv(bucket, key, metadata_filename, **kwargs)
        sql_data = read_sql(metadata_filename, programid)
        ok = compare_data(metadata_filename, csv_data, sql_data)
        program_ok = program_ok and ok
    print(f'{programid} compares {"OK" if program_ok else "WITH ERRORS"}')

def test():
    clean_tables(programid='ZZZZTEST')
    all_ok = True
    for mdf in acmMdHelper.LOAD_DATA_PARAMS.keys():
        ok = test_md_file(mdf, programid='ZZZZTEST', deployment='ZZTEST-22-4', revisions=['m', 'n'])
        all_ok = all_ok and ok
        print(f'{mdf} compare {"OK" if ok else "WITH ERRORS"}')
    print(f'All files compare {"OK" if all_ok else "WITH ERRORS"}')


def main():
    global args
    arg_parser = argparse.ArgumentParser(description="Load deployment metadata into PostgreSQL.")
    arg_parser.add_argument('--verbose', action='count', default=0, help="More verbose output.")

    arg_parser.add_argument('--disposition', choices=['abort', 'commit'], default='abort',
                            help='Database disposition for the import operation.')

    arg_parser.add_argument('--test', action='store_true', default=False, help='Run the test function')
    arg_parser.add_argument('--test2', action='store_true', default=False,
                            help='Run the test function against all programs')
    arg_parser.add_argument('--test3', action='store_true', default=False,
                            help='Compare current SQL against all program metadata files.')
    arg_parser.add_argument('--test4', action='store_true', default=False,
                            help='Compare current SQL against TEST metadata files.')

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

    arg_parser.add_argument('programs', nargs='*', help='Program(s) to import and/or export.')

    arglist = None
    ######################################################
    #
    #
    # arglist = sys.argv[1:] + ['--db-host', 'localhost']
    #
    #
    ######################################################

    args = arg_parser.parse_args(arglist)

    get_db_engine(args=args)

    if args.test:
        test()
    elif args.test2:
        test2()
    elif args.test3:
        test3()
    elif args.test4:
        test4()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
