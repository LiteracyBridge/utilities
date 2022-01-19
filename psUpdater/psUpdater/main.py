import argparse
import sys
from os.path import expanduser
from pathlib import Path
from typing import Any, List, Optional

from sqlalchemy.engine import Engine

import XlsExporter
import XlsImporter
import db
from SpecCompare import SpecCompare

engine: Optional[Engine] = None

cursor = None

args: Optional[argparse.Namespace] = None
# @formatter:off
NON_PROGRAMS = ['ARM-DEMO', 'CARE-ETH']

# programs:
PROGRAMS_TABLE_NO_SPEC = ['CARE-ETH-BIRHAN', 'ILC-MW-R2R', 'LANDESA-LR-LVG', 'SSA-ETH', 'UNICEF-KE-BIO', 'VSO-TALK-III']

# specs:
SPEC_NO_PROGRAMS_TABLE = ['BUSARA', 'CARE-BGD-JANO', 'CARE-ETH', 'CARE-ETH-BOYS', 'CARE-ETH-GIRLS',
    'CARE-GH-COCOA', 'CARE-HTI', 'CBCC-ANZ', 'CBCC-AT', 'CBCC-ATAY', 'CBCC-RTI', 'ITU-NIGER', 'LANDESA-LR',
    'LBG-APME_2A', 'LBG-COVID19', 'LBG-ESOKO', 'NANDOM-NAP-GH', 'TS-NG-BWC', 'UNFM', 'UNICEF-2', 'UNICEF-ETH-P1',
    'UNICEF-GH-CHPS', 'UNICEFGHDF-MAHAMA', 'WKW-TLE'
]

# both:
PROGRAMS_TABLE_AND_SPEC = ['MC-NGA', 'UNICEF-CHPS']  # , 'VSO-TALK']

# tests:
TEST_PROGRAMS = ['AMPLIO-ED', 'DEMO', 'DEMO-DL', 'INSTEDD', 'LBG-DEMO', 'SANDBOX', 'TEST', 'TPORTAL', 'XTEST',
    'XTEST-2', 'XTEST-BB-1', 'XTEST-BB-2', 'XTEST-BB-3', 'XTEST-BB-4', 'XTEST-BB-5', 'XTEST-S3']

PROJECTS_TABLE = ['AMPLIO-ED', 'ARM-DEMO', 'BUSARA', 'CARE', 'CARE-BGD-JANO', 'CARE-ETH', 'CARE-ETH-BIRHAN',
    'CARE-ETH-BOYS', 'CARE-ETH-GIRLS', 'CARE-GH-COCOA', 'CARE-HTI', 'CBCC', 'CBCC-ANZ', 'CBCC-AT', 'CBCC-ATAY',
    'CBCC-RTI', 'DEMO', 'DEMO-DL', 'ILC-MW-R2R', 'INSTEDD', 'ITU-NIGER', 'LANDESA-LR', 'LANDESA-LR-LVG', 'LBG-APME_2A',
    'LBG-COVID19', 'LBG-DEMO', 'LBG-ESOKO', 'LBG-FL', 'MC-NGA', 'MEDA', 'NANDOM-NAP-GH', 'SANDBOX', 'SSA-ETH', 'TEST',
    'TEST-TS-NG', 'TPORTAL', 'TS-NG-BWC', 'TUDRIDEP', 'UNFM', 'UNICEF-2', 'UNICEF-CHPS', 'UNICEF-ETH-P1',
    'UNICEF-GH-CHPS', 'UNICEFGHDF-MAHAMA', 'UNICEF-KE-BIO', 'Unknown', 'UWR', 'VSO-TALK', 'VSO-TALK-III', 'WKW-TLE',
    'XTEST', 'XTEST-2', 'XTEST-BB-1', 'XTEST-BB-2', 'XTEST-BB-3', 'XTEST-BB-4', 'XTEST-BB-5', 'XTEST-S3'
]

PROGSPEC_BUCKET = 'amplio-progspecs'
# @formatter:on

# noinspection SqlDialectInspection,SqlNoDataSourceInspection


# Given a project or acm name, return the acm directory name.
def cannonical_acm_dir_name(project: str, is_dbx: bool = True) -> str:
    acmdir = project.upper()
    if is_dbx and acmdir[0:4] != 'ACM-':
        # There isn't an 'ACM-' prefix, but should be.
        acmdir = 'ACM-' + acmdir
    elif not is_dbx and acmdir[0:4] == 'ACM-':
        # There is an 'ACM-' prefix, but there shouldn't be.
        acmdir = acmdir[4:]
    return acmdir


def read_spec_from_s3(programid: str) -> Optional[bytes]:
    key = f'{programid}/program_spec.xlsx'
    from botocore.client import BaseClient
    import boto3
    s3: Optional[BaseClient] = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=PROGSPEC_BUCKET, Key=key)
        obj_data = obj.get('Body').read()
        return obj_data
    except Exception as ex:
        pass
    return None


def _get_path(program: str, file: str) -> Path:
    """
    Get the path to a config or program spec file. If the '--dbx' option is given, returns a Path to the
    file in the ~/Dropbox/ACM-{program}/ directory. If the '--s3' option is given, returns a Path to the
    file in the ~/Amplio directory. If neither is given, returns {args.directory}/{program}/{file}.
    :param program: Program for which the path is desired.
    :param file: File for which the path is desired.
    :return: Path to the file.
    """
    global args
    if args.dbx:
        acmdir = cannonical_acm_dir_name(program)
        if file == 'config.properties':
            result = Path(args.dropbox, acmdir, file)
        else:
            result = Path(args.dropbox, acmdir, 'programspec', file)
    elif args.s3:
        acmdir = cannonical_acm_dir_name(program, is_dbx=False)
        if file == 'config.properties':
            result = Path(args.amplio, 'acm-dbs', acmdir, file)
        else:
            result = Path(args.amplio, 'acm-dbs', acmdir, 'programspec', file)
    else:
        result = Path(args.directory, program, file)
    return result


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
        super(StorePathAction, self).__init__(option_strings, dest, default=self._expand(default), **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        values = [self._expand(v) for v in values] if isinstance(values, list) else self._expand(values)
        setattr(namespace, self.dest, values)


def look_for_program_spec(programid: str) -> bytes:
    path = Path(args.dropbox, cannonical_acm_dir_name(programid, is_dbx=True), 'programspec', 'program_spec.xlsx')
    data = None
    if path.exists():
        with path.open('rb') as input_file:
            data = input_file.read()
    if data is None or len(data) == 0:
        path = Path(args.amplio, cannonical_acm_dir_name(programid, is_dbx=False), 'programspec', 'program_spec.xlsx')
        if path.exists():
            with path.open('rb') as input_file:
                data = input_file.read()
    if data is None or len(data) == 0:
        data = read_spec_from_s3(programid)
    return data


def _do_trial_publish(targets: List[str], path: Path = None, bucket: str = None) -> None:
    for programid in targets:
        output_path = Path(path, programid)
        if not output_path.exists():
            output_path.mkdir(exist_ok=True, parents=True)
        print(f'Processing {programid} for export')
        exporter = XlsExporter.Exporter(programid, engine)
        ok, errors = exporter.do_export(output_path=output_path, bucket=bucket)
        if not ok:
            for e in errors:
                print(e)


def _do_trial_import(targets: List[str], commit: bool = False) -> None:
    for programid in targets:
        print(f'Processing {programid} for import')
        data = look_for_program_spec(programid)
        importer = XlsImporter.Importer(programid)
        ok, errors = importer.do_import('all', data=data, engine=engine, commit=commit)
        if not ok:
            print(*errors, sep='\n')


def do_programs_table_no_spec():
    # programs
    global args
    commit = args.disposition == 'commit'
    targets = [x for x in PROGRAMS_TABLE_NO_SPEC if len(args.programs) == 0 or x in args.programs]
    # _do_trial_import(targets, commit=commit)
    _do_trial_publish(targets, path=args.directory, bucket=PROGSPEC_BUCKET)


def do_spec_no_programs_table():
    # specs
    global args
    commit = args.disposition == 'commit'
    targets = [x for x in SPEC_NO_PROGRAMS_TABLE if len(args.programs) == 0 or x in args.programs]
    # _do_trial_import(targets, commit=commit)
    _do_trial_publish(targets, path=args.directory, bucket=PROGSPEC_BUCKET)


def do_programs_table_and_spec():
    # both
    global args
    commit = args.disposition == 'commit'
    targets = [x for x in PROGRAMS_TABLE_AND_SPEC if len(args.programs) == 0 or x in args.programs]
    # _do_trial_import(targets, commit=commit)
    _do_trial_publish(targets, path=args.directory, bucket=PROGSPEC_BUCKET)


def do_test_programs():
    # tests
    global args
    commit = args.disposition == 'commit'
    targets = [x for x in TEST_PROGRAMS if len(args.programs) == 0 or x in args.programs]
    _do_trial_import(targets, commit=commit)
    _do_trial_publish(targets, path=args.directory, bucket=PROGSPEC_BUCKET)


def just_go_for_it():
    # go-for-it
    print('Programs in programs table without spec in S3')
    do_programs_table_no_spec()
    print('\nPrograms with spec in S3, but no programs table entry')
    do_spec_no_programs_table()
    print('\nPrograms with spec in S3 and programs table entry')
    do_programs_table_and_spec()
    print('\nTest programs')
    do_test_programs()


def do_group():
    global args
    which = args.group
    if which == 'programs':
        do_programs_table_no_spec()
    elif which == 'specs':
        do_spec_no_programs_table()
    elif which == 'both':
        do_programs_table_and_spec()
    elif which == 'tests':
        do_test_programs()
    elif which == 'go-for-it':
        just_go_for_it()
    else:
        raise Exception('Unexpected value for group')


def do_export(programs: List[str]) -> None:
    global engine, args
    for program in programs:
        print(f'Exporting program spec for {program}.')
        output_path = args.directory
        if '{' in str(output_path):
            name = str(output_path).format(program_id=program)
            output_path = Path(name)
        if not output_path.exists():
            output_path.mkdir(parents=True, exist_ok=True)
        exporter = XlsExporter.Exporter(program, engine)
        bucket = 'amplio-progspecs' if args.s3 else None
        ok, errors = exporter.do_export(output_path=output_path, bucket=bucket)
        if not ok:
            for e in errors:
                print(e)


def do_import(programs: List[str], what: Any, commit: bool = True) -> None:
    global engine, args
    for program in programs:
        print(f'Importing program spec for {program}.')
        if args.file:
            path = args.file
        else:
            path: Path = _get_path(program, 'program_spec.xlsx')
        importer = XlsImporter.Importer(program)

        with path.open('rb') as input_file:
            data = input_file.read()

        ok, errors = importer.do_import(what, data=data, engine=engine, commit=commit)
        if not ok:
            print(*errors, sep='\n')


def do_copy():
    global args, engine
    if len(args.copy) != 2:
        raise Exception('Must provide exactly two arguments to copy.')
    if len(args.programs) != 1:
        raise Exception('Copy only works with a single program at a time.')
    in_spec: Path = args.copy[0]
    out_spec: Path = args.copy[1]
    program = args.programs[0]

    with in_spec.open('rb') as input_file:
        data = input_file.read()
        importer = XlsImporter.Importer(program)
        ok, issues = importer.do_open(data=data)
        if len(issues) > 0:
            print(*issues, sep='\n')
        if ok:
            exporter = XlsExporter.Exporter(program, engine, program_spec=importer.program_spec)
            ok, issues = exporter.do_save(path=out_spec)
            if len(issues) > 0:
                print(*issues, sep='\n')


def do_compare(comparees):
    global args
    importer_a = XlsImporter.Importer(args.programs[0])
    a = None
    with comparees[0].open('rb') as input_file:
        data = input_file.read()
        if importer_a.do_open(data=data):
            a = importer_a.program_spec
    importer_b = XlsImporter.Importer(args.programs[0])
    b = None
    with comparees[1].open('rb') as input_file:
        data = input_file.read()
        if importer_b.do_open(data=data):
            b = importer_b.program_spec
    if a and b:
        comp = SpecCompare(a, b)
        diffs = comp.diff()
        if diffs:
            for d in diffs:
                print(d)


def _init() -> None:
    global engine, args
    engine = db.get_db_engine(args=args)


def main():
    global args
    default_dropbox_directory = '~/Dropbox (Amplio)'

    arg_parser = argparse.ArgumentParser(description="Synchronize published content to S3.")
    arg_parser.add_argument('--verbose', action='count', default=0, help="More verbose output.")

    arg_parser.add_argument('--dropbox', action=StorePathAction, default=expanduser(default_dropbox_directory),
                            help='Dropbox directory (default is ' + default_dropbox_directory + ').')
    arg_parser.add_argument('--amplio', action=StorePathAction, default=expanduser('~/Amplio'),
                            help='Amplio diectory (default is ~/Amplio).')

    command_group = arg_parser.add_mutually_exclusive_group()
    command_group.add_argument('--dbx', action='store_true', help='Read from dropbox directory structure.')
    command_group.add_argument('--s3', action='store_true', help='Read from an S3 directory structure.')

    arg_parser.add_argument('--file', action=StorePathAction, default=None, help='Specify file to import/export')
    arg_parser.add_argument('--import', dest='imports', nargs='?', default=None, const='all',
                            choices=['all', 'deployments', 'content', 'recipients'],
                            help='Import from CSV to SQL.')
    arg_parser.add_argument('--disposition', choices=['abort', 'commit'], default='abort',
                            help='Database disposition for the import operation.')
    arg_parser.add_argument('--export', dest='exports', action='store_true', help='Export from SQL to CSV.')

    arg_parser.add_argument('--copy', nargs=2, action=StorePathAction, help='Copy a program spec, removing extranea.')

    arg_parser.add_argument('--group', choices=['programs', 'specs', 'both', 'tests', 'go-for-it'],
                            help='Process one of the pre-defined groups of programs')

    arg_parser.add_argument('--compare', dest='comparees', action=StorePathAction, nargs=2)
    arg_parser.add_argument('--directory', action=StorePathAction, default='.', help='Directory for CSV files.')

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
    arglist = sys.argv[1:] + ['--db-host', 'localhost']
    #
    #
    ######################################################

    args = arg_parser.parse_args(arglist)

    _init()

    if args.group:
        do_group()
    elif args.exports:
        do_export(args.programs)
    elif args.imports:
        do_import(args.programs, args.imports, args.disposition == 'commit')
    elif args.comparees:
        do_compare(args.comparees)
    elif args.copy:
        do_copy()


if __name__ == '__main__':
    main()
