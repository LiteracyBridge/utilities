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

    def _expand(self, v: str) -> Optional[str]:
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
        if 'file' in args:
            path = args.file
        else:
            path: Path = _get_path(program, 'program_spec.xlsx')
        importer = XlsImporter.Importer(program)

        with path.open('rb') as input_file:
            bytes = input_file.read()

        importer.do_import(what, data=bytes, engine=engine, commit=commit)


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
    arg_parser.add_argument('--disposition', choices=['abort', 'commit'], default='commit',
                            help='Database disposition for the import operation.')
    arg_parser.add_argument('--export', dest='exports', action='store_true', help='Export from SQL to CSV.')

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

    arg_parser.add_argument('programs', nargs='+', help='Program(s) to import and/or export.')

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
    global engine

    if args.exports:
        do_export(args.programs)
    elif args.imports:
        do_import(args.programs, args.imports, args.disposition == 'commit')
    elif args.comparees:
        do_compare(args.comparees)


if __name__ == '__main__':
    main()
