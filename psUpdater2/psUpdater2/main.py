import argparse
import sys
from os.path import expanduser
from pathlib import Path
from typing import Any, List, Optional, Dict

import ps
from ps import ProgramSpec, publish_to_s3, write_to_csv, write_to_xlsx

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


def _get_path(program: str, file: str, dir: Optional[Path]) -> Path:
    """
    Get the path to a config or program spec file. If the '--dbx' option is given, returns a Path to the
    file in the ~/Dropbox/ACM-{program}/ directory. If the '--s3' option is given, returns a Path to the
    file in the ~/Amplio directory. If neither is given, returns {args.directory}/{program}/{file}.
    :param program: Program for which the path is desired.
    :param file: File for which the path is desired.
    :return: Path to the file.
    """
    global args
    if dir:
        result = Path(dir, program, file)
    elif args.dbx:
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


def test_update_general(program='TEST'):
    global args
    print('Creating program spec from database')
    spec_db: ProgramSpec
    spec_db, messages = ProgramSpec.create_from_db(program)

    spec_db.general.listening_models = ["Other", "Groups", "Place-based"]

    reimported_db_spec: ProgramSpec
    with ps.get_db_connection() as conn:
        transaction = conn.begin()
        spec_db.write_to_db(connection=conn)
        reimported_db_spec, messages = ProgramSpec.create_from_db(program, connection=conn)
        transaction.commit()
    diffs = ps.compare_program_specs(spec_db, reimported_db_spec)
    print(f'{"Diffs" if diffs else "No diffs"} from original db vs saved2db')
    print(*diffs, sep='\n')

    return reimported_db_spec


def do_publish(spec_db, artifacts=None):
    global args
    if artifacts is None:
        artifacts = ['general', 'deployments', 'content', 'recipients']
    print('Creating program spec from database')
    # publish_to_s3(spec_db)
    temp = Path('~/temp/').expanduser()
    for artifact in artifacts:
        spec_db.write_to_csv(artifact, temp)
    spec_db.write_to_xlsx(temp)
    json_str = spec_db.write_to_json()
    json_path = Path(temp, 'progspec.json')
    with json_path.open('w') as json_file:
        json_file.write(json_str)

def test_publish():
    global args
    program = 'TEST'
    print('Creating program spec from database')
    spec_db: ProgramSpec
    spec_db, messages = ProgramSpec.create_from_db(program)
    do_publish(spec_db)

def test_clone():
    p_source = 'TEST'
    p_dest = 'TEST-2'

    spec_source: ProgramSpec
    spec_source, msgs = ProgramSpec.create_from_db(p_source)
    if not spec_source or msgs:
        print('Messages loading source')
        print(*msgs, sep='\n')
    spec_dest = ProgramSpec.clone_from_other(spec_source, p_dest)
    ok, msgs = spec_dest.write_to_db()
    if not ok or msgs:
        print('Messages saving clone')
        print(*msgs, sep='\n')

    spec_check: ProgramSpec
    spec_check, msgs = ProgramSpec.create_from_db(p_dest)
    if not spec_source or msgs:
        print('Messages loading clone')
        print(*msgs, sep='\n')
    diffs = ps.compare_program_specs(spec_source, spec_check)
    print(f'{"Diffs" if diffs else "No diffs"} from original db vs clone')
    print(*diffs, sep='\n')


def do_tests():
    program = 'DEMO'
    spec_db = test_update_general(program)
    do_publish(spec_db)
    # test_clone()

def do_export(programs: List[str], dir: Path):
    global args
    for program in programs:
        print(f'Exporting program spec for {program}.')
        path = _get_path(program, 'db_progspec.xlsx', dir)
        if not path.expanduser():
            print(f'File {path} is not found')
            return
        stem = path.stem
        path_db = path.with_name(f'{stem}-db{path.suffix}')
        spec_db, messages = ProgramSpec.create_from_db(program)
        print(f'Exporting  db-created program spec to {path_db}')
        path_db.parent.mkdir(parents=True, exist_ok=True)
        spec_db.write_to_xlsx(path_db)



def do_import(programs: List[str], what: Any, commit: bool = True) -> None:
    global args
    specs: Dict[str, ProgramSpec] = {}

    for program in programs:
        print(f'Importing program spec for {program}.')
        if args.file:
            path = args.file
        else:
            path: Path = _get_path(program, 'pub_progspec.xlsx')
        if not path.expanduser():
            print(f'File {path} is not found')
            return
        stem = path.stem
        path2 = path.with_name(f'{stem}-2{path.suffix}')
        path_json = path.with_name(f'{stem}-json{path.suffix}')
        path_db = path.with_name(f'{stem}-db{path.suffix}')

        work_dir = path.parent

        print(f'Importing program spec from {path}')
        spec: ProgramSpec
        spec, errors = ProgramSpec.create_from_xlsx(program, path)
        specs[''] = spec
        if not spec:
            print(*errors, sep='\n')
        else:
            print(f'Exporting program spec to {path2}')
            spec.write_to_xlsx(path2)

        spec_s3: ProgramSpec
        spec_s3, messages = ProgramSpec.create_from_s3(program)
        diffs = ps.compare_program_specs(spec, spec_s3)
        print(f'{"Diffs" if diffs else "No diffs"} from original spreadsheet vs s3')
        print(*diffs, sep='\n')

        print(f'Exporting program spec as json')
        json_str = spec.write_to_json(json_args={'indent': 2})
        # print(json_str)
        print('[Re-]importing program spec from json')
        spec_json: ProgramSpec
        spec_json, messages = ProgramSpec.create_from_json(program, json_str)
        specs['-json'] = spec_json
        # print(spec_json)
        print(f'Exporting re-imported program spec as {path_json}')
        spec_json.write_to_xlsx(path_json)

        diffs = ps.compare_program_specs(spec, spec_json)
        print(f'{"Diffs" if diffs else "No diffs"} from original spreadsheet -> json -> ps')
        print(*diffs, sep='\n')

        print('Creating program spec from database')
        spec_db: ProgramSpec
        spec_db, messages = ProgramSpec.create_from_db(program)
        specs['-db'] = spec_db

        diffs = ps.compare_program_specs(spec, spec_db)
        print(f'{"Diffs" if diffs else "No diffs"} from original spreadsheet -> database')
        print(*diffs, sep='\n')

        reimported_db_spec: ProgramSpec
        with ps.get_db_connection() as conn:
            transaction = conn.begin()
            spec.write_to_db(connection=conn)
            reimported_db_spec, messages = ProgramSpec.create_from_db(program, connection=conn)
            if args.disposition=='commit':
                transaction.commit()
            else:
                transaction.rollback()

        diffs = ps.compare_program_specs(spec, reimported_db_spec)
        print(f'{"Diffs" if diffs else "No diffs"} from original and re-imported database')
        print(*diffs, sep='\n')

        print(f'Exporting  db-created program spec to {path_db}')
        spec_db.write_to_xlsx(path_db)
        for suffix, spec in specs.items():
            for art in ['general', 'deployments', 'content', 'recipients']:
                out_path = Path(work_dir, f'pub_{art + suffix}.csv')
                spec_db.write_to_csv(artifact=art, path=out_path)

def do_validate() -> None:
    def validate(program):
        print(f'Processing {program}')
        csv_dir = Path(args.outdir, program)
        csv_dir.mkdir(parents=True, exist_ok=True)

        spec_db: ProgramSpec
        spec_db, messages = ProgramSpec.create_from_db(program)

        spec_s3: ProgramSpec
        spec_s3, messages = ProgramSpec.create_from_s3(program)
        diffs = ps.compare_program_specs(spec_db, spec_s3)
        with Path(csv_dir, 'diff.txt').open('w') as diff_file:
            print(f'{"Diffs" if diffs else "No diffs"} from db vs s3 spreadsheet', file=diff_file)
            print(*diffs, sep='\n', file=diff_file)

        spec_db.write_to_xlsx(csv_dir)
        for art in ['general', 'deployments', 'content', 'recipients']:
            spec_db.write_to_csv(artifact=art, path=csv_dir)

    global args
    for program in args.programs:
        validate(program)

def do_compare(ps1, ps2) -> None:
    program = None
    if (path := Path(expanduser(ps1))).exists():
        program = ps2
    elif (path := Path(expanduser(ps2))).exists():
        program = ps1
    if not program:
        print(f'Can\'t find speadsheet in {ps1} or {ps2}')
        return

    xlsx_ps = ProgramSpec.create_from_xlsx(program, path)
    db_ps = ProgramSpec.create_from_db(program)

    if not xlsx_ps[0]:
        print(f'Can\'t find spreadsheet for {program} in {path}.')
        return
    if not db_ps[0]:
        print(f'Can\'t find database for {program}.')
        return

    diffs = ps.compare_program_specs(db_ps[0], xlsx_ps[0])
    print(f'{"Diffs" if diffs else "No diffs"} for {program} from db to {path}.')
    print(*diffs, sep='\n')


def do_list(programs: List[str]) -> None:
    for program in programs:
        db_ps, msgs = ProgramSpec.create_from_db(program)
        json = db_ps.write_to_json(json_args={'indent': 2})
        print(db_ps.general)


def do_json(programs: List[str]) -> None:
    for program in programs:
        db_ps, msgs = ProgramSpec.create_from_db(program)
        json = db_ps.write_to_json(json_args={'indent': 2})
        output_path = Path(f'~/temp/{program}-new.json').expanduser()
        with open(output_path, 'w') as json_out:
            json_out.write(json)
        print(db_ps.general)


def _init() -> None:
    global args
    # Initialize the ps engine with any command line args.
    ps.get_db_engine(args=args)


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
                            help='Import from XLS to Progspec.')
    arg_parser.add_argument('--disposition', choices=['abort', 'commit'], default='abort',
                            help='Database disposition for the import operation.')
    arg_parser.add_argument('--export', action=StorePathAction, help='Export the program spec from the database.')

    arg_parser.add_argument('--compare', nargs=2, help='Compare a program spec vs the database')

    arg_parser.add_argument('--test', action='store_true', default=False, help='Run the test function')

    arg_parser.add_argument('--list', action='store_true', default=False,
                            help='List the general info from given programs')

    arg_parser.add_argument('--json', action='store_true', default=None, help='Export to json')

    arg_parser.add_argument('--validate', action='store_true', help='Compare spec from db against published xlsx; create .csv artifacts')
    arg_parser.add_argument('--outdir', action=StorePathAction, default='~/temp', help='Directory to which to write artifacts.')

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

    if args.list:
        do_list(args.programs)
    elif args.json:
        do_json(args.programs)
    elif args.imports:
        do_import(args.programs, args.imports, args.disposition == 'commit')
    elif args.export:
        do_export(args.programs, args.export)
    elif args.compare:
        do_compare(args.compare[0], args.compare[1])
    elif args.test:
        do_tests()
    elif args.validate:
        do_validate()


if __name__ == '__main__':
    main()
