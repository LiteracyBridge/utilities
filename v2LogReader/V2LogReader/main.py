# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import argparse
import sys
from pathlib import Path
from typing import Optional, Dict

from tbstats import TbCollectedData
from S3Data import S3Driver
from utils import StorePathAction, get_db_engine

args: Optional[argparse.Namespace] = None
verbose = 0

_tbsdeployed_csv: Optional[Path] = None
_tbscollected_csv: Optional[Path] = None
_playstatistics_csv: Optional[Path] = None

_num_files = 0
_tbs_processed = set()
_programs_processed = dict()
_num_collected = 0
_num_deployed = 0
_num_playstatistics = 0
_num_uf = 0

# class StorePathAction(argparse.Action):
#     """
#     An argparse.Action to store a Path object. A leading ~ is expanded to the user's home directory.
#     """
#
#     @staticmethod
#     def _expand(v: str) -> Optional[str]:
#         """
#         Does the work of expanding.
#         :param v: A string, possibly with a leading ~ to be expanded ot user's home directory.
#         :return: A Path object that encapsulates the given path. Note that there is no guarantee of
#             any actual file system object at that path.
#         """
#         return v and Path(expanduser(v))  # 'None' if v is None, otherwise Path(expanduser(v))
#
#     def __init__(self, option_strings, dest, default=None, **kwargs):
#         if 'glob' in kwargs:
#             self._glob = kwargs.get('glob', False)
#             del kwargs['glob']
#         else:
#             self._glob = False
#         super(StorePathAction, self).__init__(option_strings, dest, default=self._expand(default), **kwargs)
#
#     def __call__(self, parser, namespace, values, option_string=None):
#         if self._glob and not isinstance(values, list): raise TypeError(
#             "The 'glob' option requires that the value is a list; did you use 'nargs'?")
#         if not isinstance(values, list):
#             values = self._expand(values)
#         else:
#             result = []
#             for value in values:
#                 path = self._expand(value)
#                 path_str = str(path)
#                 if self._glob and ('*' in path_str or '?' in path_str):
#                     import glob
#                     result.extend([Path(x) for x in glob.glob(path_str)])
#                 else:
#                     result.append(path)
#             values = result
#         setattr(namespace, self.dest, values)


def process_tb_collected_data(path: Path, num=0) -> None:
    """
    Process a zip file of collected statistics. These files are named like:
        yyyymmddThhmmss.mmmZ (mm:month, mm:minute, mmm:millisecond)
    :param path: The path of the .zip file, or the directory of an expanded zip file.
    :return: None
    """
    global _num_files, _tbs_processed, _programs_processed, _num_collected, _num_deployed, _num_playstatistics, _num_uf
    if verbose > 1:
        print(f'Processing file{""if not num else " #"+str(num)}: {path.name}')
    tbd = TbCollectedData(path)
    tbd.process_tb_collected_data()
    tbd.save_operational_csvs(tbsdeployed='tbsdeployed.csv', tbscollected='tbscollected.csv')

    if tbd.has_statistics:
        tbd.save_playstatistics_csv(_playstatistics_csv)

    _num_files += 1
    programid = tbd.programid
    _programs_processed.setdefault(programid, set()).add(tbd.talkingbookid)
    _tbs_processed.add(tbd.talkingbookid)
    _num_collected += tbd.num_tbs_collected
    _num_deployed += tbd.num_tbs_deployed
    _num_playstatistics += tbd.num_playstatistics
    _num_uf += tbd.num_uf_files


def process_v1_operation_data(dirs: list[Path]) -> int:
    global _num_files
    for dir in dirs:
        if verbose >= 2:
            print(f'Examining {str(dir)}')
        kwargs = {}
        tbData: Path = Path(dir, 'tbDataAll.kvp')
        deployments: Path = Path(dir, 'deploymentsAll.kvp')
        stats_collected_props: Path = Path(dir, 'stats_collected.properties')
        tbscollected: Path = Path(dir, 'tbscollected.csv')
        tbsdeployed: Path = Path(dir, 'tbsdeployed.csv')

        if tbsdeployed.exists() and not args.force:
            print(f'File {tbsdeployed.name} already exists')
        elif not deployments.exists():
            print(f'Can not create {tbsdeployed.name} because file {deployments.name} does not exist.')
        else:
            if verbose >= 2:
                print(f'{"Deleting and re-c" if tbsdeployed.exists() else "C"}reating {tbsdeployed.name}.')
            tbsdeployed.unlink(missing_ok=True)
            kwargs['tbsdeployed'] = tbsdeployed
            kwargs['deployments'] = deployments

        if tbscollected.exists() and not args.force:
            if verbose >= 1:
                print(f'File {tbscollected.name} already exists')
        elif not (stats_collected_props.exists() and tbData.exists()):
            missing = ' and '.join([x.name for x in [stats_collected_props, tbData] if not x.exists()])
            print(f'Can not create {tbscollected.name} because files {missing} do not exist.')
        else:
            if verbose >= 2:
                print(f'{"Deleting and re-c" if tbscollected.exists() else "C"}reating {tbscollected.name}.')
            tbscollected.unlink(missing_ok=True)
            kwargs['tbscollected'] = tbscollected
            kwargs['tbData'] = tbData

        if kwargs:
            kwargs['verbose'] = verbose
            tbd = TbCollectedData(dir, verbose=verbose)
            tbd.save_operational_csvs(**kwargs)
        _num_files += 1
    return _num_files


def init_global_tb_operations_csvs():
    global _tbscollected_csv, _tbsdeployed_csv, _playstatistics_csv
    if _tbscollected_csv is None:
        _tbscollected_csv = Path('tbscollected.csv')
        _tbscollected_csv.unlink(missing_ok=True)
    if _tbsdeployed_csv is None:
        _tbsdeployed_csv = Path('tbsdeployed.csv')
        _tbsdeployed_csv.unlink(missing_ok=True)
    if _playstatistics_csv is None:
        _playstatistics_csv = Path('playstatistics.csv')
        _playstatistics_csv.unlink(missing_ok=True)

def process_unzipped_dirs(dirs) -> None:
    init_global_tb_operations_csvs()
    for dir in dirs:
        process_tb_collected_data(dir, num=_num_files+1)


def search_dirs_and_process_zips() -> (int,int,int,int,int):
    global args
    init_global_tb_operations_csvs()
    excluded_names = set([x.name for x in args.excluded])
    queue: list[Path] = [x for x in args.logs]
    while len(queue) > 0:
        path = queue.pop(0)
        if path.is_file():
            if path.suffix == '.zip':
                process_tb_collected_data(path, num=_num_files+1)

        else:
            if path in args.excluded or path.name in excluded_names:
                continue
            queue.extend(path.iterdir())


def process_s3_imports():
    global args
    # prefix to concatenate with the global prefix (default is 'collected-data.v2')
    for prefix in args.s3:
        s3_driver = S3Driver(prefix=prefix, args=args)
        if s3_driver.find_objects():
            s3_driver.process_objects()

def report():
    if _num_uf is None:
        print(f'{len(_num_files)} processed')
    else:
        details = f'{len(_programs_processed)} programs, {len(_tbs_processed)} tbs seen, {_num_collected} collections, {_num_deployed} updates, {_num_playstatistics} play statistics, {_num_uf} user recordings'
        print(f'{_num_files} zip files/directories processed {"" if _num_uf is None else details}.')
        for program, tbs in _programs_processed.items():
            print(f'{program} had {len(tbs)} TBs.')



def main():
    global args, verbose, playstatistics_csv_file2, tbsdeployed_csv_file2
    arg_parser = argparse.ArgumentParser(description="Read TB-2 log files and create statistics CSV files.")
    arg_parser.add_argument('--playstatistics', '-ps', action=StorePathAction, help='Store playstatistics here.')
    arg_parser.add_argument('--verbose', '-v', action='count', default=0, help="More verbose output.")

    arg_parser.add_argument('--dry-run', '--dryrun', '-n', action='store_true', help='Dry run, do not write to s3, do not insert to db.')
    arg_parser.add_argument('--no-uf', action='store_true', help='Do not publish uf files, nor update uf_messages table.')
    arg_parser.add_argument('--uf-only', action='store_true', help='Only publish uf files and update uf_messages table.')

    arg_parser.add_argument('--no-db', action='store_true', help='Do not update the database.')
    arg_parser.add_argument('--no-s3', action='store_true', help='Do not publish anything to s3.')

    arg_parser.add_argument('--no-archive', action='store_true', help='Do not move the timestamp.zip file (eg 202212119T111230.123Z.zip) to the archived-data.v2 area.')
    arg_parser.add_argument('--archive-only', action='store_true', help='Only move the timestamp.zip file to the archive area; do not update any database or other s3 area.')

    arg_parser.add_argument('--force', '-f', action='store_true',
                            help="Force creation, even if target files already exist.")
    arg_parser.add_argument('--dir', dest='dirs', nargs='*', action=StorePathAction, default=[],
                            help='Expanded TB-2 log archives.')
    from S3Data.S3Utils import DEFAULT_SOURCE_BUCKET
    arg_parser.add_argument('--source-bucket', action='store', default=DEFAULT_SOURCE_BUCKET, help='S3 bucket in which to find collected data.')
    arg_parser.add_argument('--s3', nargs='*', action='store', help="Prefix to objects. If the value starts with '/', it is considered an absolute path under the S3 bucket; " +
                                                                    "if not, it is relative to '/collected-data.v2' in the S3 bucket")
    arg_parser.add_argument('--v1op', nargs='*', action=StorePathAction, glob=True,
                            help='Generate TB operation data from v1 collected data')
    arg_parser.add_argument('--exclude', dest='excluded', nargs='*', action=StorePathAction, default=[],
                            help='Directories to be excluded (at any level).')
    arg_parser.add_argument('logs', nargs='*', action=StorePathAction, glob=True,
                            help='Zip files and/or directories containing zip files to import.')

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

    arglist = None
    ######################################################
    #
    #
    # arglist = sys.argv[1:] + ['--db-host', 'localhost']
    #
    #
    ######################################################
    args = arg_parser.parse_args(arglist)

    if not args.dirs and not args.logs and not args.v1op and not args.s3:
        arg_parser.print_usage()
        return 1

    verbose = args.verbose
    num_uf = None

    bool_args = {k: v for k, v in args.__dict__.items() if isinstance(v, bool)}
    if len(conflicts:=[x for x in ['dry_run', 'uf_only', 'archive_only'] if bool_args.get(x)]) > 1:
        print(f'Incompatible arguments specified: {conflicts}')
        arg_parser.print_usage()
        return 1

    if args.s3:
        get_db_engine(args)
        process_s3_imports()

    # If there are individual .zip files given for processing (test & development usage)
    process_unzipped_dirs(args.dirs)

    if len(args.logs) > 0 and _num_files == 0:
        search_dirs_and_process_zips()

    if args.v1op and _num_files == 0:
        process_v1_operation_data(args.v1op)

    report()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
