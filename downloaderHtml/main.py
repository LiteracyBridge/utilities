#!/usr/bin/env zsh
"exec" "./downloaderEnv/bin/python3" "main.py" "$@"

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import argparse
from datetime import date, datetime
from io import StringIO
from math import floor
from os import stat_result
from os.path import expanduser
from pathlib import Path

# The part of the web page before the list of files.
from typing import Union, List, Tuple

import boto3 as boto3

s3 = boto3.client('s3')

PROLOG = """
<!DOCTYPE html><html lang="en">
<head> 
<link rel="preconnect" href="https://fonts.gstatic.com"> 
<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@300&display=swap" rel="stylesheet"> 
<meta charset="UTF-8"> 
<title>File Downloads</title> 
<style> body { background-color: #f7f7f7; font-family: 'Arial', sans-serif; } #main { margin: 2em 10%; padding: 1em; border: 2px solid #007; border-radius: 0.5em; } #main h1 { font-size: 2em; } #main tr:nth-child(even) { background-color: #e0e7f7; } td:nth-child(1) { font-family: monospace, helvetica, sansserif; font-weight: 100; } tr:nth-child(n+2) td {padding-top: 10px;} #main table { width: 100%; } </style>
</head>
<body><div id="main"> <h1>Available Files</h1> <table>
"""

# A file row. Use FILE_ROW.format(**kwargs) where kwargs has values for 'filename_str', 'timestamp_str', and "size_str'
FILE_ROW = '<tr><td><a href="./{filename_str}" download>{filename_str}</a></td><td>{timestamp_str}</td><td>{size_str}</td></tr>'

DIR_ROW = '<tr><td><a href="./{dirname_str}/{indexname_str}">{dirname_str}</a></td></tr>'

# The part of the web page after the list of files
EPILOG = """
</table></div>
</body>
</html>
"""


class StorePathAction(argparse.Action):
    """
    An argparse.Action to store a Path object. A leading ~ is expanded to the user's home directory.
    """

    def _expand(self, v: str) -> Union[None, str]:
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


# List the objects with the given prefix.
# noinspection PyPep8Naming
def _list_objects(Bucket, Prefix='', **kwargs):
    paginator = s3.get_paginator("list_objects_v2")
    request_args = {'Bucket': Bucket, 'Prefix': Prefix, **kwargs}
    for objects in paginator.paginate(**request_args):
        for obj in objects.get('Contents', []):
            yield obj


def _list_prefixes(Bucket: str, Prefix: str = '', **kwargs):
    paginator = s3.get_paginator("list_objects_v2")
    request_args = {'Bucket': Bucket, 'Prefix': Prefix, **kwargs}
    for objects in paginator.paginate(**request_args):
        for obj in objects.get('CommonPrefixes', []):
            yield obj


def _format_size_str(size: int) -> str:
    """
    Format a file's size in an easier-to-read form. "Normal" units, not SI units.
    :param size: The size in bytes.
    :return: A string representing the approximate size, in KB, MB, etc.
    """
    units = ['Bytes', 'KB', 'MB', "GB", "TB"]
    size_in_units = size
    for i in range(len(units)):
        if size_in_units <= 999:
            if floor(size_in_units) == size_in_units:
                size_in_units = floor(size_in_units)
                format = '{:d} {}'
            else:
                format = '{:.3f} {}'
            return format.format(size_in_units, units[i])
        size_in_units /= 1000
    return '{:.2f} PB'.format(size_in_units)


def make_index(dirs: List[str], files: List[Tuple[str, int, datetime]], index_file,
               indexname_str: str = 'index.html') -> None:
    print(PROLOG, file=index_file)
    for d in dirs:
        print(DIR_ROW.format(dirname_str=d, indexname_str=indexname_str), file=index_file)
    for f in files:
        if f[0].startswith('.'):
            continue
        size_str = _format_size_str(f[1])
        timestamp_str = f[2].strftime('%Y-%m-%d %H:%M')
        print(FILE_ROW.format(filename_str=f[0], timestamp_str=timestamp_str, size_str=size_str),
              file=index_file)
    print(EPILOG, file=index_file)


def process_directory(directory: Path, **kwargs) -> None:
    """
    Gets a list of the files in a directory, and creates an index file in that directory. The index file
    has a table of the files found in the directory, with a clickable & downloadable file name.

    Note that the index file, if found in the directory, won't be included in the list of downloadable files.
    :param directory: Directory with files.
    :param index: Name of the index file, default index.html
    :return: None
    """
    if not directory.is_dir():
        raise ValueError(f"'{str(directory)}' is not a directory.")

    index = kwargs.get('index', 'index.html')
    dry_run = kwargs.get('dry_run', False)
    recursive = kwargs.get('recursive', True)
    verbose = kwargs.get('verbose', 0)

    index = index.lower()

    contents = [f for f in directory.iterdir()]
    dirs = sorted([d for d in contents if d.is_dir()]) if recursive else []
    files = sorted([f for f in contents if f.is_file()])

    with StringIO() as index_file:
        dir_names = [d.name for d in dirs]
        file_info = []
        for f in files:
            if f.name.lower() != index:
                stat: stat_result = f.stat()
                file_info.append((f.name, stat.st_size, datetime.fromtimestamp(stat.st_ctime)))
        make_index(dir_names, file_info, index_file, indexname_str=index)

        if not dry_run:
            with open(Path(directory, index), 'w') as file_out:
                file_out.write(index_file.getvalue())
        if verbose:
            print(index_file.getvalue())

    for d in dirs:
        process_directory(d, **kwargs)


def process_bucket(bucket: str, prefix: str, **kwargs):
    index = kwargs.get('index', 'index.html')
    dry_run = kwargs.get('dry_run', False)
    recursive = kwargs.get('recursive', True)
    verbose = kwargs.get('verbose', 0)

    index = index.lower()
    prefix_len = len(prefix)
    dirs = [d for d in _list_prefixes(Bucket=bucket, Prefix=prefix, Delimiter='/')] if recursive else []
    objs = [x for x in _list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/')]

    with StringIO() as index_file:
        dir_names = [d['Prefix'][prefix_len:-1] for d in dirs]
        file_info = []
        for f in objs:
            if f['Key'][prefix_len:] != index:
                file_info.append((f['Key'][prefix_len:], f['Size'], f['LastModified']))
        make_index(dir_names, file_info, index_file, indexname_str=index)
        if not dry_run:
            key = prefix + index
            put_result = s3.put_object(Body=index_file.getvalue(), Bucket=bucket, Key=key, ContentType= 'text/html')
            status = put_result.get('ResponseMetadata', {}).get('HTTPStatusCode', -1)
            if status != 200:
                print(f"Error saving {'s3://' + bucket + '/' + prefix + index}: {status}.")
        if verbose:
            print(index_file.getvalue())

    for d in dirs:
        process_bucket(bucket, d['Prefix'], **kwargs)


def process(args: argparse.Namespace) -> None:
    target: Path = args.target
    kwargs = {
        'dry_run': args.dry_run,
        'recursive': not args.no_recursive,
        'index': args.index,
        'verbose': args.verbose
    }
    if kwargs['verbose']:
        print('Verbose output is on.')
        print(f'Will {"not " if not kwargs["recursive"] else ""}recurse into sub-directories.')
        if kwargs['dry_run']:
            print('Dry run: no files / objects will be created.')
        print(f'Using {kwargs["index"]} for index file.')

    target_parts = target.parts
    if len(target_parts) >= 3 and target_parts[0].startswith('s3:'):
        bucket = target_parts[1]
        prefix = '/'.join(target_parts[2:]) + '/'
        process_bucket(bucket, prefix, **kwargs)
    elif not target.exists():
        print(f'Target {str(target)} does not exist.')
    elif not target.is_dir():
        print(f'Target {str(target)} is not a directory.')
    else:
        process_directory(target, **kwargs)


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('target', metavar='target', action=StorePathAction, default=None,
                            help='The directory or S3 bucket/path for which to build an index file.')
    arg_parser.add_argument('--no-recursive', action='store_true', default=False,
                            help='Do not generate links or index files for sub-directories.')
    arg_parser.add_argument('--dry-run', '--dryrun', '-n', action='store_true',
                            help='Dry-run; do not save index files.')
    arg_parser.add_argument('--verbose', '-v', action='count', default=0, help='Verbose output.')
    arg_parser.add_argument('--index', default="index.html", help='Name of index file, default index.html.')

    args = arg_parser.parse_args()

    process(args)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Tests:
    # for x in [0, 1, 9, 99, 999, 1000, 9999, 999999, 1000000,
    #           999999999, 1000000000, 123456789,
    #           999999999999, 1000000000000, 1234567890123,
    #           1234567890123456789]:
    #     print(f'{x} -> {_format_size_str(x)}')

    main()
