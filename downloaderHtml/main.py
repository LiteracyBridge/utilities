# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import argparse
from datetime import date, datetime
from math import floor
from os import stat_result
from os.path import expanduser
from pathlib import Path


# The part of the web page before the list of files.
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
FILE_ROW='<tr><td><a href="./{filename_str}" download>{filename_str}</a></td><td>{timestamp_str}</td><td>{size_str}</td></tr>'

# The part of the web page after the list of files
EPILOG = """
</table></div>
</body>
</html>
"""


class StorePath(argparse.Action):
    """
    Mostly exists to expand leading '~' in a path.
    """
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        if nargs is not None:
            raise ValueError("nargs not allowed")
        super(StorePath, self).__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        ENSURE_TRAILING_SLASH = []
        values = expanduser(values)
        if option_string in ENSURE_TRAILING_SLASH and values[-1:] != '/':
            values += '/'
        setattr(namespace, self.dest, Path(values))


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


def process(directory: Path, index: str='index.html') -> None:
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
    index = index.lower()

    with open(Path(directory, index), 'w') as index_file:
        print(PROLOG, file=index_file)
        for f in directory.iterdir():
            if f.name.lower() != index:
                stat: stat_result = f.stat()
                size_str = _format_size_str(stat.st_size)
                timestamp_str = datetime.fromtimestamp(stat.st_ctime).strftime('%Y-%m-%d %H:%M')
                print(FILE_ROW.format(filename_str=f.name, timestamp_str=timestamp_str, size_str=size_str),
                    file=index_file)
        print(EPILOG, file=index_file)


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('directory', metavar='directory', action=StorePath, default=None,
                            help='The directory for which to build an index file.')
    arg_parser.add_argument('--index', default="index.html", help='Name of index file, default index.html.')

    args = arg_parser.parse_args()

    process(directory=args.directory, index=args.index)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Tests:
    # for x in [0, 1, 9, 99, 999, 1000, 9999, 999999, 1000000,
    #           999999999, 1000000000, 123456789,
    #           999999999999, 1000000000000, 1234567890123,
    #           1234567890123456789]:
    #     print(f'{x} -> {_format_size_str(x)}')

    main()
