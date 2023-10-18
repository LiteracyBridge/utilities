import argparse
import re
import sys
from os.path import expanduser
from pathlib import Path
from typing import Optional

import boto3

args: Optional[argparse.Namespace] = None
# These are the database columns, and the properties they come from
# We don't actually use this as a dict, but it provides a nice way to get parallel lists of property & column names.
PROP_TO_COLUMN = {
    'metadata.MESSAGE_UUID': 'message_uuid',
    'PROJECT': 'programid',
    'DEPLOYMENT_NUMBER': 'deploymentnumber',
    'RECIPIENTID': 'recipientid',
    'TALKINGBOOKID': 'talkingbookid',
    'collection.deployment_TBCDID': 'deployment_tbcdid',
    'collection.deployment_TIMESTAMP': 'deployment_timestamp',
    'collection.deployment_USERNAME': 'deployment_user',
    'TESTDEPLOYMENT': 'test_deployment',
    'collection.TBCDID': 'collection_tbcdid',
    'collection.TIMESTAMP': 'collection_timestamp',
    'collection.USERNAME': 'collection_user',
    'metadata.SECONDS': 'length_seconds',
    'metadata.BYTES': 'length_bytes',
    'metadata.LANGUAGE': 'language',
    'metadata.DATE_RECORDED': 'date_recorded',
    'metadata.RELATION': 'relation'
}
# This is what we need, but props is what we have.
# REQUIRED_COLUMNS = [
#     'message_uuid', 'programid', 'deploymentnumber', 'recipientid', 'talkingbookid', 'deployment_timestamp',
#     'collection_timestamp', 'length_seconds', 'length_bytes', 'language',
# ]
REQUIRED_PROPS = [
    'metadata.MESSAGE_UUID', 'PROJECT', 'DEPLOYMENT_NUMBER', 'RECIPIENTID', 'TALKINGBOOKID',
    'collection.deployment_TIMESTAMP', 'collection.TIMESTAMP', 'metadata.SECONDS', 'metadata.BYTES', 'metadata.LANGUAGE'
]


s3_client = boto3.client('s3')
# Recognizer for:
#   key_no_spaces = value may have spaces # comment
prop_pattern = re.compile(r'\s*(?P<key>\S*)\s*[:=]\s*(?P<value>.*?)\s*$')

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
        self._glob = kwargs.get('glob', False)
        if 'glob' in kwargs:
            del kwargs['glob']
        super(StorePathAction, self).__init__(option_strings, dest, default=self._expand(default), **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        if self._glob and not isinstance(values, list): raise TypeError(
            "The 'glob' option requires that the value is a list; did you use 'nargs'?")
        if not isinstance(values, list):
            values = self._expand(values)
        else:
            result = []
            for value in values:
                path = self._expand(value)
                path_str = str(path)
                if self._glob and ('*' in path_str or '?' in path_str):
                    import glob
                    result.extend([Path(x) for x in glob.glob(path_str)])
                else:
                    result.append(path)
            values = result
        setattr(namespace, self.dest, values)

def get_properties(key: str) -> Optional[dict[str,str]]:
    """
    Reads and parses the object at key, presumed to a a properties file.
    :param key: of the object.
    :return: the parsed properties as a dict, or None if non-existant, empty, or unparseable
    """
    result = None
    found_nones = 0
    try:
        obj = s3_client.get_object(Bucket=args.bucket, Key=key)
        raw_data = obj.get('Body').read().decode('utf-8')
        prop_lines = raw_data.splitlines()
        result = {}
        for line in prop_lines:
            text = line.split('#', 1)[0].strip()
            if not text: continue
            if m := prop_pattern.match(line):
                if m['value'] == 'None':
                    found_nones = found_nones + 1
                    # print(f'Found literal \'None\' in properties file\n    {key}\n    {line}', file=sys.stderr)
                else:
                    result[m['key']] = m['value']
            elif line:
                print(f'Can\'t parse line from properties file\n    {key}\n    {line}', file=sys.stderr)
        if len(result) == 0:
            result = None
    except s3_client.NoSuchKey:
         # No properties object, nothing we can do. Report and move on.
         print(f'No properties file: {key}', file=sys.stderr)
    except Exception as ex:
        print(f'Unexpected exception getting properties object \'{key}\': {ex}', file=sys.stderr)
    return result

def escape_csv(s: any) -> str:
    """
    Given a value, return a string suitable for a CSV file. Double-quotes (") are
    doublec (""), and values with newlines, carriage returns, commas, or double
    quotes are enclosed in quotes.
    :param s: the value to be escaped
    :return: the value as a csv-safe string
    """
    if s is None:
        return ''
    if not isinstance(s, str):
        s = str(s)
    # escape a double quote character with another double quote
    if '"' in s:
        s = s.replace('"', '""')
    # If the string contains a newline, a comma, or a double quote (now escaped),
    # enclose the entire string in double quotes.
    if '\n' in s or '\r' in s or ',' in s or '"' in s:
        s = f'"{s}"'
    return s

def make_csv_line(properties: dict) -> Optional[str]:
    """
    Given a dict from a properties file, return the CSV line for that dict, if all
    of the required columns are present. Otherwise return None.
    :param properties: dict from a properties file.
    :return: a csv-line from the dict.
    """
    if any( len(properties.get(x, ''))==0 for x in REQUIRED_PROPS):
        return None
    props = [escape_csv(properties.get(p)) for p in PROP_TO_COLUMN.keys()]
    line = ','.join(props)
    return line

def make_csv_header() -> str:
    """
    Header line for the csv, matching the columns of the uf_messages table.
    :return: string of the header line.
    """
    return ','.join(PROP_TO_COLUMN.values())

def process_input_line(line, output):
    """
    Process one line from the "missing uf report". Lines look like this:
        collected/CBCC-AT/5/08457807-c0a0-5d65-a7bb-70f025e196ca
    :param line: string with the path and UUID of a UF file not in the uf_messages table.
    :type line:
    :param output: The file to which to write the line.
    :return: None
    """
    key = f'{line}.properties'
    if properties := get_properties(key=key):
        if csv_line := make_csv_line(properties):
            print(csv_line, file=output)


def process_input_lines(input, output):
    """
    Read the lines from input, and process each one.
    :param input: A file of lines from the "missing uf report".
    :param output: A file to create with a CSV to be inserted into the database.
    :return: None
    """
    print(make_csv_header(), file=output)
    i=0
    while line:=input.readline():
        if (i:=i+1) % 16 == 0: print('.', end='')
        process_input_line(line.strip(), output)

def main() -> int:
    global args
    arg_parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    arg_parser.add_argument('--verbose', action='count', help='Verbose logging', default=0)
    arg_parser.add_argument('--output', action=StorePathAction, default=Path('uf_messages.csv'),
                            help='File to create with inserts for missing uf_messages.')
    arg_parser.add_argument('--input', action=StorePathAction, default=None)
    arg_parser.add_argument('--bucket', default='amplio-uf', help='Bucket with collected UF')
    args = arg_parser.parse_args()

    input = args.input.open('r') if isinstance(args.input, Path) else sys.stdin
    output = args.output.open('w')

    process_input_lines(input=input, output=output)
    return 0

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    sys.exit( main() )

