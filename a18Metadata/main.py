# This is a sample Python script.
import argparse
import csv
import struct
from os.path import expanduser
from pathlib import Path
from typing import List, Tuple, Dict

ENSURE_TRAILING_SLASH = []


class StorePath(argparse.Action):
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        if nargs is not None:
            raise ValueError("nargs not allowed")
        super(StorePath, self).__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        values = expanduser(values)
        if option_string in ENSURE_TRAILING_SLASH and values[-1:] != '/':
            values += '/'
        setattr(namespace, self.dest, Path(values))


class BinaryReader:
    """
    Helper class for reading binary data, providing those functions (and only those)
    required to read TB metadata from an .a18 file.
    """

    def __init__(self, buffer: bytes):
        """
        Initialize with a bytes object; keep track of where we are in that object.
        :param buffer: bytes of metadata.
        """
        self._buffer = buffer
        self._offset = 0
        # adjust these if we ever need to support big-endian
        self._I32 = '<l'
        self._I16 = '<h'
        self._I8 = '<b'

    def _readX(self, format: str) -> any:
        """
        Read value(s) per the format string, and advance the offset.
        :param format: specification of one or more values to read.
        :return: the raw result from struct.unpack_from(), a tuple of values
        """
        values = struct.unpack_from(format, self._buffer, self._offset)
        self._offset += struct.calcsize(format)
        return values

    def readI32(self) -> int:
        """
        Read a 32-bit signed integer.
        :return: the integer.
        """
        return self._readX(self._I32)[0]

    def readI16(self) -> int:
        """
        Read a 16-bit signed integer.
        :return: the integer.
        """
        return self._readX(self._I16)[0]

    def readI8(self) -> int:
        """
        Read a 8-bit signed integer.
        :return: the integer.
        """
        return self._readX(self._I8)[0]

    def readUtf8(self) -> str:
        """
        Read a UTF-8 encoded string. These are encoded as a 16-bit length, followed by ${length}
        bytes of encoded data.
        :return: the string.
        """
        str_len = self.readI16()
        str_format: str = f'<{str_len}s'
        str_bytes: bytes = self._readX(str_format)[0]
        return str_bytes.decode('utf-8')


class MetadataReader():
    """
    Class to parse .a18 metadata.
    """

    def __init__(self, buffer: BinaryReader, directory_map: Dict):
        self._buffer = buffer
        self._directory_map = directory_map
        # The known metadata types. From LBMetadataIDs.java. 
        self._md_parsers = {
            0: (self._string_md_parser, 'CATEGORY'),
            1: (self._string_md_parser, 'TITLE'),
            5: (self._string_md_parser, 'PUBLISHER'),
            10: (self._string_md_parser, 'IDENTIFIER'),
            11: (self._string_md_parser, 'SOURCE'),
            12: (self._string_md_parser, 'LANGUAGE'),
            13: (self._string_md_parser, 'RELATION'),
            16: (self._string_md_parser, 'REVISION'),
            22: (self._string_md_parser, 'DURATION'),
            23: (self._string_md_parser, 'MESSAGE_FORMAT'),
            24: (self._string_md_parser, 'TARGET_AUDIENCE'),
            25: (self._string_md_parser, 'DATE_RECORDED'),
            26: (self._string_md_parser, 'KEYWORDS'),
            27: (self._string_md_parser, 'TIMING'),
            28: (self._string_md_parser, 'PRIMARY_SPEAKER'),
            29: (self._string_md_parser, 'GOAL'),
            30: (self._string_md_parser, 'ENGLISH_TRANSCRIPTION'),
            31: (self._string_md_parser, 'NOTES'),
            32: (self._string_md_parser, 'BENEFICIARY'),
            33: (self._integer_md_parser, 'STATUS'),
            35: (self._string_md_parser, 'SDG_GOALS'),
            36: (self._string_md_parser, 'SDG_TARGETS'),
        }

    def _string_md_parser(self, joiner: str = ';'):
        """
        Parses a string-valued metadata entry.
        :param buffer: The BinaryReader with the metadata.
        :param joiner: A delimiter with which to join multiple values.
        :return: The value(s) found.
        """
        results = []
        num_values = self._buffer.readI8()
        for i in range(num_values):
            str_value = self._buffer.readUtf8()
            results.append(str_value)
        return joiner.join(results)

    def _integer_md_parser(self, joiner: str = ';'):
        """
        Parses an integer-valued metadata entry. Values are returned as their string representation.
        :param buffer: The BinaryReader with the metadata.
        :param joiner: A delimiter with which to join multiple values.
        :return: The value(s) found.
        """
        results = []
        num_values = self._buffer.readI8()
        for i in range(num_values):
            int_value = self._buffer.readI32()
            results.append(str(int_value))
        return joiner.join(results)

    def parse(self):
        version = self._buffer.readI32()
        if version != 1:
            raise ValueError(f"Unknown metadata version. Expected '1', but found '{version}'.")
        num_fields = self._buffer.readI32()
        metadata = {}

        for i in range(num_fields):
            field_id = self._buffer.readI16()
            field_len = self._buffer.readI32()

            if field_id in self._md_parsers:
                fn, name = self._md_parsers.get(field_id)
                result = fn()
                metadata[name] = result
            else:
                print(f'undecoded field {field_id}')

        return metadata


class A18Processor():
    def __init__(self, **kwargs):
        self._directory_map = {}
        self._unique_directory_map = {}
        if 'map' in kwargs:
            self.load_recipients_map(kwargs['map'])
        self._create_info = kwargs.get('info', False)
        self._project = kwargs.get('project', None)


    def extract_metadata_blob_from_a18(self, a18_path: Path) -> bytes:
        """
        Extract the metadata from an .a18 file. The file consists of a 32-bit length-of-audio-data, length bytes of
        audio data, bytes-til-eof of metadata
        :param a18_path: path to the .a18 file.
        :return: a bytes consisting of the data.
        """
        file_len = a18_path.stat().st_size
        f = open(a18_path, 'rb')
        buffer = f.read(4)
        audio_len = struct.unpack('<l', buffer)[0]
        md_offset = audio_len + 4
        md_len = file_len - md_offset
        f.seek(md_offset)
        md = f.read(md_len)
        return md

    def _add_recipientid(self, a18_path: Path, metadata: Dict) -> None:
        source = metadata.get('SOURCE')
        if source in self._unique_directory_map:
            # There was exactly one entry in recipients_map with this SOURCE.
            project, recipientid = self._unique_directory_map[source]
        else:
            # There were zero, or 2 or more entries in recipients_map with this SOURCE
            project = None
            recipientid = None
            parts = a18_path.parts
            if not self._project and len(parts) > 5:
                # like /Users/bill/Dropbox/collected-data-processed/2020/11/12/recordingsprocessed/lbg-covid19/LBG-COVID19-20-1/000d/EREMON-DAZUURI_CHPS-ANNA_KYAAKYELE/B-000D0686_9-0_45E56EF2.a18
                # ('/', 'Users', ... 'collected-data-processed', {year}, {month}, {day}, {recordingsdir}, {project}, {deployment}, {collector}, {source}, {feedback id}
                community_directory = parts[-2].upper()
                if community_directory == source:
                    project = parts[-5].upper()
                    _, recipientid = self._directory_map[(project, source)]
                else:
                    print(f"Community directory '{community_directory}' doesn't match SOURCE '{source}'.")

        if project:
            metadata['project'] = project
        if recipientid:
            metadata['recipientid'] = recipientid

    def process_file(self, a18_path: Path) -> None:
        """
        Process one a18 file.
        :param a18_path: The file to process.
        :return: None
        """
        if not a18_path.suffix.lower() == '.a18':
            print(f'Not an .a18 file: {a18_path}')
        else:
            try:
                md_bytes = self.extract_metadata_blob_from_a18(a18_path)
                bytes_reader = BinaryReader(md_bytes)
                md_parser = MetadataReader(bytes_reader, self._directory_map)
                metadata = md_parser.parse()

                if self._create_info:
                    self._add_recipientid(a18_path, metadata)
                    # print(f'Source: {metadata.get("SOURCE", "**unknown*")}\n  File: {a18_file}')
                    print(
                        f'File: {a18_path}\n  id: {metadata.get("recipientid", "**unknown**")}, source: {metadata.get("SOURCE", "**unknown*")}')
                    # for k, v in metadata.items():
                    #     print('{:>16}: {}'.format(k, v))

                    info_path = a18_path.with_suffix('.info')
                    with open(info_path, "w") as info_file:
                        for k, v in metadata.items():
                            print(f'{k}={v}', file=info_file)

            except Exception as ex:
                print(f'Exception parsing {a18_path}: {ex}')

    def process_files(self, file_spec: Path) -> Tuple[int, int, int]:
        """
        Given a Path to an a18 file, or a directory containing a18 files, process the file(s).
        :param file_spec: A path.
        :return: a tuple of the counts of directories and files processed, and the files skipped.
        """
        n_files = 0
        n_skipped = 0
        n_dirs = 0
        if not file_spec.exists():
            raise FileNotFoundError(f'{str(file_spec)} does not exist')
        if file_spec.is_file():
            n_files += 1
            self.process_file(file_spec)
        else:
            n_dirs += 1
            remaining: List[Path] = [f for f in file_spec.iterdir()]
            while len(remaining) > 0:
                f: Path = remaining.pop(0)
                if f.is_dir():
                    n_dirs += 1
                    remaining = [ff for ff in f.iterdir()] + remaining
                elif f.suffix.lower() == '.a18':
                    n_files += 1
                    self.process_file(f)
                else:
                    n_skipped += 1
        return n_dirs, n_files, n_skipped

    def load_recipients_map(self, map_csv: Path) -> None:
        """
        Reads a recipients_map file, which allows us to translate the "community directory" to recipientid.
        :param map_csv: The name of the recipients_map file.
        :return: None
        """
        duplicate_directories = set()
        with open(map_csv, 'r') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                directory = row['directory']
                project = row['project']

                if directory in self._unique_directory_map:
                    # no longer unique
                    del self._unique_directory_map[directory]
                    duplicate_directories.add(directory)
                elif directory not in duplicate_directories:
                    self._unique_directory_map[directory] = (row['project'], row['recipientid'])

                # Maps to disambiguate when the same community is in multiple projects
                self._directory_map[(project, directory)] = (row['project'], row['recipientid'])


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--dropbox', action=StorePath, default=expanduser('~/Dropbox'),
                            help='Dropbox directory (default is ~/Dropbox).')
    arg_parser.add_argument('a18', metavar='a18', action=StorePath, default=None,
                            help='The a18 file from which to extract metadata, or a directory with .a18 files.')
    arg_parser.add_argument('--map', action=StorePath, help='A recipients_map.csv file.')
    arg_parser.add_argument('--info', action="store_true", help='Create a filename.info for every .a18 file processed.')
    arg_parser.add_argument('--project', help='Assume this, if the project can\'t be determined reliably.')

    arg_parser.add_argument('--dry-run', '-n', action='store_true', help='Don\'t update anything.')

    arg_parser.add_argument('--feedback', action='store_true', help='Create an ACM suitable for User Feedback.')

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
    dropbox_directory = args.dropbox

    map_spec = args.map
    a18_spec: Path = args.a18
    n_dirs, n_files, n_skipped = A18Processor(map=map_spec, info=args.info, project=args.project).process_files(a18_spec)
    print(f'Processed {n_files} files in {n_dirs} directories. Skipped {n_skipped} non-a18 files.')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
