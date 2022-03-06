import re
import struct
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime
from http import HTTPStatus
from io import BytesIO
from pathlib import Path
from typing import List, Optional
from uuid import uuid4
from zipfile import ZipFile

from sqlalchemy import text

from S3Data.S3Utils import s3, PROCESSED_PREFIX, PROCESSED_BUCKET, UF_PREFIX, UF_BUCKET, ARCHIVE_PREFIX, \
    ARCHIVE_BUCKET
from tbstats import TbCollectedData, decode_properties
from utils import escape_csv, get_table_metadata, get_db_connection
from utils.csvUtils import parse_as_csv, csv_as_str

# Recognize the TalkingBookData.zip filename.
_TBDATA_ZIP_PATTERN = re.compile(r'(?ix)(?P<year>\d{4})-?(?P<month>\d{2})-?(?P<day>\d{2})[ T]?'
                                 r'(?P<hour>\d{2}):?(?P<minute>\d{2}):?(?P<second>\d{2})\.(?P<fraction>\d*)Z/'
                                 r'TalkingBookData\.zip')

# Recognize an OperationalData filename, and extract the entire filename (file), basename (name), and extention
_OPDATA_FILE_PATTERN = re.compile(r'(?ix)(?P<year>\d{4})-?(?P<month>\d{2})-?(?P<day>\d{2})[ T]?'
                                  r'(?P<hour>\d{2}):?(?P<minute>\d{2}):?(?P<second>\d{2})\.(?P<fraction>\d*)Z/'
                                  r'OperationalData/'
                                  r'(?P<name>(?P<stem>[a-z0-9_.-]+)(?P<suffix>\.[a-z0-9_]+))')

# Recognize a userrecording filename, and extract the parts.
_UF_FILE_PATTERN = re.compile(r'(?ix)(?P<year>\d{4})-?(?P<month>\d{2})-?(?P<day>\d{2})[ T]?'
                              r'(?P<hour>\d{2}):?(?P<minute>\d{2}):?(?P<second>\d{2})\.(?P<fraction>\d*)Z/'
                              r'userrecordings/'
                              r'(?P<name>(?P<stem>[a-z0-9_.-]+)(?P<suffix>\.[a-z0-9_]+))')

# Extract package #, playlist #, message # from a UF filename
_UF_RELATION_PATTERN = re.compile(r'(?ix)uf_pkg(?P<pkg>\d+)_pl(?P<pl>\d+)_msg-?(?P<msg>\d+).*')

_UF_PROPERTY_TO_UF_COLUMN = {
    # prop_name: column_name
    'metadata.MESSAGE_UUID': 'message_uuid',
    # 'deployment_uuid': 'DEPLOYEDUUID', # Timestamp is probably sufficient
    'PROJECT': 'programid',
    'DEPLOYMENT_NUMBER': 'deploymentnumber',
    'RECIPIENTID': 'recipientid',
    'TALKINGBOOKID': 'talkingbookid',
    'TBCDID': 'deployment_tbcdid',
    'TIMESTAMP': 'deployment_timestamp',
    'USERNAME': 'deployment_user',
    'TESTDEPLOYMENT': 'test_deployment',
    # 'collection_uuid': 'collection.STATSUUID', # Timestamp is probably sufficient
    'collection.TBCDID': 'collection_tbcdid',
    'collection.TIMESTAMP': 'collection_timestamp',
    'collection.USERNAME': 'collection_user',
    'metadata.SECONDS': 'length_seconds',
    'metadata.BYTES': 'length_bytes',
    'metadata.LANGUAGE': 'language',
    'metadata.DATE_RECORDED': 'date_recorded',
    'metadata.RELATION': 'relation',
}


def aws_status_code(aws_result) -> int:
    return aws_result.get('ResponseMetadata', {}).get('HTTPStatusCode', -1)


def is_ok_result(aws_result, *args) -> bool:
    """
    Did the AWS call return an HTTPStatusCode of 200?
    :param aws_result: Result value from an AWS call.
    :param args: Optional additional success code(s).
    """
    status_code = aws_status_code(aws_result)
    return status_code == HTTPStatus.OK or status_code in args


def is_err_result(aws_result) -> bool:
    """
    If it is not OK, it is an error.
    """
    return not is_ok_result(aws_result)


@dataclass
class Summary:
    key: str
    zip_len: int
    programid: str
    names: str
    talkingbookid: str
    have_statistics: bool
    collections: int
    deployments: int
    play_statistics: int
    uf_messages: int
    s3_errors: int
    unexpected_files: int
    disposition: str


class S3Importer:
    def __init__(self, s3_bucket, s3_object, now: datetime, **kwargs):
        """
        Import a single zip file from collected-data in s3.
        :param s3_bucket: The S3 bucket in which the object was found.
        :param s3_object: The object with the bits of the zip file.
        :param now: datetime of when the driver was started.
        :param kwargs: Optional arguments: 'verbose', 'upsert', 'dry_run', 'no_db', 'uf_only',
                    'archive_only'
        """
        self._temp_dir = tempfile.TemporaryDirectory(prefix='stats_import')
        self._temp_dir_path = Path(self._temp_dir.name)

        self._verbose = kwargs.get('verbose', 1)
        self._save_db = True
        self._save_uf = True # including uf_messages db, even if _save_db is False
        self._save_s3 = True
        self._archive = True
        self.set_action_flags(kwargs)

        self._upsert = kwargs.get('upsert', False)

        self._found_uf = False
        self._s3_bucket = s3_bucket
        self._s3_object = s3_object
        self._s3_write_errors = 0
        self._archive_status = 'not archived'
        self._year = f'{now.year:04d}'
        self._month = f'{now.month:02d}'
        self._day = f'{now.day:02d}'

        self._tb_data_zip_bytes = None
        self._tb_collected_data: Optional[TbCollectedData] = None
        self._summary = None

        self._matched_files = set()
        self._unmatched_files = 0

        self._operational_data = {}
        self._userrecordings_properties = {}

        self._tbscollected_rows = []
        self._tbsdeployed_rows = []
        self._playstatistics_rows = []
        self._uf_messages_csv_rows = []

        self._timestamp_str = re.match(r'(?ix).*(\d{4}-?\d{2}-?\d{2}[T ]?\d{2}:?\d{2}:?\d{2}.\d{3}Z).zip',
                                       s3_object['Key']).group(1)
        self._data_source_name = self._s3_object['Key']

    def set_action_flags(self, kwargs:dict):
        if kwargs.get('dry_run') or kwargs.get('no_db') or kwargs.get('uf_only') or kwargs.get('archive_only'):
            self._save_db = False
        if kwargs.get('dry_run') or kwargs.get('no_uf') or kwargs.get('archive_only'):
            self._save_uf = False
        if kwargs.get('dry_run') or kwargs.get('no_s3') or kwargs.get('uf_only') or kwargs.get('archive_only'):
            self._save_s3 = False
        if kwargs.get('dry_run') or kwargs.get('uf_only') or kwargs.get('no_archive'):
            self._archive = False


    def log_aws_write_error(self, aws_result, key: str):
        print(f'Error writing \'{key}\' to s3: {aws_status_code(aws_result)}')
        self._s3_write_errors += 1

    # noinspection PyPep8Naming
    def put_s3_object(self, Body, Bucket, Key):
        """
        Helper to wrap s3.put_object(...). Detects and logs errors.
        :param Body: The 'Body' argument for s3.put_object(...) 
        :param Bucket: The 'Bucket' argument for s3.put_object(...) 
        :param Key: The 'Key' argument for s3.put_object(...) 
        :return: The result of the s3.put_object(...) call.
        """
        put_result = s3.put_object(Body=Body, Bucket=Bucket, Key=Key)
        if is_err_result(put_result):
            self.log_aws_write_error(put_result, Key)
        return put_result

    @property
    def have_tb_data(self) -> bool:
        return self._tb_collected_data is not None and self._tb_data_zip_bytes is not None

    @property
    def have_collection(self) -> bool:
        return self._operational_data.get('tbscollected.csv') is not None

    @property
    def have_deployment(self) -> bool:
        return self._operational_data.get('tbsdeployed.csv') is not None

    @property
    def have_statistics(self) -> bool:
        return self.have_tb_data and self._tb_collected_data.has_statistics

    @property
    def have_uf(self) -> bool:
        return self._found_uf

    @property
    def summary(self) -> Summary:
        return self._summary

    def do_import(self):
        get_result = s3.get_object(Bucket=self._s3_bucket, Key=self._s3_object['Key'])
        if is_err_result(get_result):
            print(f'Could not fetch \'{self._s3_object["Key"]}\' object from s3: {aws_status_code(get_result)}')
            return
        data = get_result.get('Body').read()
        print(f'\nProcessing statistics from {self._s3_object["Key"]}')
        zip_data = BytesIO(data)
        with ZipFile(zip_data) as zipfile:
            self.import_operational_data(zipfile)
            self.import_talkingbook_data(zipfile)
            self.import_userrecordings(zipfile)
            self.report_unmatched_files(zipfile)
        self.emit_statistics()
        self.fill_userrecordings_metadata()
        self.put_processed_data()
        self.update_database()
        self.archive_collected_data()
        self.make_summary(len(data))

    def make_summary(self, zip_len: int):
        """
        Prints a summary of the import activity.
        """
        key = Path(self._s3_object["Key"]).name
        programid = None
        names = 'unknown'
        talkingbookid = None
        if self._tb_collected_data:
            programid = self._tb_collected_data.programid
            name_list = []
            if useremail := self._tb_collected_data.stats_collected_useremail:
                name_list.append(useremail)
            if tbcdid := self._tb_collected_data.stats_collected_tbcdid:
                name_list.append(tbcdid)
            names = '/'.join(name_list)
            talkingbookid = self._tb_collected_data.talkingbookid
        self._summary = Summary(key, zip_len, programid, names, talkingbookid, self.have_statistics,
                                len(self._tbscollected_rows), len(self._tbsdeployed_rows),
                                len(self._playstatistics_rows), len(self._uf_messages_csv_rows),
                                self._s3_write_errors, self._unmatched_files, self._archive_status)

        s = self._summary
        summary = f'File {s.key}'
        if s.have_statistics:
            summary += f' {programid}, by {names}'
            summary += f', {s.collections} collections' \
                       f', {s.deployments} deployments' \
                       f', {s.play_statistics} play stats' \
                       f', {s.uf_messages} uf messages' \
                       f', {s.s3_errors} s3 write errors'
        else:
            summary += f', no collected data found'
        summary += f', disposition: {s.disposition}.'
        print(summary)

    def import_operational_data(self, zipfile: ZipFile):
        """
        For every file in OperationalData (in the zip), read and remember the contents of the file. Create a file
        on the file system for the log reader's use.
        :param zipfile: The ZipFile of the collected data.
        :return: None
        """
        for zipinfo in zipfile.infolist():
            if m := _OPDATA_FILE_PATTERN.match(zipinfo.filename):
                self._matched_files.add(zipinfo.filename)
                data = zipfile.read(zipinfo.filename)
                self._operational_data[m.group('name')] = data.decode('utf-8')
                opdata_path = Path(self._temp_dir_path, 'OperationalData', m.group('name'))
                opdata_path.parent.mkdir(parents=True, exist_ok=True)
                with opdata_path.open('wb') as opdata_file:
                    opdata_file.write(data)

        if tbscollected_str := self._operational_data.get('tbscollected.csv'):
            self._tbscollected_rows.extend(parse_as_csv(tbscollected_str, c2ll=True))
        if tbsdeployed_str := self._operational_data.get('tbsdeployed.csv'):
            self._tbsdeployed_rows.extend(parse_as_csv(tbsdeployed_str, c2ll=True))

    def import_talkingbook_data(self, zipfile: ZipFile):
        # we could simply look for "TalkingBookData.zip", but this lets us be case-insensitive.
        for zipinfo in zipfile.infolist():
            if _TBDATA_ZIP_PATTERN.match(zipinfo.filename):
                self._matched_files.add(zipinfo.filename)
                self._tb_data_zip_bytes = zipfile.read(zipinfo.filename)
                # Save TalkingBookData.zip to collected-data-processed
                tb_zip_data = BytesIO(self._tb_data_zip_bytes)
                with ZipFile(tb_zip_data) as tb_zip:
                    tb_zip.extractall(self._temp_dir_path)
                    # Load the TbCollectedData from ${self._temp_dir_path}/TalkingBookData.zip
                    self._tb_collected_data = TbCollectedData(self._temp_dir_path, timestamp_str=self._timestamp_str, data_source_name=self._data_source_name)
                    self._tb_collected_data.process_tb_collected_data()
                self._playstatistics_rows.extend(self._tb_collected_data.playstatistics)

                if 'tbscollected.csv' not in self._operational_data:
                    if tbc := self._tb_collected_data.tbscollected:
                        self._tbscollected_rows.append(tbc)
                        self._operational_data['tbscollected.csv'] = csv_as_str(tbc)
                else:
                    if tbc := self._tb_collected_data.tbscollected:
                        if csv_as_str(tbc) != self._operational_data['tbscollected.csv']:
                            print("oops! Re-created tbscollected.csv doesn't match original.")
                if not self._tb_collected_data.is_stats_only:
                    if 'tbsdeployed.csv' not in self._operational_data:
                        if tbd := self._tb_collected_data.tbsdeployed:
                            self._tbsdeployed_rows.append(tbd)
                            self._operational_data['tbsdeployed.csv'] = csv_as_str(tbd)
                    else:
                        if tbc := self._tb_collected_data.tbsdeployed:
                            if csv_as_str(tbc) != self._operational_data['tbsdeployed.csv']:
                                print("oops! Re-created tbsdeployed.csv doesn't match original.")


    def import_userrecordings(self, zipfile: ZipFile):
        """
        Process the files in the userrecordings directory (in the zip). Convert the
        .wav files to .mp3 (they take 1/10th the space). Remember the contents of the
        corresponding .properties files.
        :param zipfile: The ZipFile of the collected data.
        :return: None
        """
        # pass 1, .properties files.
        for zipinfo in zipfile.infolist():
            if (m := _UF_FILE_PATTERN.match(zipinfo.filename)) and m.group('suffix').lower() == '.properties':
                self._matched_files.add(zipinfo.filename)
                data = zipfile.read(zipinfo.filename)
                self._userrecordings_properties[m.group('stem')] = decode_properties(data.decode('utf-8'), sep='[:=]')
        # pass 2, .wav files
        for zipinfo in zipfile.infolist():
            if (m := _UF_FILE_PATTERN.match(zipinfo.filename)) and m.group('suffix').lower() == '.wav':
                self._matched_files.add(zipinfo.filename)
                # if self._save_s3 or self._save_uf:
                    # Only take the hit for conversion if we want to keep the converted file.
                data = zipfile.read(zipinfo.filename)
                self._found_uf = self.convert_wav_to_mp3(data, m.group('stem')) or self._found_uf

    def report_unmatched_files(self, zipfile: ZipFile):
        for zipinfo in zipfile.infolist():
            if zipinfo.filename not in self._matched_files and zipinfo.filename[-1] != '/':  # directories end in '/'
                self._unmatched_files += 1
                if self._verbose > 1:
                    print(f'No match for zipped file: {zipinfo.filename} (unexpected in .zip file)')

    def convert_wav_to_mp3(self, raw_data: bytes, fn: str) -> bool:
        """
        Given the raw bytes of a .wav file, convert it to a .mp3 file. Also fix the
        bad length fields generated by some versions of TBv2. The .mp3 file is written
        to the local file system in userrecordings.
        :param raw_data: bytes of the .wav file
        :param fn: The name of the file, like uf_pkg0_pl0_msg0_1.wav
        :return: None
        """

        def fix_zero_length_bug(data_to_fix: bytes):
            mutable = bytearray(data_to_fix)
            # check for the bug
            byte_buf = mutable[4:8]
            value = struct.unpack('<I', byte_buf)[0]
            if value == 0:
                # The bug's here, so fix it. The real wavesize should be: wavesize = filesize - 8
                actual_len = len(data_to_fix)
                byte_buf = struct.pack('<I', actual_len - 8)
                mutable[4:8] = byte_buf
                # And the audiosize: audiosize = filesize - 44 (44 bytes of RIFF and WAVE header)
                byte_buf = struct.pack('<I', actual_len - 44)
                mutable[40:44] = byte_buf
            return mutable

        def duration(wav_bytes) -> int:
            """
            Calculate the play time of a .wav file.
            :param wav_bytes: The bytes of the wav file.
            :return: Play time in seconds.
            """
            byte_buf = wav_bytes[28:32]
            bytes_per_second = struct.unpack('<I', byte_buf)[0]
            byte_buf = wav_bytes[40:44]
            data_bytes = struct.unpack('<I', byte_buf)[0]
            seconds = data_bytes / bytes_per_second
            return seconds

        if len(raw_data) < 44:
            return False  # can't be a valid .wav file

        properties = self._userrecordings_properties[fn]
        # If the UUID has not already been created, do so now. Ideally it is assigned at TB-Loader time or earlier.
        # We need the UUID here to name the .mp3 file.
        if not (uuid := properties.get('metadata.MESSAGE_UUID')):
            uuid = str(uuid4())
            properties['metadata.MESSAGE_UUID'] = uuid

        data = fix_zero_length_bug(raw_data)

        wav_path = Path(self._temp_dir_path, 'userrecordings', fn).with_suffix('.wav')
        wav_path.parent.mkdir(parents=True, exist_ok=True)
        with wav_path.open('wb') as wav_file:
            wav_file.write(data)

        # Create the .mp3 file with the UUID filename.
        mp3_path = wav_path.with_stem(uuid).with_suffix('.mp3')

        # Use lame for conversion; it's faster than ffmpeg.
        conversion_command = ['lame', str(wav_path), str(mp3_path)]
        if self._verbose > 1:
            print(f'wav->mp3 command line: {" ".join(conversion_command)}')
        ff_result = subprocess.run(conversion_command, capture_output=True)
        if ff_result.returncode != 0:
            print(ff_result.stdout.decode('utf-8'))
            print(ff_result.stderr.decode('utf-8'))
            return False
        if self._verbose > 1:
            print(ff_result)
        duration = int(duration(data) + 0.5)
        properties['metadata.SECONDS'] = str(duration)
        file_len = mp3_path.lstat().st_size
        properties['metadata.BYTES'] = str(file_len)
        wav_path.unlink(missing_ok=True)
        return True

    def emit_statistics(self):
        if self.have_statistics:
            # Get the playstatistics.csv, the only statistics we currently get from the logs
            playstatistics_path = Path(self._temp_dir_path, 'playstatistics.csv')
            self._tb_collected_data.save_playstatistics_csv(playstatistics_path)

    def fill_userrecordings_metadata(self):
        if not self.have_uf:
            return
        collection_props = self._tb_collected_data.stats_collected_properties

        for fn, properties in self._userrecordings_properties.items():
            if (uuid := properties.get('metadata.MESSAGE_UUID')) and (
                    mp3_path := Path(self._temp_dir_path, 'userrecordings', fn)
                            .with_stem(uuid).with_suffix('.mp3')).exists():
                properties['PROJECT'] = collection_props['deployment_PROJECT']
                properties['DEPLOYMENT_NUMBER'] = collection_props['deployment_DEPLOYMENT_NUMBER']
                properties['RECIPIENTID'] = collection_props['deployment_RECIPIENTID']
                properties['TALKINGBOOKID'] = collection_props['deployment_TALKINGBOOKID']
                properties['TBCDID'] = collection_props['deployment_TBCDID']
                properties['TIMESTAMP'] = collection_props['deployment_TIMESTAMP']
                properties['USERNAME'] = collection_props.get('deployment_USEREMAIL',
                                                              collection_props.get('deployment_USERNAME'))
                properties['TESTDEPLOYMENT'] = collection_props['deployment_TESTDEPLOYMENT']
                properties['collection.TBCDID'] = collection_props['TBCDID']
                properties['collection.TIMESTAMP'] = collection_props['TIMESTAMP']
                properties['collection.USERNAME'] = collection_props['USEREMAIL']
                #     'date_recorded': make_date_extractor('metadata.DATE_RECORDED'),
                properties['metadata.filename'] = Path(fn).with_suffix('').name
                properties['metadata.LANGUAGE'] = self._tb_collected_data.deployment_language
                if m := _UF_RELATION_PATTERN.match(fn):
                    message = self._tb_collected_data.packages_data.get_message_by_index(int(m.group('pkg')),
                                                                                         int(m.group('pl')),
                                                                                         int(m.group('msg')))
                    if relation := message.filename if message else None:
                        properties['metadata.RELATION'] = Path(relation).with_suffix('').name
                # save the uuid.properties file
                with mp3_path.with_suffix('.properties').open('w') as props_file:
                    props_file.write('\n'.join([f'{k}={v}' for k, v in properties.items()]))
                # create a row for the uf_messages.csv file, {column_name: value}
                uf_messages_row = {_UF_PROPERTY_TO_UF_COLUMN[k]: properties.get(k) for k in
                                   _UF_PROPERTY_TO_UF_COLUMN.keys()}
                self._uf_messages_csv_rows.append(uf_messages_row)

        # Create the uf_messages.csv file
        uf_messages_path = Path(self._temp_dir_path, 'uf_messages.csv')
        with uf_messages_path.open('w') as uf_messages_file:
            print(','.join(_UF_PROPERTY_TO_UF_COLUMN.values()), file=uf_messages_file)
            for row in self._uf_messages_csv_rows:
                print(','.join([escape_csv(row[x]) for x in _UF_PROPERTY_TO_UF_COLUMN.values()]), file=uf_messages_file)

    def put_processed_data(self):
        prefix = f'{PROCESSED_PREFIX}/{self._year}/{self._month}/{self._day}/{self._timestamp_str}'

        if self._save_s3:
            # OperationalData
            for fn, data in self._operational_data.items():
                key = f'{prefix}/OperationalData/{fn}'
                self.put_s3_object(Body=data, Bucket=PROCESSED_BUCKET, Key=key)
            # Also save tbscollected.csv and tbsdeployed.csv under the timestamp key.
            if 'tbsdeployed.csv' in self._operational_data:
                self.put_s3_object(Body=self._operational_data['tbsdeployed.csv'], Bucket=PROCESSED_BUCKET,
                                   Key=f'{prefix}/tbsdeployed.csv')
            if 'tbscollected.csv' in self._operational_data:
                self.put_s3_object(Body=self._operational_data['tbscollected.csv'], Bucket=PROCESSED_BUCKET,
                                   Key=f'{prefix}/tbscollected.csv')

            # Create playstatistics.csv under the timestamp key.
            playstatistics_path = Path(self._temp_dir_path, 'playstatistics.csv')
            if playstatistics_path.exists():
                with playstatistics_path.open('r') as playstatistics_file:
                    data = playstatistics_file.read()
                    self.put_s3_object(Body=data, Bucket=PROCESSED_BUCKET, Key=f'{prefix}/playstatistics.csv')

            # TalkingBookData.zip
            if self.have_tb_data:
                self.put_s3_object(Body=self._tb_data_zip_bytes, Bucket=PROCESSED_BUCKET,
                               Key=f'{prefix}/TalkingBookData.zip')

        if self.have_uf and self._save_uf:
            # Copy uf_messages.csv under the timestamp key.
            if (uf_messages_path := Path(self._temp_dir_path, 'uf_messages.csv')).exists():
                with uf_messages_path.open('r') as uf_messages_file:
                    data = uf_messages_file.read()
                    self.put_s3_object(Body=data, Bucket=PROCESSED_BUCKET, Key=f'{prefix}/uf_messages.csv')

        if self.have_uf and (self._save_s3 or self._save_uf):
            # copy userrecordings
            collection_props = self._tb_collected_data.stats_collected_properties
            uf_prefix = f'{UF_PREFIX}/{collection_props["deployment_PROJECT"]}/{collection_props["deployment_DEPLOYMENT_NUMBER"]}'
            for fn, properties in self._userrecordings_properties.items():
                if (uuid := properties.get('metadata.MESSAGE_UUID')) and (
                        (mp3_path := Path(self._temp_dir_path, 'userrecordings', fn).with_stem(uuid).with_suffix(
                            '.mp3')).exists() and
                        (props_path := mp3_path.with_suffix('.properties')).exists()):
                    mp3_key = mp3_path.name
                    props_key = props_path.name
                    with mp3_path.open('rb') as mp3_file:
                        data = mp3_file.read()
                        if self._save_s3:
                            self.put_s3_object(Body=data, Bucket=PROCESSED_BUCKET, Key=f'{prefix}/userrecordings/{mp3_key}')
                        if self._save_uf:
                            self.put_s3_object(Body=data, Bucket=UF_BUCKET, Key=f'{uf_prefix}/{mp3_key}')
                    with props_path.open('rb') as props_file:
                        data = props_file.read()
                        if self._save_s3:
                            self.put_s3_object(Body=data, Bucket=PROCESSED_BUCKET,
                                           Key=f'{prefix}/userrecordings/{props_key}')
                        if self._save_uf:
                            self.put_s3_object(Body=data, Bucket=UF_BUCKET, Key=f'{uf_prefix}/{props_key}')

    def update_database(self):
        # noinspection SqlDialectInspection
        def make_insert(metadata):
            columns = [x.name for x in metadata.columns]
            # noinspection SqlNoDataSourceInspection
            command_str = (f'INSERT INTO {metadata.name} ({",".join(columns)}) '
                           f'VALUES (:{",:".join(columns)}) ')
            conflict_str = 'ON CONFLICT DO NOTHING'
            if self._upsert and metadata.primary_key:
                # ON CONFLICT ON CONSTRAINT pky_name DO
                #     UPDATE SET
                #       c1=EXCLUDED.c1,
                #       c2=EXCLUDED.c2,
                #       -- for all non-pkey columns
                pkey_name = metadata.primary_key.name
                pkey_columns = [x.name for x in metadata.primary_key.columns]
                non_pkey_columns = [x for x in columns if x not in pkey_columns]
                if non_pkey_columns:
                    conflict_str = f'ON CONFLICT ON CONSTRAINT {pkey_name} DO UPDATE SET '
                    setters = [f'{x}=EXCLUDED.{x}' for x in non_pkey_columns]
                    conflict_str += ','.join(setters)
            command_str += conflict_str + ';'
            return text(command_str)

        def insert_rows(rows: list[dict], table_name: str):
            def norm(col, val):
                # nullable columns with no value are inserted as None, not as ''
                return val if val is None or (not isinstance(val, str)) or len(val) != 0 or col not in nullables else None
            if len(rows) == 0: return
            metadata = get_table_metadata(table_name)
            columns = [x.name for x in metadata.columns]
            nullables: List[str] = [x.name for x in metadata.columns if x.nullable]
            normalized = [{col: norm(col, row.get(col)) for col in columns} for row in rows]  # inserts col:None for missing values
            command = make_insert(metadata)
            result = conn.execute(command, normalized)
            print(f'{result.rowcount} rows inserted{"/updated" if self._upsert else ""} into {table_name}.')

        with get_db_connection() as conn:
            if self.have_collection and self._save_db:
                insert_rows(self._tbscollected_rows, 'tbscollected')
            if self.have_deployment and self._save_db:
                insert_rows(self._tbsdeployed_rows, 'tbsdeployed')
            if self.have_statistics and self._save_db:
                insert_rows(self._playstatistics_rows, 'playstatistics')
            if self.have_uf and self._save_uf:
                insert_rows(self._uf_messages_csv_rows, 'uf_messages')

    def archive_collected_data(self):
        """
        Moves the imported stats file to the archive area (acm-stats/archived-data.v2)
        """
        if not self._archive:
            return  # nothing to do here on a dry run
        if self._s3_write_errors != 0:
            print('**************************************************')
            print(f'{self._s3_write_errors}, not moving {self._s3_object["Key"]}.')
            print('**************************************************')
            return

        tbcdid = self._tb_collected_data.stats_collected_tbcdid if self._tb_collected_data else 'unknown'
        dest_prefix = f'{ARCHIVE_PREFIX}/{self._year}/{self._month}/{self._day}/{tbcdid}'
        dest_name = Path(self._s3_object['Key']).name
        copy_result = s3.copy_object(Bucket=ARCHIVE_BUCKET, Key=f'{dest_prefix}/{dest_name}',
                                     CopySource={'Bucket': self._s3_bucket, 'Key': self._s3_object['Key']})
        if is_ok_result(copy_result):
            delete_result = s3.delete_object(Bucket=self._s3_bucket, Key=self._s3_object['Key'])
            if is_ok_result(delete_result, HTTPStatus.NO_CONTENT):
                self._archive_status = 'archived'
            else:
                self._archive_status = 'copied, not removed'
        else:
            self._archive_status = 'copy error'