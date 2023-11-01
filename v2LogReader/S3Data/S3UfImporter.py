import base64
import re
import struct
import subprocess
from pathlib import Path
from typing import Optional, Tuple
from uuid import uuid4
from zipfile import ZipFile

import boto3
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from tbstats import decode_properties, TbCollectedData
from utils import escape_csv

# Recognize a userrecording filename, and extract the parts.
# Like: yyyy-mm-ddTHH:MM:SS.tttZ/userrecordings/uf_pkg0_pl0_msg10_0.properties
_UF_FILE_PATTERN = re.compile(r'(?ix)(?P<year>\d{4})-?(?P<month>\d{2})-?(?P<day>\d{2})[ T]?'
                              r'(?P<hour>\d{2}):?(?P<minute>\d{2}):?(?P<second>\d{2})\.(?P<fraction>\d*)Z/'
                              r'userrecordings/'
                              r'(?P<name>(?P<stem>[a-z0-9_.-]+)(?P<suffix>\.[a-z0-9_]+))')

# Extract package #, playlist #, message # from a UF filename
_UF_RELATION_PATTERN = re.compile(r'(?ix)uf_pkg(?P<pkg>\d+)_pl(?P<pl>\d+)_msg-?(?P<msg>\d+).*')
_UF_RECORDING_PATTERN = re.compile(r'(?ix)uf_pkg(?P<pkg>\d+)_pl(?P<pl>\d+)_msg-?(?P<msg>\d+)_(?P<no>\d+).*')

# Pick 'survey1' out of 'userrecordings/survey1_1.survey'
_SURVEY_ID_RE = re.compile(r'(?ix)(.*\\)?(?P<id>[^.]*?)(?:_\d*)?\.survey')

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

REGION_NAME = 'us-west-2'
PROFILE_NAME = None
# noinspection PyTypeChecker
session = boto3.Session(profile_name=PROFILE_NAME)
dynamodb = session.resource('dynamodb', region_name=REGION_NAME)
KEY_TABLE_NAME = 'uf_keys'
uf_key_table = None


class S3UfImporter:
    def __init__(self, temp_dir_path: Path, tb_collected_data: TbCollectedData, **kwargs):
        self._verbose = kwargs.get('verbose', 1)

        self._temp_dir_path = temp_dir_path
        self._tb_collected_data = tb_collected_data

        self._matched_files = set()
        self._userrecordings_properties = {}
        self._uf_messages_csv_rows = []
        self._found_uf = False
        self._fn_stem_to_uuid_map = {}
        self._surveys = {}

    @property
    def matched_files(self):
        return self._matched_files

    @property
    def userrecordings_properties(self):
        return self._userrecordings_properties

    @property
    def surveys(self):
        return self._surveys

    @property
    def have_surveys(self):
        return len(self._surveys) > 0

    @property
    def have_uf(self):
        return self._found_uf

    @property
    def uf_messages_csv_rows(self):
        return self._uf_messages_csv_rows

    def import_userrecordings(self, zipfile: ZipFile):
        """
        Process the files in the userrecordings directory (in the zip). Convert the
        .wav files to .mp3 (they take 1/10th the space). Remember the contents of the
        corresponding .properties files.
        If the .wav files are encrypted, use the .key file to decrypt the .enc file.

        :param zipfile: The ZipFile of the collected data.
        :return: None
        """

        uf_files = {}  # {uf_stem: {uf_suffix: fname, ...}, ...}

        # Look for UF related files.
        for zipinfo in zipfile.infolist():
            if m := _UF_FILE_PATTERN.match(zipinfo.filename):
                self._matched_files.add(zipinfo.filename)
                if m.group('suffix').lower() == '.properties':
                    # remember as { stem : {uf_properties} }
                    data = zipfile.read(zipinfo.filename)
                    self._userrecordings_properties[m.group('stem')] = (
                        decode_properties(data.decode('utf-8'), sep='[:=]'))
                elif m.group('suffix').lower() == '.survey':
                    # remember as { name : {survey_answers} }
                    data = zipfile.read(zipinfo.filename)
                    self._surveys[m.group('name')] = (
                        decode_properties(data.decode('utf-8'), sep='[:=]'))
                uf_suffixes = uf_files.setdefault(m.group('stem'), {})
                uf_suffixes[m.group('suffix')] = zipinfo.filename

        # Process the UF files.
        for stem, suffixes in uf_files.items():
            if '.enc' in suffixes and '.key' in suffixes:
                self.decrypt_and_convert(zipfile, suffixes, stem)
            elif '.wav' in suffixes:
                data = zipfile.read(suffixes['.wav'])
                self.convert_wav_to_mp3(data, stem)

    def decrypt_and_convert(self, zipfile: ZipFile, suffixes, stem):
        """
        Decrypt an encrypted user feedback file, then convert it to a .mp3 file.
        :param zipfile: The ZipFile with the binary data.
        :param suffixes: A Dict of {suffix: filename, ...} for whatever suffixes were
                found for the UF file. We expect a .properties and either a .wav or
                a .enc and .key. In the early days of encryption, we may keep both
                the .wav and the .enc, to validate the decryption, but we'll want to
                stop that soon (the .enc is exactly the same size as the .wav, which
                is huge).
        :param stem: The base name of the UF file, sans extension.
        :return: None
        """
        # Read the key data for the base-64 encoded encrypted-session-key and iv.
        key_data = zipfile.read(suffixes['.key']).decode('ascii').split('\n')
        # The session key is an AES key, encrypted with RSA. Try to decrypt the session key.
        session_key, iv = self.recover_session_key(key_data[0], key_data[1])
        if session_key:
            # We have the session key, so get the encrypted bytes, and decrypt them.
            encrypted_bytes = zipfile.read(suffixes['.enc'])
            cipher = Cipher(algorithms.AES(session_key), modes.CBC(iv))
            decryptor = cipher.decryptor()
            decrypted_content = decryptor.update(encrypted_bytes) + decryptor.finalize()
            # If we have a .wav file, verify the decryption.
            if '.wav' in suffixes:
                wav_bytes = zipfile.read(suffixes['.wav'])
                if wav_bytes != decrypted_content:
                    print(f'Decrypted file mismatch: {stem}')
                wav_bytes = None
            # Finally, convert to .mp3.
            self.convert_wav_to_mp3(decrypted_content, stem)

    def convert_wav_to_mp3(self, raw_data: bytes, fn: str) -> bool:
        """
        Given the raw bytes of a .wav file, convert it to a .mp3 file. Also fix the
        bad length fields generated by some versions of TBv2. The .mp3 file is written
        to the local file system in userrecordings.
        :param raw_data: bytes of the .wav file
        :param fn: The name of the file, like uf_pkg0_pl0_msg0_1.wav
        :return: None
        """

        def fix_wav_length(data_to_fix: bytes):
            """
            The .wav header _should_ contain the length of the file, but when we create the file
            and write the encrypted bits, we don't know what the final length will be, therefore
            we simply write zeros for the length. Now that we know the length, we can fixup the
            .wav file.
            :param data_to_fix: bytes of the .wav file.
            :return:
            """
            mutable = bytearray(data_to_fix)
            # check for zero length
            byte_buf = mutable[4:8]
            value = struct.unpack('<I', byte_buf)[0]
            if value == 0:
                # The length is zero, so fix it. The real wavesize should be: wavesize = filesize - 8
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

        def get_props():
            if fn in self._userrecordings_properties:
                return self._userrecordings_properties[fn]
            props = {}
            # there wasn't a .properties file, try to synthesize one.
            if m := _UF_RELATION_PATTERN.match(fn):
                # Get as much as we can. If no message has been selected, there won't be "MESSAGE-*"; if no
                # playlist, then no PLAYLIST-*. The first package is auto-selected, so it should exist.
                if pkg := self._tb_collected_data.packages_data.get_package(int(m.group('pkg'))):
                    props = {'PACKAGE_NAME': pkg.name, 'PACKAGE_NUM': int(m.group('pkg')),
                             'DEVICE_ID': self._tb_collected_data.talkingbookid}
                    if pl := pkg.get_playlist(int(m.group('pl'))):
                        props['PLAYLIST_NAME'] = pl.title
                        props['PLAYLIST_NUM'] = int(m.group('pl'))
                        if msg := pl.get_message(int(m.group('msg'))):
                            props['MESSAGE_NAME'] = msg.title
                            props['MESSAGE_NUM'] =  int(m.group('msg'))
                    self._userrecordings_properties[fn] = props
                    return props

        if len(raw_data) < 44:
            return False  # can't be a valid .wav file

        properties = get_props()
        if not properties:
            return False  # Can't tell what recording this applies to.
        # If the UUID has not already been created, do so now. Ideally it is assigned at TB-Loader time or earlier.
        # We need the UUID here to name the .mp3 file.
        if not (uuid := properties.get('metadata.MESSAGE_UUID')):
            uuid = str(uuid4())
            properties['metadata.MESSAGE_UUID'] = uuid

        self._fn_stem_to_uuid_map[Path(fn).stem] = uuid
        data = fix_wav_length(raw_data)

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
        self._found_uf = True

    def fill_userrecordings_metadata(self):
        """
        Update the .properties data for the UF files that we've imported.
        :return: None
        """
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

    def fill_survey_metadata(self):
        """
        Adds global information about this collection to any surveys that were collected.
        :return: None
        """
        if not self.have_surveys:
            return
        collection_props = self._tb_collected_data.stats_collected_properties

        survey: dict
        for fn, survey in self._surveys.items():
            updates: dict = {}
            for key, value in survey.items():
                # Translate filenames to message_uuids, like: s1q10:uf_pkg0_pl0_msg10_0 => s1q10:1181fc6b-89c8-47fc-a5c5-2c520d537352
                value_stem = Path(value).stem
                if value_stem in self._fn_stem_to_uuid_map:
                    updates[key] = self._fn_stem_to_uuid_map[value_stem]
                # Create a surveyid if there isn't one already.
                if 'surveyid' not in survey:
                    if (m:=_SURVEY_ID_RE.match(fn)):
                        updates['surveyid'] = m.group('id')
                # Add deployment & collection data
                updates['programid'] = collection_props['deployment_PROJECT']   # for convenience only; implicit in recipientid
                updates['recipientid'] = collection_props['deployment_RECIPIENTID']
                updates['talkingbookid'] = collection_props['deployment_TALKINGBOOKID']
                updates['deployment_uuid'] = collection_props['deployment_DEPLOYEDUUID']
                updates['collection_uuid'] = collection_props['STATSUUID']
            if updates:
                survey.update(updates)

    def recover_session_key(self, encoded_session_key, encoded_iv) -> Tuple[Optional[bytes], Optional[bytes]]:
        """
        Given a base64 encoded, encrypted session key, and a base64 encoded IV, return the true values.
        :param encoded_session_key: a base64 representation of the session key, encrypted with RSA.
        :param encoded_iv: a base64 representation of the IV used in encrypting the UF.
        :return: (session_key, IV), or (None, None) if can't
        """
        def get_private_key(programid: str, deployment_num: int):
            """
            Read the bytes of the RSA private key from DyanmoDb, and initialize an RSA Private Key from it.
            :param programid: The program for which the session key was encrypted.
            :param deployment_num: The deployment for which the session key was encrypted.
            :return: an RSAPrivateKey
            """
            # Initialize DyanmoDB
            global uf_key_table
            if uf_key_table is None:
                uf_key_table = dynamodb.Table(KEY_TABLE_NAME)
            # Read the value.
            query = uf_key_table.get_item(Key={'programid': programid, 'deployment_num': deployment_num})
            key_row = query.get('Item')
            if key_row:
                # Create the RSAPrivateKey
                key_bytes = bytes(key_row.get('private_pem'))
                private_key = crypto_serialization.load_pem_private_key(
                    key_bytes,
                    password=None,
                )
                return private_key

        programid = self._tb_collected_data.programid
        deployment_num = self._tb_collected_data.deployment_num
        if private_key := get_private_key(programid, deployment_num):
            encrypted_session_key = base64.b64decode(encoded_session_key.strip())
            iv = base64.b64decode(encoded_iv.strip())
            session_key = private_key.decrypt(
                encrypted_session_key,
                padding.PKCS1v15(),
                # padding.OAEP(
                #     mgf=padding.MGF1(algorithm=hashes.SHA256()),
                #     algorithm=hashes.SHA256(),
                #     label=None
                # )
            )
            return session_key, iv
        return None, None
