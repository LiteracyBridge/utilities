import io
import re
import tempfile
import traceback
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict
from zipfile import ZipFile, BadZipFile

import dateutil.parser
from sqlalchemy import text

from tbstats import packagesdata
# from tbstats.PackagesData import PackagesData
from tbstats.logfilereader import LogFileReader
from tbstats.packagesdata import Deployment
from tbstats.statistics import Statistics
from utils import escape_csv, get_db_connection, get_db_engine

TBSDEPLOYED_CSV_COLUMNS: list[str] = [
    'talkingbookid', 'recipientid', 'deployedtimestamp', 'project', 'deployment', 'contentpackage', 'firmware',
    'location', 'latitude', 'longitude', 'username', 'tbcdid', 'action', 'newsn', 'testing', 'deployment_uuid'
]
DEPLOYMENTS_LOG_TO_TBSDEPLOYED_CSV = {'sn': 'talkingbookid', 'recipientid': 'recipientid',
                                      'timestamp': 'deployedtimestamp', 'project': 'project',
                                      'deployment': 'deployment', 'package': 'contentpackage', 'firmware': 'firmware',
                                      'location': 'location', 'latitude':'latitude', 'longitude':'longitude', 'username': 'username',
                                      'tbcdid': 'tbcdid', 'action': 'action', 'newsn': 'newsn', 'testing': 'testing',
                                      'deployment_uuid': 'deployment_uuid'}
TBSDEPLOYED_CSV_TO_DEPLOYMENTS_LOG = {'talkingbookid': 'sn', 'recipientid': 'recipientid',
                                      'deployedtimestamp': 'timestamp', 'project': 'project',
                                      'deployment': 'deployment', 'contentpackage': 'package', 'firmware': 'firmware',
                                      'location': 'location', 'latitude':'latitude', 'longitude':'longitude', 'username': 'username',
                                      'tbcdid': 'tbcdid', 'action': 'action', 'newsn': 'newsn', 'testing': 'testing',
                                      'deployment_uuid': 'deployment_uuid'}

TBS_DEPLOYED_DEFAULTS = {'newsn': 'f', 'testing': 'f', 'latitude': '', 'longitude': ''}

TBSCOLLECTED_CSV_COLUMNS: list[str] = [
    'talkingbookid', 'recipientid', 'collectedtimestamp', 'project', 'deployment', 'contentpackage', 'firmware',
    'location', 'latitude', 'longitude', 'username', 'tbcdid', 'action', 'testing', 'deployment_uuid', 'collection_uuid'
]
TBDATA_LOG_TO_TBSCOLLECTED_CSV = {
    'in_sn': 'talkingbookid', 'in_recipientid': 'recipientid', 'timestamp': 'collectedtimestamp',
    'in_project': 'project', 'in_deployment': 'deployment', 'in_package': 'contentpackage', 'in_firmware': 'firmware',
    'location': 'location', 'latitude':'latitude', 'longitude':'longitude', 'username': 'username', 'tbcdid': 'tbcdid',
    'action': 'action', 'in_testing': 'testing', 'in_deployment_uuid': 'deployment_uuid',
    'stats_uuid': 'collection_uuid'
}
TBSCOLLECTED_CSV_TO_TBDATA_LOG = {'talkingbookid': 'in_sn', 'recipientid': 'in_recipientid',
                                  'collectedtimestamp': 'timestamp', 'project': 'in_project',
                                  'deployment': 'in_deployment', 'contentpackage': 'in_package',
                                  'firmware': 'in_firmware', 'location': 'location', 'latitude':'latitude', 'longitude':'longitude',
                                  'username': 'username', 'tbcdid': 'tbcdid', 'action': 'action',
                                  'testing': 'in_testing', 'deployment_uuid': 'in_deployment_uuid',
                                  'collection_uuid': 'stats_uuid'}

TBS_COLLECTED_DEFAULTS = {
    'latitude': '', 'longitude': '', 'testing': 'f', 'deployment_uuid': '',
}
STATS_COLLECTED_PROPERTIES_TO_TBSCOLLECTED_CSV = {'deployment_TALKINGBOOKID': 'talkingbookid',
                                                  'deployment_RECIPIENTID': 'recipientid',
                                                  'TIMESTAMP': 'collectedtimestamp',
                                                  'deployment_PROJECT': 'project',
                                                  'deployment_DEPLOYMENT': 'deployment',
                                                  'deployment_PACKAGE': 'contentpackage',
                                                  'deployment_FIRMWARE': 'firmware', 'deployment_LOCATION': 'location',
                                                  # missing coordinates
                                                  'USEREMAIL': 'username', 'TBCDID': 'tbcdid',
                                                  'ACTION': 'action',
                                                  'deployment_TESTDEPLOYMENT': 'testing',
                                                  'deployment_DEPLOYEDUUID': 'deployment_uuid',
                                                  'STATSUUID': 'collection_uuid',
                                                  }


def _nameof(x) -> str:
    if isinstance(x, Path):
        return x.name
    return str(x)


def decode_properties(data: str, sep='=', comment_char='#') -> dict[str, str]:
    props: dict[str, str] = {}
    lines = data.split('\n')
    for line in lines:
        ls = line.strip()
        if ls and not ls.startswith(comment_char):
            key_value = re.split(sep, ls, 1)
            key = key_value[0].strip()
            value = key_value[1].strip().strip('"')
            props[key] = value
    return props


def load_properties_file(filepath, sep='=', comment_char='#') -> Dict[str, str]:
    """
    Read the file passed as parameter as a properties file. Compatible with Java .properties files.
    """
    props: dict[str, str] = {}
    with open(filepath, "rt") as f:
        for line in f:
            ls = line.strip()
            if ls and not ls.startswith(comment_char):
                key_value = ls.split(sep, 1)
                key = key_value[0].strip()
                value = key_value[1].strip().strip('"')
                props[key] = value
    if len(props) == 0:
        print(f'Properties file {_nameof(filepath)} is empty.')
    return props


_db_connection = None
_db_recipientinfo = {}


def _query_recipient_language(recipientid: str) -> str:
    """
    Given a recipientid, look up the language in the recipients table. Cache the value, since it'll probably
    be needed again.
    :param recipientid: The recipientid for which to query the language.
    :return: The language for that recipient.
    """
    global _db_connection, _db_recipientinfo
    if language := _db_recipientinfo.get(recipientid):
        return language
    if _db_connection == None:
        _db_connection = get_db_engine().connect()

    command = text('SELECT language FROM recipients WHERE recipientid=:recipientid;')
    values = {'recipientid': recipientid}
    result = _db_connection.execute(command, values)
    for row in result:
        language = row['language']
    _db_recipientinfo[recipientid] = language
    return language


class TbCollectedData:
    def __init__(self, zip_file_path: Path, verbose: int = 0, timestamp_str=None, **kwargs):
        """

        :param zip_file_path: The path to the TalkingBookData.zip file, or to
            a directory into which it has been expanded.
        :param kwargs: Optional values
        """
        self._verbose = verbose
        self._stats_collected_properties_data = None
        if zip_file_path.is_dir():
            zip_file_path = zip_file_path.absolute()  # resolve /./, /../, etc.
            self._zip_file_name = kwargs.get('zip_file_name', str(zip_file_path))
            self._tb_collected_data_path = zip_file_path
            timestamp_str = timestamp_str or zip_file_path.name
        else:
            self._zip_file_name = str(zip_file_path)
            self._temp_dir = tempfile.TemporaryDirectory(prefix=zip_file_path.name)
            temp_path = Path(self._temp_dir.name)
            if self._verbose >= 2:
                print(f'Expanding into {self._temp_dir.name}')
            try:
                with ZipFile(zip_file_path) as zipfile:
                    zipfile.extractall(path=temp_path)
            except BadZipFile:
                # just try again -- remote file may need downloading.
                with ZipFile(zip_file_path) as zipfile:
                    zipfile.extractall(path=temp_path)

            self._tb_collected_data_path: Path = Path(temp_path, zip_file_path.with_suffix('').name)
            timestamp_str = timestamp_str or zip_file_path.with_suffix('').name

        self._data_source_name = kwargs.get('data_source_name', self._zip_file_name)
        self._num_tbs_deployed = 0
        self._num_tbs_collected = 0

        stats_collected_properties_path = Path(self._tb_collected_data_path, 'stats_collected.properties')
        if not stats_collected_properties_path.exists():
            # try OperationalData subdirectory
            stats_collected_properties_path = Path(self._tb_collected_data_path, 'OperationalData',
                                                   'stats_collected.properties')
        if stats_collected_properties_path.exists():
            self._stats_collected_properties_data: dict[str, str] = load_properties_file(
                stats_collected_properties_path)
        self._timestamp = dateutil.parser.parse(timestamp_str)

    @staticmethod
    def load_log_file(log_file_path: Path):
        """
        NOT the log files on the Talking Book, rather the .log files created by the TB-Loader when it retrieves
        statistics from the TB.

        statsData.log, tbData.log, and deployments.log are created by the TB-Loader whenever a collection or deployment
        operation is conducted.  These log files are a sequence of timestamp, operation, and a list of key:value pairs.

        The statsData.log contains these fields:
            elapsedTime, action, tbcdid, username, useremail, project, update_date_time, out_synch_dir,
            location, duration_sec, in_sn, in_deployment, in_package, in_firmware, in_community, in_project,
            in_update_timestamp, in_synchdir, in_disk_label, disk_corrupted, in_recipientid, in_testing,
            statsonly, deployment_uuid, stats_uuid,

        The tbData.log contains these:
            elapsedTime, action, tbcdid, username, useremail, project, update_date_time,
            out_synch_dir, location, duration_sec, out_sn, out_deployment, out_package,
            out_firmware, out_community, out_rotation, out_project, out_testing, out_recipientid,
            in_recipientid, in_sn, in_deployment, in_package, in_firmware, in_community,
            in_project, in_update_timestamp, in_synchdir, in_disk_label, disk_corrupted,
            in_testing, in_deployment_uuid, out_deployment_uuid, stats_uuid, append,

        And deployments.log contains these:
            elapsedTime, action, tbcdid, username, useremail, sn, newsn,
            project, deployment, package, community, firmware, location,
            timestamp, duration, testing, deploymentnumber, recipientid,
            prev_deployment_uuid, deployment_uuid,

        :param log_file_path: The log file to be read, if it exists.
        :return: a list of dicts from the lines in the log file.
        """

        def process_line(line: str) -> Optional[Dict]:
            """
            Read one line, parse it, and return as a dict.
            :param line: The line to parse.
            :return: A dict, or None if the line can't be parsed.
            """
            # Split the fields, and if there's no data, return None
            parts: list[str] = line.strip().split(',')
            if len(parts) < 3:
                return
            data = {}
            # These are the key:value pairs
            for ix in range(2, len(parts)):
                (k, v) = parts[ix].split(':', 1)
                data[k] = v
            return data

        if not log_file_path.exists():
            print(f'File {str(log_file_path)} not found')
            return None
        with log_file_path.open() as log_file:
            result = []
            # Should be only one, but architecturally there could be multiples.
            for log_line in log_file:
                if ll := process_line(log_line):
                    result.append(ll)
        if len(result) == 0:
            print(f'Log file {_nameof(log_file_path)} is empty.')
        return result

    def _load_collected_talking_book_files(self):
        def find_or_unzip_tb_data() -> Optional[Path]:
            # Unzip the TalkingBookData, which contains log files, package_data.txt, and other data.
            found_tb_data: Path = Path(self._tb_collected_data_path, 'TalkingBookData')
            if not found_tb_data.exists():
                tb_zip: Path = Path(self._tb_collected_data_path, 'TalkingBookData.zip')
                if tb_zip.exists():
                    with ZipFile(tb_zip) as zipfile:
                        zipfile.extractall(path=found_tb_data)
                if not found_tb_data.exists():
                    print(f'No TalkingBookData in {self._zip_file_name}.')
                    return
                if (nested := Path(found_tb_data, "TalkingBookData")).exists():
                    found_tb_data = nested
            return found_tb_data

        def numeric_sort(fn: Path) -> str:
            """
            To properly sort files with names like "log_2.txt" and "log_10.txt". Turns the numbers into 4-digit ones.
            :param fn: the file's name
            :return: an appropriate sort key
            """
            if match := numeric_suffix.match(fn.name):
                # prepend '0' to make 4 digits
                n = max(0, 4 - len(match.group(1)))
                return '0' * n + match.group(1)
            return fn.name

        numeric_suffix = re.compile(r'.*_(\d+).txt')

        # Get the TalkingBookData, the pseudo-image of the Talking Book file system.
        if not (tb_data := find_or_unzip_tb_data()):
            return

        # Read packages_data.txt, so we know the packages, playlists, messages, and prompts
        packages_data_path: Path = Path(tb_data, 'content', 'packages_data.txt')
        if not packages_data_path.exists():
            if self._verbose:
                print(f'No packages_data.txt in {self._zip_file_name} from {self._data_source_name}')
            return
        self._packages_data: Deployment = packagesdata.read_packages_data(packages_data_path,
                                                                          collected_data_zip_name=self._data_source_name)

        # Read the stats_collected.properties file; info about the collection and about the original deployment.
        # We may already have it, in which case we don't need to read it again.
        if not self._stats_collected_properties_data:
            properties_path: Path = Path(tb_data, 'stats_collected.properties')
            self._stats_collected_properties_data: dict[str, str] = load_properties_file(properties_path)

        # Create the Statistics object, to accumulate info from the log files.
        self._play_statistics: Statistics = Statistics(self._stats_collected_properties_data)
        # Read the log files.
        logs_path: Path = Path(tb_data, 'log')
        if not logs_path.exists():
            logs_path = Path(tb_data, 'LOG')
        if logs_path.is_dir():
            log_paths: List[Path] = sorted(logs_path.iterdir(), key=numeric_sort)

            num_errors = 0
            latest_time: Optional[datetime] = None
            for log_path in log_paths:
                lr: LogFileReader = LogFileReader(log_path, self._packages_data, latest_time,
                                                  properties=self._stats_collected_properties_data,
                                                  play_statistics=self._play_statistics, verbose=self._verbose,
                                                  collected_data_zip_name=self._data_source_name)
                lr.read()
                num_errors += lr.num_errors
                latest_time = lr.latest_time

            if num_errors > 0:
                print(f'{num_errors} processing {self._zip_file_name} from {self._data_source_name}')
            else:
                print(f'Processed stats in {self._zip_file_name} from {self._data_source_name}')
        else:
            print(f'No "log" directory in {self._zip_file_name} from {self._data_source_name}')

    @property
    def packages_data(self):
        return self._packages_data

    @property
    def has_statistics(self) -> bool:
        try:
            return self._play_statistics and len(self._play_statistics._play_statistics) > 0
        except Exception:
            pass
        return False

    def save_playstatistics_csv(self, playstatistics_file):
        """
        After the v2 log files have been parsed, this writes a playstatistics.csv file.
        :param playstatistics_file: File to which to write the data.
        :return: None
        """
        self._play_statistics.emit(self._stats_collected_properties_data, playstatistics_file)

    @property
    def playstatistics(self) -> List[Dict]:
        try:
            return self._play_statistics.get_playstatistics(self._stats_collected_properties_data)
        except:
            return []

    @property
    def talkingbookid(self) -> str:
        try:
            return self._stats_collected_properties_data.get('deployment_TALKINGBOOKID')
        except:
            return None

    @property
    def programid(self) -> str:
        try:
            return self._stats_collected_properties_data.get('deployment_PROJECT')
        except:
            return None

    @property
    def deployment_num(self) -> int:
        try:
            return int(self._stats_collected_properties_data.get('deployment_DEPLOYMENT_NUMBER'))
        except:
            return None

    @property
    def stats_collected_properties(self):
        return {k: v for k, v in self._stats_collected_properties_data.items()}

    @property
    def stats_collected_tbcdid(self):
        try:
            tbcdid = self._stats_collected_properties_data.get('TBCDID')
            if len(tbcdid) == 4:
                tbcdid = f'tbcd{tbcdid}'
            return tbcdid
        except:
            return 'unknown-tbid'

    @property
    def stats_collected_useremail(self):
        try:
            useremail = self._stats_collected_properties_data.get('USEREMAIL')
            return useremail
        except:
            return 'unknown user'

    @property
    def deployment_language(self):
        language = self._stats_collected_properties_data.get('deployment_LANGUAGECODE')
        if not language:
            recipientid = self._stats_collected_properties_data.get('deployment_RECIPIENTID')
            language = _query_recipient_language(recipientid)
        return language

    @property
    def num_uf_files(self) -> int:
        uf_path = Path(self._tb_collected_data_path, 'userrecordings')
        n_uf = 0
        if uf_path.exists() and uf_path.is_dir():
            n_uf = len([x for x in uf_path.glob('*.wav')])
        return n_uf

    @property
    def is_stats_only(self) -> bool:
        """
        Returns True if the collected data is DEFINIETLY stats-only. If don't know, returns False
        """
        try:
            return self._stats_collected_properties_data.get('ACTION', '').startswith('stats')
        except:
            return False

    @property
    def num_tbs_collected(self) -> int:
        return self._num_tbs_collected

    @property
    def num_tbs_deployed(self) -> int:
        return self._num_tbs_deployed

    @property
    def num_playstatistics(self) -> int:
        try:
            return len(self._play_statistics._play_statistics)
        except:
            return 0

    @property
    def _operational_data_path(self) -> Path:
        # todo: support override?
        op_data: Path = Path(self._tb_collected_data_path, 'OperationalData')
        if not op_data.exists():
            print(f'No OperationalData in {self._zip_file_name}.')
        return op_data

    @property
    def _tbdata_log_path(self) -> Path:
        # todo: support override?
        p = [x for x in self._operational_data_path.glob('**/tbData*.log')]
        if len(p) >= 1:
            return p[0]
        p = [x for x in self._operational_data_path.glob('**/tbDataAll.kvp')]
        if len(p) >= 1:
            return p[0]
        # Doesn't exist, but is better for messages.
        return Path(self._operational_data_path, 'tbData.log')

    @property
    def tbdata_log(self) -> dict:
        def is_valid_tb_data() -> bool:
            """
            Looks at the data in a tblog to see if it appears to be a valid collection. Basically if many of the
            in-XXX values are "UNKNOWN", there's no collection taking place.
            :param log_to_check: the log to examine
            :return: True if it looks OK
            """
            if not tbdata_logs or len(tbdata_logs) != 1:
                return False
            keys = ['in_recipientid', 'in_deployment', 'in_package', 'in_community',
                    'in_update_timestamp', 'in_sn']
            values = [tbdata_logs[0].get(k) for k in keys]
            good = [x is not None and x != 'UNKNOWN' and x != '-- TO BE ASSIGNED --' for x in values]
            return all(good)

        tbdata_logs = self.load_log_file(self._tbdata_log_path)
        return tbdata_logs[0] if is_valid_tb_data() else None

    @property
    def _deployments_log_path(self) -> Path:
        # v2:OperationalData/deployments.log or v1:OperationalData/0086/tbData/deployments-2022y12m10d-0086.log
        p = [x for x in self._operational_data_path.glob('**/deployments*.log')]
        if len(p) >= 1:
            return p[0]
        # v1: ${ts_dir}/deploymentsAll.kvp
        p = [x for x in self._operational_data_path.glob('**/deploymentsAll.kvp')]
        if len(p) >= 1:
            return p[0]
        # Doesn't exist, but is better for messages.
        return Path(self._operational_data_path, 'deployments.log')

    @property
    def deployments_log(self) -> dict:
        deployments_logs = self.load_log_file(self._deployments_log_path)
        return deployments_logs[0] if deployments_logs else None

    @property
    def tbscollected(self) -> Optional[dict]:
        if list(TBSCOLLECTED_CSV_TO_TBDATA_LOG.keys()) != TBSCOLLECTED_CSV_COLUMNS:
            raise Exception("tbscollected.csv columns mismatch")
        data = None
        try:
            if tbdata_log := self.tbdata_log:
                cols = TBSCOLLECTED_CSV_COLUMNS
                map = TBSCOLLECTED_CSV_TO_TBDATA_LOG
                data = {c: (tbdata_log[map[c]] if map[c] in tbdata_log else None) for c in cols}

                # Prefer the values in stats_collected.properties. Use map of {stats collected_name : tbscollected_name}
                for stc_name, tbc_name in STATS_COLLECTED_PROPERTIES_TO_TBSCOLLECTED_CSV.items():
                    if stc_name in self._stats_collected_properties_data:
                        if not data.get(tbc_name) and self._stats_collected_properties_data.get(stc_name):
                            if self._verbose >= 2:
                                print(
                                    f'Filling missing property, tbData[{tbc_name}] = {self._stats_collected_properties_data.get(stc_name)}')
                            data[tbc_name] = self._stats_collected_properties_data.get(stc_name)
                for k, v in TBS_COLLECTED_DEFAULTS.items():
                    if k not in data:
                        data[k] = v
                if 'talkingbookid' not in data or not data['talkingbookid']:
                    print(f'No talking book id: {data}')
                    return None  # nothing else to do if we don't know what TB. For v2 means corrupt data.
                self._fix_content_package(data)
        except Exception as ex:
            traceback.print_exc()
            print(f'Exception creating tbscollected.csv from {self._zip_file_name}: {ex}')
            return None
        return data

    @property
    def tbsdeployed(self) -> Optional[dict]:
        if list(TBSDEPLOYED_CSV_TO_DEPLOYMENTS_LOG.keys()) != TBSDEPLOYED_CSV_COLUMNS:
            raise Exception("tbsdeployed.csv columns mismatch")
        data = None
        try:
            if deployments_log := self.deployments_log:
                cols = TBSDEPLOYED_CSV_COLUMNS
                map = TBSDEPLOYED_CSV_TO_DEPLOYMENTS_LOG
                data = {c: (deployments_log[map[c]] if map[c] in deployments_log else None) for c in cols}

                for k, v in TBS_DEPLOYED_DEFAULTS.items():
                    if k not in data:
                        data[k] = v
                if 'talkingbookid' not in data or not data['talkingbookid']:
                    print(f'No talking book id: {data}')
                    return None  # nothing else to do if we don't know what TB. For v2 means corrupt data.
                self._fix_content_package(data)
        except Exception as ex:
            traceback.print_exc()
            print(f'Exception creating tbsdeployed.csv from {self._zip_file_name}: {ex}')
            return None
        return data

    def _fix_content_package(self, data: Dict) -> None:
        """
        Eliminate secondary packages, because they mess up statistics
        :param data: a dict that may contain a contentpackage needing fixing. Fixed in-place.
        """
        content_package = data['contentpackage']
        packages = re.split(r'[,;]', content_package)
        if len(packages) > 1:
            data['contentpackage'] = packages[0]

    def save_operational_csvs(self, **kwargs):
        """

        :param kwargs: Arguments to the function
        :return:  None
        """

        # def fix_content_package(data: Dict) -> None:
        #     """
        #     Eliminate secondary packages, because they mess up statistics
        #     :param data: a dict that may contain a contentpackage needing fixing. Fixed in-place.
        #     """
        #     content_package = data['contentpackage']
        #     packages = re.split(r'[,;]', content_package)
        #     if len(packages) > 1:
        #         data['contentpackage'] = packages[0]

        # def is_valid_collection_tb_data(log_to_check) -> bool:
        #     """
        #     Looks at the data in a tblog to see if it appears to be a valid collection. Basically if many of the
        #     in-XXX values are "UNKNOWN", there's no collection taking place.
        #     :param log_to_check: the log to examine
        #     :return: True if it looks OK
        #     """
        #     keys = ['in_recipientid', 'in_deployment', 'in_package', 'in_community',
        #             'in_update_timestamp', 'in_sn']
        #     values = [log_to_check[0].get(k) for k in keys]
        #     good = [x is not None and x != 'UNKNOWN' and x != '-- TO BE ASSIGNED --' for x in values]
        #     return all(good)

        # # noinspection PyShadowingNames
        # def build_tbscollected(tbscollected_csv_file, tbdata_log) -> bool:
        #     try:
        #         if tbdata_log:
        #             # There is only ever one entry.
        #             data = {TBDATA_LOG_TO_TBSCOLLECTED_CSV[k]: v for k, v in tbdata_log[0].items() if
        #                     k in TBDATA_LOG_TO_TBSCOLLECTED_CSV}
        #             # Prefer the values in stats_collected.properties.
        #             for f, t in STATS_COLLECTED_PROPERTIES_TO_TBSCOLLECTED_CSV.items():
        #                 if f in self._stats_collected_properties_data:
        #                     if not data.get(t) and self._stats_collected_properties_data.get(f):
        #                         if self._verbose >= 2:
        #                             print(
        #                                 f'Filling missing property, tbData[{t}] = {self._stats_collected_properties_data.get(f)}')
        #                         data[t] = self._stats_collected_properties_data.get(f)
        #             for k, v in TBS_COLLECTED_DEFAULTS.items():
        #                 if k not in data:
        #                     data[k] = v
        #             if 'talkingbookid' not in data or not data['talkingbookid']:
        #                 print(f'No talking book id: {data}')
        #                 return False  # nothing else to do if we don't know what TB. For v2 means corrupt data.
        #             self._fix_content_package(data)
        #             print(','.join([escape_csv(data[x]) for x in TBSCOLLECTED_CSV_COLUMNS]), file=tbscollected_csv_file)
        #     except Exception as ex:
        #         traceback.print_exc()
        #         print(f'Exception creating tbscollected.csv from {self._zip_file_name}: {ex}')
        #         return False
        #     return True

        # # noinspection PyShadowingNames
        # def build_tbsdeployed(tbsdeployed_csv_file, deployments_log) -> bool:
        #     try:
        #         if deployments_log:
        #             # There is only ever one entry.
        #             data = deployments_log[0]
        #             data = {DEPLOYMENTS_LOG_TO_TBSDEPLOYED_CSV[k]: v for k, v in data.items() if
        #                     k in DEPLOYMENTS_LOG_TO_TBSDEPLOYED_CSV}
        #             for k, v in TBS_DEPLOYED_DEFAULTS.items():
        #                 if k not in data:
        #                     data[k] = v
        #             if 'talkingbookid' not in data or not data['talkingbookid']:
        #                 print(f'No talking book id: {data}')
        #                 return False  # nothing else to do if we don't know what TB. For v2 means corrupt data.
        #             self._fix_content_package(data)
        #             print(','.join([escape_csv(data[x]) for x in TBSDEPLOYED_CSV_COLUMNS]), file=tbsdeployed_csv_file)
        #     except Exception as ex:
        #         traceback.print_exc()
        #         print(f'Exception creating tbsdeployed.csv from {self._zip_file_name}: {ex}')
        #         return False
        #     return True

        # noinspection PyShadowingNames
        def do_save(file_spec, default_name: str, headings: List[str] = None, csv_dict: dict = None):
            csv_data = ','.join([escape_csv(csv_dict[c]) for c in headings])
            if isinstance(file_spec, io.IOBase):
                # We were given a file, so write to it.
                print(csv_data, file=file_spec)
            else:
                if isinstance(file_spec, Path):
                    # We were given a Path; if a file, write to it, if a directory, create a file under it.
                    if file_spec.is_dir():
                        file_path = Path(file_spec, default_name)
                    else:
                        file_path = file_spec
                else:
                    # Something else. If "Path()" understands it, all's good.
                    file_path = Path(file_spec)
                need_header = not file_path.exists()
                with file_path.open('a') as file:
                    if need_header:
                        print(','.join(headings), file=file)
                    print(csv_data, file=file)

        if (tbscollected_csv := kwargs.get('tbscollected')):
            if (tsbcollected_data := self.tbscollected):
                do_save(tbscollected_csv, 'tbscollected.csv', headings=TBSCOLLECTED_CSV_COLUMNS,
                        csv_dict=tsbcollected_data)
                self._num_tbs_collected += 1
            else:
                # Why not?
                missing = []
                if not self._stats_collected_properties_data: missing.append('stats_collected.properties file')
                if not self.tbdata_log: missing.append(f'{self._tbdata_log_path.name} file')
                print(f'Unable to find (or read) {" or ".join(missing)}; can not create {_nameof(tbscollected_csv)}')

        if tbsdeployed_csv := kwargs.get('tbsdeployed'):
            if (self._stats_collected_properties_data and
                    self._stats_collected_properties_data.get('ACTION', '').startswith('stats')):
                if self._verbose:
                    print(f'Stats only, will not create {_nameof(tbsdeployed_csv)}')
            else:
                if (tsbdeployed_data := self.tbsdeployed):
                    do_save(tbsdeployed_csv, 'tbsdeployed.csv', headings=TBSDEPLOYED_CSV_COLUMNS,
                            csv_dict=tsbdeployed_data)
                    self._num_tbs_deployed += 1
                else:
                    # Why not?
                    missing = []
                    if not self.self.deployments_log: missing.append(f'{self._deployments_log_path.name} file')
                    print(f'Unable to find (or read) {" or ".join(missing)}; can not create {_nameof(tbsdeployed_csv)}')

    def process_tb_collected_data(self):
        """
        Process the collected data from one Talking Book, one collection event.
        :return: None
        """
        try:
            self._load_collected_talking_book_files()
        except Exception as ex:
            traceback.print_exc()
            print(f'Exception processing usage {self._zip_file_name}: {ex}')
