import re
import traceback
from datetime import datetime
from pathlib import Path

# h_mm_ss.t RecType[,] [...]
# from tbstats import LogRecord, Deployment, LogContext, Statistics
from tbstats.logrecord import parse_log_record
from tbstats.packagesdata import Deployment
from tbstats.logcontext import LogContext
from tbstats.statistics import Statistics

verbose = 0

_LOG_REC = re.compile(
    r'^(?P<hours>\d+)_(?P<minutes>\d+)_(?P<seconds>\d+)\.(?P<tenths>\d):\s*(?P<type>\w+)[, ]*(?P<params>.*)$')


class LogFileReader():
    def __init__(self, log_file_path: Path, packages_data: Deployment, base_time: datetime = None,
                 properties: dict[str, str] = None, play_statistics: Statistics = None, **kwargs):
        """
        A LogFileReader reads and processes one log file.
        :param log_file_path: Path of the file to be processed.
        :param packages_data: The packages_data.txt file from the deployment.
        :param base_time: The latest time found in a previous log. This log should not contain
            any entries before the base_time.
        :param properties: A key:value pair list of properties from the statistics collection.
            Also includes the deployment properties, like "deployment_PROPERTY=VALUE"
        """
        global verbose
        verbose = kwargs.get('verbose', verbose)
        if not log_file_path.exists():
            raise Exception(f'Log file {log_file_path} does not exist.')

        self._log_file_path: Path = log_file_path
        self._log_file_name = log_file_path.name
        self._data_source_name = kwargs.get('data_source_name')

        # TODO: do something with the given "base_time", the latest timestamp of the previous log file.
        self._context = LogContext(packages_data=packages_data, logfile=log_file_path)
        self._properties: dict[str, str] = properties
        self._play_statistics: Statistics = play_statistics
        self._kwargs = kwargs
        self._errors = 0

    @property
    def num_errors(self):
        return self._errors

    @property
    def latest_time(self):
        return self._context.latest_time

    def read(self):
        # The log files are ascii when well formed. "badLog_N.txt", by definition, has non-ascii characters.
        # However, most of the lines may be good, so try to read them.
        with self._log_file_path.open('rb') as log_file:
            line_number = 0
            try:
                for raw_line in log_file:
                    # TODO: This is stupid. There should be a way to line.decode('XYZ') that simply copies bytes.
                    #   If there is such a thing the documentation is well obscured.
                    line = (''.join([chr(x) for x in raw_line])).strip()
                    line_number += 1
                    log_record = parse_log_record(line, line_number, log_context=self._context,
                                                  play_statistics=self._play_statistics,
                                                  log_file_name=self._log_file_path.name, **self._kwargs)
                    self._errors += log_record.num_errors if log_record else 0
            except Exception as ex:
                traceback.print_exc()
                print(f'Error reading log file on or after line {line_number} in {self._log_file_name} from {self._data_source_name}')
        if self._context.current_message is not None:
            print(f'EOF with open message {self._context.current_message.id} in {self._log_file_name} from {self._data_source_name}')

        # if self._play_stats_file:
        #     self._statistics.emit(self._properties, self._play_stats_file)
        # print(self._statistics)