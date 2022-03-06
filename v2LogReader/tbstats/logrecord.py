import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

# from tbstats import LogContext, Playlist, Message, Statistics
from tbstats.logcontext import LogContext
from tbstats.packagesdata import Playlist, Message
from tbstats.statistics import Statistics

# Matches timestamp: type params
# Timestamp: hours_minutes_seconds.tenths or hours_minutes_seconds.milliseconds
_LOG_REC = re.compile(
    r'^(?P<hours>\d+)_(?P<minutes>\d+)_(?P<seconds>\d+)\.(?:(?P<tenths>\d)|(?P<milliseconds>\d{3})):\s*(?P<type>\w+)[, ]*(?P<params>.*)$')

verbose = 0


@dataclass
class LogType:
    # The type of the log record.
    cls: callable
    # Does it (implicitly) adjust the running timestamps?
    adjusts_time: bool


_LOG_TYPES: dict[str, LogType] = {}


# def repeat(_func=None, *, num_times=2):
#     def decorator_repeat(func):
#         @functools.wraps(func)
#         def wrapper_repeat(*args, **kwargs):
#             for _ in range(num_times):
#                 value = func(*args, **kwargs)
#             return value
#         return wrapper_repeat
#
#     if _func is None:
#         return decorator_repeat
#     else:
#         return decorator_repeat(_func)


def log_record(cls):
    """
    A decorator for log record types. Registers the handler for the record type in _LOG_TYPES. For log record
    types that adjust the running time tracker.
    :param cls: The class that implements the handler for this log record type.
    :return: The class
    """
    name = cls.__name__
    if name[0:4] == 'Log_':
        name = name[4:]
    _LOG_TYPES[name] = LogType(cls, True)
    return cls


def log_record_no_adjust(cls):
    """
    A decorator for log record types. Registers the handler for the record type in _LOG_TYPES. For log record
    types that DO NOT adjust the running time tracker.
    :param cls: The class that implements the handler for this log record type.
    :return: The class
    """
    name = cls.__name__
    if name[0:4] == 'Log_':
        name = name[4:]
    _LOG_TYPES[name] = LogType(cls, False)
    return cls


def datetime_from_RTC(to_adjust: Dict[str, int]) -> Optional[datetime]:
    """
    For some reason the date/time on the RTC records sometimes has an
    hour value of "24", but datetime() only accepts hours in 0-23. This
    function accommodates those times.
    :param to_adjust: A dict of values to construct a datetime(), but
        possibly with a value of "hour" that is too large.
    :return: A datetime() constructed & adjusted with corrected values
    """
    days_adjust = 0
    while to_adjust['hour'] > 23:
        to_adjust['hour'] -= 24
        days_adjust += 1
    try:
        result: datetime = datetime(**to_adjust)
    except:
        return None
    if days_adjust:
        result += timedelta(days=days_adjust)
    return result


@log_record
class LogRecord:
    def __init__(self, **kwargs):
        self._kwargs = kwargs
        self._match = kwargs['match']  # the regular expression that matched the record
        self._log_context = kwargs['context']
        self._line_number = kwargs['line_number']
        self._verbose = kwargs.get('verbose', 0)
        self._errors = 0
        millis = self._match['milliseconds']
        tenths = self._match['tenths']
        milliseconds = int(millis) if millis is not None else int(tenths) * 100
        et = {'hours': int(self._match['hours']),
              'minutes': int(self._match['minutes']),
              'seconds': int(self._match['seconds']),
              'milliseconds': milliseconds}

        self._timestamp = None  # until we know better
        self._elapsed_time_since_boot: timedelta = timedelta(**et)
        if not self._log_context.awaiting_time:
            self._timestamp = self._log_context.base_day + self._elapsed_time_since_boot
            if self._log_context.latest_time:
                delta = self._log_context.latest_time - self._timestamp
                if delta >= timedelta(hours=2):
                    if self._verbose:
                        print(
                            f'Large time jump: {self._elapsed_time_since_boot} in {kwargs.get("collected_data_zip_name", "??")}')
            # Should the "latest time" be updated with the timestamp of this record?
            if _LOG_TYPES[self._match['type']].adjusts_time and not self._log_context.awaiting_time:
                self._log_context.latest_time = self._timestamp

        # prev_latest_time = self._context.latest_time
        # self._context.latest_time = (
        #             self._context.base_time + self._elapsed_time_since_boot) if self._context.base_time else None
        # if self._context.latest_time and prev_latest_time:
        #     delta = self._context.latest_time - prev_latest_time
        #     if delta >= timedelta(hours=2):
        #         print(f'Large time jump: {self._elapsed_time_since_boot}')
        #
        # # Should the "latest time" be updated with the timestamp of this record?
        # if _LOG_TYPES[self._match['type']].adjusts_time and self._context.base_time:
        #     self._context.latest_time = self._context.base_time + self._elapsed_time_since_boot

    def log_error(self, msg):
        self._errors += 1
        if self._verbose:
            print(f'{self._log_context.logfile.name}:{self._line_number}: '
                  f'{msg} in {self._kwargs.get("collected_data_zip_name", "??")}')

    @property
    def num_errors(self):
        return self._errors

    @property
    def timestamp(self):
        if self._timestamp:
            return self._timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
        return str(self._elapsed_time_since_boot)

    @property
    def label(self):
        return f'{self._line_number}: f{self.timestamp}'


@log_record_no_adjust
class Log_REBOOT(LogRecord):
    # 0_00_00.2: REBOOT --------, BootKey: ' '
    # 12_28_52.976: REBOOT --------, BootKeys: '       '
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 0_00_00.4: REBOOT - -------, BootKey: ' '
        if m := re.match(r".*(BootKey|BootKeys): '(.)'", kwargs['params']):
            self._log_context.timestamp_time_of_day = m[1] == 'BootKeys'
            self._boot_key = m[2]
            if self._verbose:
                if self._boot_key == '' or self._boot_key == ' ':
                    print(f'{self.timestamp}: Boot with Tree+Table')
                else:
                    print(f'{self.timestamp}: Boot with key {self._boot_key}')
        self._log_context.reboot()


@log_record_no_adjust
class Log_RTC(LogRecord):
    # OLD STYLE:
    # 0_00_00.2: REBOOT --------, BootKey: ' '
    # 0_00_00.3:      RTC, Dt: 2000-01-05 (Wed), Tm: 02_19_59.000
    # . . .
    # 0_00_01.4:   setRTC, DtTm: '2022-1-11 13:29'
    # . . .
    # 0_00_00.2: REBOOT --------, BootKey: ' '
    # 0_00_00.3:      RTC, Dt: 2022-01-11 (Tue), Tm: 13_29_32.2147483647

    # INTERMEDIATE
    # 0_00_03.2: REBOOT --------, BootKey: ' '
    # 0_00_03.2:      RTC, 2022-02-24 15:47:24.960 (Thu)

    # NEW STYLE:
    # 12_28_52.976: REBOOT --------, BootKeys: '       '
    # 12_28_52.980:      RTC, Dt: 2022-09-06 12:28:52.980

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # 0_00_00.4: RTC, Dt: 2022 - 01 - 28(Fri), Tm: 11_02_54.000
        # This log record tells us the current (possibly just updated) RTC value. This tends to happen a couple
        # hundred milliseconds after the boot, so the rtc needs to be adjusted by the elapsed time stamp of the
        # log record to get the actual base time of this group of log records. The base time should not be
        # allowed to go backwards, so if a backwards adjustment is detected, print an error, and don't change
        # the base time.

        rt = None
        # Match '2022-09-22 09:25:55.123' or 'Dt: 22-09-22 09:25:55.123' or '...09_25_55.123' or '...(Thu)...'
        if m := re.match(r'(?:Dt: )?(\d{4})-(\d{2})-(\d{2})\D*(\d{2})[_:](\d{2})[_:](\d{2})\.(\d{3}).*',
                         kwargs['params']):
            msec = int(m[7]) * 1000  # we have milliseconds but need microseconds
            rt = {i[0]: int(i[1]) for i in
                  zip(['year', 'month', 'day', 'hour', 'minute', 'second'], [m[x] for x in range(1, 7)])}
            rt['microsecond'] = msec

        # Match 'Dt: 2022-09-25 Tm: 09_25_55.123'
        elif m := re.match(r'Dt: (\d{4})-(\d{2})-(\d{2}).*Tm: (\d{2})_(\d{2})_(\d{2})\.(\d{3,6}).*', kwargs['params']):
            rt = {i[0]: int(i[1]) for i in
                  zip(['year', 'month', 'day', 'hour', 'minute', 'second'], [m[x] for x in range(1, 7)])}
            # the fractional part of a second is always '2147483647', which is clearly meaningless. Ignore it.
            # if int(m[7]) != 0:
            #     msec = m[7][0:6]
            #     rt['microsecond'] = int(m[7])*1000

        if rt and rt['year'] >= 2022:  # TB-2 new in 2022; no timestamps before that are valid.
            rtc: datetime = datetime_from_RTC(rt)
            if self._log_context.awaiting_time:
                # (Finally) setting the time after a reboot.
                if rtc is None:
                    self.log_error('RTC record with no time (following DFU?)')
                else:
                    # If the computed base time is greater than the latest time seen, use it as the base. If not,
                    # use the latest time seen as the base.
                    if rtc > datetime.now():
                        self.log_error('Setting time to the future')
                    if self._log_context.latest_time is None or rtc >= self._log_context.latest_time:
                        new_base = datetime(year=rtc.year, month=rtc.month, day=rtc.day)
                        if self._verbose:
                            print(f'{self.timestamp}: RTC setting new base time to {new_base}')
                        if self._log_context.timestamp_time_of_day:
                            self._log_context.base_day = datetime(year=rtc.year, month=rtc.month, day=rtc.day)
                        else:
                            self._log_context.base_day = rtc
                        self._log_context.awaiting_time = False
                        self._log_context.latest_time = rtc
                    elif self._log_context.timestamp_time_of_day:
                        self.log_error(f'Time went backwards : {self._log_context.latest_time} -> {rtc}')
            else:
                if self._log_context.latest_time and rtc < self._log_context.latest_time and self._log_context.timestamp_time_of_day:
                    rtc_delta = self._log_context.latest_time - rtc
                    if rtc_delta > timedelta(seconds=1):
                        self.log_error(f'Time went backwards : {self._log_context.latest_time} -> {rtc}')
                ts_delta = rtc - self._timestamp
                if ts_delta > timedelta(seconds=2):
                    if self._verbose:
                        print(f'RTC record differs from from timestamp: {ts_delta}')


@log_record_no_adjust
class Log_setRTC(LogRecord):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 0_00_00.3:   setRTC, DtTm: '2022-3-9 11:30'
        if m := re.match(r'DtTm: *\'(\d{4})-(\d{1,2})-(\d{1,2})\D*(\d{2})[_:](\d{2}).*', kwargs['params']):
            if int(m[1]) >= 2022:
                rt = {i[0]: int(i[1]) for i in
                      zip(['year', 'month', 'day', 'hour', 'minute'], [m[x] for x in range(1, 6)])}
                rtc = datetime_from_RTC(rt)
                if rtc is None:
                    self.log_error('NO RTC following DFU; keeping latest as base')
                    self._log_context.base_day = self._log_context.latest_time
                else:
                    # This happens after a boot, in response to SetRTC.txt.
                    if self._log_context.timestamp_time_of_day:
                        new_base = datetime(year=rtc.year, month=rtc.month, day=rtc.day)
                    else:
                        new_base = rtc
                    if self._verbose:
                        print(
                            f'{self.timestamp}: setRTC setting new base time to {new_base} in {kwargs.get("collected_data_zip_name", "??")}')
                    self._log_context.base_day = new_base
                    self._log_context.latest_time = rtc


@log_record_no_adjust
class Log_resetRTC(Log_setRTC):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 0_00_09.888: resetRTC, DtTm: '2022-3-20 14:54'


@log_record
class Log_TB_V2(LogRecord):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 0_00_00.4: TB_V2, NEW_Firmware: 'V3.1 of Mon 01/24/22 20:07:38.39'


@log_record
class Log_CPU(LogRecord):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 0_00_00.4: CPU, Id: '0x441.c'
        pass


@log_record
class Log_TB_ID(LogRecord):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 0_00_00.4: TB_ID, Id: '800b.000d.d.Q802314'
        pass


@log_record
class Log_TB_NM(LogRecord):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 0_00_00.4: TB_NM, Nm: '800b.000d.d.Q802314'
        pass


@log_record
class Log_CPU_CLK(LogRecord):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 0_00_00.4: CPU_CLK, MHz: 24
        pass


@log_record
class Log_BUS_CLK(LogRecord):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 0_00_00.4: BUS_CLK, APB2: 24, APB1: 12
        pass


@log_record
class Log_BOOT(LogRecord):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 0_00_00.4: BOOT, cnt: 1
        # Only logs the boot count


@log_record
class Log_NorLog(LogRecord):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 0_00_00.4: NorLog, Idx: 23, Sz: 397, Max: 131067, Free %: 92
        pass


@log_record
class Log_TB_CSM(LogRecord):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 0_00_00.4: TB_CSM, ver: '// TB CSM 2021-09-23: V11 QC test-- skip on Star after Home'
        pass


@log_record
class Log_Deploymt(LogRecord):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 0_00_01.0: Deploymt, Ver: 'DEMO-DL-21-1'
        pass


@log_record
class Log_LdPkg(LogRecord):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 0_00_01.1: LdPkg, nm: 'DEMO-DL-1-en-c', nSubj: 5
        pass


@log_record
class Log_ChgPkg(LogRecord):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 0_00_01.1: ChgPkg, Pkg: 'DEMO-DL-1-en-c'
        m = re.match(r"Pkg: '([a-zA-Z0-9_-]+)'", kwargs['params'])
        if m:
            package_name = m[1]
            package = self._log_context.packages_data.find_package(package_name)
            self._log_context.current_package = package
            if not package:
                self.log_error(
                    f'Could not find package {package_name} in deployment {self._log_context.packages_data.name}')
            if self._verbose:
                print(f'{self.label}: Changed to package {self._log_context.current_package}')


@log_record
class Log_csmEvt(LogRecord):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 0_00_01.6: csmEvt, evt: 'AudioStart', nSt: 'stWelcoming'
        pass


@log_record
class Log_PwrCheck(LogRecord):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 0_00_15.1: PwrCheck, Stat: 'u L9   P- B5 T5 V6'
        # PwrCheck, stat:  'u Lxct Pp Bb Tm Vv'
        #                   |  |||  |  |  |  |
        #                   |  |||  |  |  | Vv   v = current audio volume
        #                   |  |||  |  | Tm      m = -/0/1/2/.../9/+ = MPU temp  (m+2)* 100mV, - if <200mV, + if >1200mV
        #                   |  |||  | Bb         b = -/0/1/2/.../9/+ = Backup  batt voltage 2+b*.2V, - if <2.0V, + if >3.9V
        #                   |  ||| Pp            p = -/0/1/2/.../9/+ = Primary batt voltage 2+p*.2V, - if <2.0V, + if >3.9V
        #                   |  ||t               t = -/0/1/2/.../9/+ = Lithium thermistor  (t+2)* 100mV, - if <200mV, + if >1200mV
        #                   |  |c                c =  /c/C/X  c=charging, C=charged, X=temp fault
        #                   | Lx                 x = -/0/1/2/.../9/+ = Lithium voltage 3.xV, - if <3.0V, + if >3.9V
        #                   u                    u = U/u, U if Usb power is present
        if m := re.match(
                r"Stat: '(?P<usb>.) L(?P<li_v>.)(?P<li_chg>.)(?P<li_t>.) P(?P<pr_v>.) B(?P<bk_v>.) T(?P<temp>.) V(?P<vol>\d+)'",
                kwargs['params']):
            usb = 'USB conntected' if m['usb'] == 'U' else 'no USB'
            li_v = m['li_v']
            if li_v == '-':
                li_volts = '<3.0'
            elif li_v == '+':
                li_volts = '>=4.0'
            else:
                li_volts = f'3.{li_v}'
            li_chg = m['li_chg']
            if li_chg == 'c':
                li_charge_state = 'Charging'
            elif li_chg == 'C':
                li_charge_state = 'Charged'
            else:
                li_charge_state = 'Temp Fault'
            li_t = m['li_t']
            if li_t == ' ':
                li_temp = 'n/c'
            elif li_t == '-':
                li_temp = '<200mV'
            elif li_t == '+':
                li_temp = '>=1200mV'
            else:
                li_temp = str(200 + 100 * int(li_t))
            volume = m['vol']

            if self._verbose:
                print(
                    f'{self.label}: Power {li_volts}V, {li_charge_state}, {li_temp}, volume {volume}, {usb} {self._log_context._logfile}:{self._line_number}')


@log_record
class Log_PwrDown(LogRecord):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 0_00_33.8: PwrDown
        if self._verbose:
            print(f'{self.timestamp}: Power down')


@log_record
class Log_SvStats(LogRecord):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 0_00_33.8: SvStats, cnt: 0


@log_record
class Log_chngSubj(LogRecord):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


def on_CutPlay(self, natch):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if self._log_context.current_message:
            # 0_00_09.3: CutPlay, ms: 289, pct: 14, nSamp: 20480
            if self._verbose:
                print(f'{self.label}: Cut Play: {self._log_context.message}')


@log_record
class Log_PlayMsg(LogRecord):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 24_12_55.312:  PlayMsg, Subj: 'tutorial', iM: 11
        # 18_17_01.225:  PlayMsg, Msg: Sound Check,q iM: 7, fn: LB-2_6hyolivfgp_kb.mp3
        # play_re = r"(?:Msg|Subj): '?([a-zA-Z0-9_ -]+)'?, iM: (\d+)(?:, fn: ([a-zA-Z0-9_ .-]*))?"

        # 18_17_01.225:  PlayMsg, iS:N, iM:M, fn:fn
        # 12_34_44.046:  PlayMsg, Subj: 'Women's role in Agriculture', iM: 0
        # RE to match any PlayMsg format. m['s'] is subject name; m['is'] is subject index. Only one will appear.
        play_re = r"(?:(?:Msg|Subj): *'?(?P<s>.+?)'?)?(?:i?S: *(?P<is>\d+))?, *i?M: *(?P<im>-?\d+)(?:, fn: *(?P<fn>[a-zA-Z0-9_ .-]*))?"

        m = re.match(play_re, kwargs['params'])
        if not m:
            self.log_error(f'play msg mismatch: {kwargs["params"]}')
            return
        message = None
        playlist = None
        try:
            if m['s']:
                playlist_title = m['s']
                playlist: Playlist = self._log_context.current_package.find_playlist(playlist_title)
            elif m['is']:
                playlist = self._log_context.current_package.get_playlist(int(m['is']))
            message: Message = playlist.get_message(int(m['im']))
        except:
            if m['fn']:
                # We couldn't find the message. Fake the message using the file name.
                fn = Path(m['fn']).stem
                message = Message(0, 0, fn, fn)
        if not message:
            self.log_error(f'Could not find Message for {kwargs["params"]}')
            return
        self._log_context.current_message = message
        self._log_context.current_message_started = self._log_context.latest_time
        if not self._log_context.latest_time:
            self.log_error(f'PlayMsg without starting time')
        # print(f'Play message {self._context.current_message.id}')
        if self._verbose:
            print(f'{self.label}: Play: {self._log_context.message}')


@log_record
class Log_plPause(LogRecord):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if self._log_context.current_message:
            # 0_00_11.5: plPause, ms: 601, pct: 0, nS: 25600
            if self._verbose:
                print(f'{self.label}: Pause: {self._log_context.message}')
            # played = self._log_context.latest_time - self._log_context.current_message_started
            # self._log_context.current_message_started = None


@log_record
class Log_plResume(LogRecord):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if self._log_context.current_message:
            # 3_19_05.6: plResume
            if self._verbose:
                print(f'{self.label}: Resume: {self._log_context.message}')
            # self._log_context.current_message_started = self._log_context.latest_time


@log_record
class Log_adjPos(LogRecord):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if self._log_context.current_message:
            # 18_17_03.789: adjPos, bySec: 45, newMs: 47088
            m = re.match(r"(bySec|byMs|byMsec): (-?\d+), newMs: (-?\d+)", kwargs['params'])
            if m:
                seconds = int(m[2])
                if m[1] != 'bySec':
                    seconds = int(seconds / 1000)
                newMs = max(0, int(m[3]))
                if self._verbose:
                    print(f'{self.label}: Skip: {self._log_context.message} by {seconds} seconds to {newMs} ms.')
                self._log_context.current_message_started = self._log_context.latest_time


@log_record
class Log_MsgDone(LogRecord):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 3_20_09.3: MsgDone, S: 0, M: 0, L_ms: 61420, P_ms: 11486176, nPaus: 1, Fwd_ms: 0, Bk_ms: 0
        # 'S:0, M:0, L_ms:61420, P_ms:-117, nPaus:0, Fwd_ms:0, Bk_ms:0'
        if m := re.match(
                r'S:([-\d]+), M:([-\d]+), L_ms:([-\d]+), P_ms:([-\d]+), nPaus:([-\d]+), Fwd_ms:([-\d]+), Bk_ms:([-\d]+).*',
                kwargs['params']):
            length = int(m[3])
            played = int(m[4])
            num_pauses = int(m[5])
            forward_ms = int(m[6])
            back_ms = int(m[7])
            id = self._log_context.current_message.id if self._log_context.current_message else "No Message"
            if self._verbose:
                print(f'{self.label}: Played message {self._log_context.message}, {played}/{length} ms')
            if self._log_context.current_message and self._log_context.current_message_started:
                # print(f'{self.timestamp}: Played message {id} tracked playing: {self._log_context.current_message_played}')
                kwargs['play_statistics'].played(self._log_context.current_message.id, duration_ms=length,
                                                 played_ms=played)
            else:
                self.log_error('MsgDone with no play context')
        else:
            # What to do if the re fails to match?  Nothing?
            pass
        self._log_context.current_message_started = None
        self._log_context.current_message = None


def parse_log_record(line: str, line_number: int, log_context: LogContext, play_statistics: Statistics, **kwargs):
    match = _LOG_REC.match(line)
    if match:
        # It looks like a good log record. Do we recognize the type?
        rec_type = match['type']
        if rec_type in _LOG_TYPES:
            # Create the log record
            log_type = _LOG_TYPES[rec_type]
            log_kwargs = kwargs | {'match': match, 'context': log_context, 'params': match['params'],
                                   'line_number': line_number,
                                   'play_statistics': play_statistics}
            log_record = log_type.cls(**log_kwargs)
            return log_record
    # print(f'Unmatched line: {line}')
