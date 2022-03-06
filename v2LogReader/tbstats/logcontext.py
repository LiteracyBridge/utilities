from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

from tbstats.packagesdata import Deployment, Package, Message


@dataclass
class LogContext:
    # The packages_data.txt file from the content directory.
    packages_data: Deployment
    # The current log file being processed.
    logfile: Path
    # Keys detected at the current boot.
    boot_key: str = None
    # Are the timestamps time-of-day (with +24 hours for additional days), or
    # time-since-boot?
    timestamp_time_of_day: bool = True
    # Time of the current boot (day only; timestamps are time-of-day in the boot day;
    # in subsequent days the hour increases by 24, 48, etc).
    base_day: datetime = None

    # Timestamp of the latest log record processed.
    latest_time: datetime = None
    # Currently selected package (there is always one selected soon after boot).
    current_package: Package = None
    # Current message being played, if any, else None.
    current_message: Message = None
    # The time at which the current message was started, or None if no message.
    current_message_started: datetime = None
    # Captures the state after a REBOOT, but before the time is set.
    awaiting_time: bool = True

    # noinspection PyTypeChecker
    def reboot(self):
        # TODO: Check for open message (or other open context items)
        self.base_day = None
        self.awaiting_time = True
        self.current_package = None
        self.current_message = None
        self.current_message_started = None

    @property
    def message(self) -> str:
        if not self.current_message:
            return "--no message--"
        return f'{self.current_message.id} ({self.current_message.title})'
