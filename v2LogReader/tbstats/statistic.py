from dataclasses import dataclass
from pathlib import Path
from typing import IO

import dateutil
from dateutil import parser


@dataclass
class Statistic:
    def id(self):
        pass

    def emit_summary(self, properties: dict[str, str], path: Path):
        pass

    @staticmethod
    def normalize_timestamp(raw: str) -> str:
        timestamp = dateutil.parser.parse(raw)
        # This gives a 6-digit fraction ...
        micro_str = timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f')
        # ... but we need a 3-digit one.
        milli_str = micro_str[:-3] + 'Z'
        return milli_str
