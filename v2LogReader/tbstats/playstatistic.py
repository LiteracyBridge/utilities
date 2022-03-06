import re
from dataclasses import dataclass
from pathlib import Path

from tbstats.statistic import Statistic
from utils import escape_csv

PLAY_STATISTICS_COLUMNS: list[str] = [
    'timestamp', 'project', 'deployment', 'contentpackage', 'community', 'talkingbookid', 'contentid', 'started',
    'quarter', 'half', 'threequarters', 'completed', 'played_seconds', 'survey_taken', 'survey_applied',
    'survey_useless', 'tbcdid', 'stats_timestamp', 'deployment_timestamp', 'recipientid', 'deployment_uuid'
]


@dataclass
class PlayStatistic(Statistic):
    # The contentid to which this statistic applies
    content_id: str
    # The number of times the message was started, but didn't reach 1/4
    started: int = 0
    # Tne number of times the message was played to 1/4, but not 1/2
    one_quarter: int = 0
    # The number of times the message was played to 1/2, but not 3/4
    half: int = 0
    # The number of times the message was played to 3/4, but not completion
    three_quarters: int = 0
    # The number of times the message was played to completion
    completions: int = 0
    # The total played seconds
    played_seconds: int = 0
    # The number of surveys started, answered positively, answered negatively
    survey_taken: int = 0
    survey_applied: int = 0
    survey_useless: int = 0

    def played(self, duration_ms: int, played_ms: int):
        self.played_seconds += played_ms / 1000
        # Calculate the fraction completed. Note: at launch, the TB reported a completed play as less than the
        # total time. Observed values are between 97% and 99.2% of the actual play. If the "played" is within
        # 2 seconds of "duration", we'll call it "completed".
        if played_ms > duration_ms - 2000:
            self.completions += 1
        elif played_ms > (duration_ms - 2000) * .75:
            self.three_quarters += 1
        elif played_ms > (duration_ms - 2000) * .5:
            self.half += 1
        elif played_ms > (duration_ms - 2000) * .25:
            self.one_quarter += 1
        elif played_ms > 2000:
            self.started += 1

    def surveyed(self, positive: bool = False, negative: bool = False):
        self.survey_taken += 1
        if positive:
            self.survey_applied += 1
        if negative:
            self.survey_useless += 1

    def emit_played_event(self, duration_ms: int, played_ms: int, properties: [str, dict], outf):
        pass

    def as_dict(self, properties: dict[str, str]) -> dict:
        """
        Emits a "playstatistics" format line of statistics.
        """
        # timestamp,project,deployment,contentpackage,community,talkingbookid,
        # 2022-08-29T16:05:25.000Z,"LANDESA-LR","LANDESA-LR-22-4","LANDESA-LR-4-MEN","FF5494E908CD07ED","C-005C00D1",
        # contentid,started,quarter,half threequarters,completed,played_seconds,survey_taken,survey_applied,survey_useless,
        # "C-005C00D1_9-0_DDCD49C6",0,0,0,0,49,1033,0,0,0,
        # tbcdid,stats_timestamp,deployment_timestamp,recipientid,deployment_uuid
        # "0052",2022-08-29T16:05:25.000,2022-05-17T16:04:59.661,"ff5494e908cd07ed","4f708c68-4ee9-4d30-bc63-2517d1afa5fc"

        deployment_timestamp = Statistic.normalize_timestamp(properties.get('deployment_TIMESTAMP'))
        stats_timestamp = Statistic.normalize_timestamp(properties.get('TIMESTAMP'))
        project = properties.get('deployment_PROJECT')
        deployment = properties.get('deployment_DEPLOYMENT')
        contentpackage = properties.get('deployment_PACKAGE')
        if m := re.match(r'''['"]?([\w-]+)([,;][\w-]+)?['"]?''', contentpackage):
            contentpackage = m[1]
        community = properties.get('deployment_COMMUNITY')
        talkingbookid = properties.get('deployment_TALKINGBOOKID')
        tbcdid = properties.get('TBCDID')  # collection id
        recipientid = properties.get('deployment_RECIPIENTID')
        deployment_uuid = properties.get('deployment_DEPLOYEDUUID')

        result = {
            'timestamp': stats_timestamp,
            'project': project,
            'deployment': deployment,
            'contentpackage': contentpackage,
            'community': community,
            'talkingbookid': talkingbookid,
            'contentid': self.content_id,
            'started': self.started,
            'quarter': self.one_quarter,
            'half': self.half,
            'threequarters': self.three_quarters,
            'completed': self.completions,
            'played_seconds': int(0.5 + self.played_seconds),
            'survey_taken': self.survey_taken,
            'survey_applied': self.survey_applied,
            'survey_useless': self.survey_useless,
            'tbcdid': tbcdid,
            'stats_timestamp': stats_timestamp,
            'deployment_timestamp': deployment_timestamp,
            'recipientid': recipientid,
            'deployment_uuid': deployment_uuid
        }
        if any([x not in result.keys() for x in PLAY_STATISTICS_COLUMNS]):
            raise Exception('Missing playstatistics.csv value')
        if any([x not in PLAY_STATISTICS_COLUMNS for x in result.keys()]):
            raise Exception('Extraneous value in playstatistics.csv')

        return result

    def print_line(self, properties: dict[str,str]) -> str:
        data = self.as_dict(properties)
        return ','.join([escape_csv(data[x]) for x in PLAY_STATISTICS_COLUMNS])

    def emit_summary(self, properties: dict[str, str], playstatistics_path: Path):
        """
        Emits a "playstatistics" format line of statistics.
        """
        # timestamp,project,deployment,contentpackage,community,talkingbookid,
        # 2022-08-29T16:05:25.000Z,"LANDESA-LR","LANDESA-LR-22-4","LANDESA-LR-4-MEN","FF5494E908CD07ED","C-005C00D1",
        # contentid,started,quarter,half threequarters,completed,played_seconds,survey_taken,survey_applied,survey_useless,
        # "C-005C00D1_9-0_DDCD49C6",0,0,0,0,49,1033,0,0,0,
        # tbcdid,stats_timestamp,deployment_timestamp,recipientid,deployment_uuid
        # "0052",2022-08-29T16:05:25.000,2022-05-17T16:04:59.661,"ff5494e908cd07ed","4f708c68-4ee9-4d30-bc63-2517d1afa5fc"

        need_header = not playstatistics_path.exists()
        with playstatistics_path.open('a') as playstatistics_file:
            if need_header:
                print(','.join(PLAY_STATISTICS_COLUMNS), file=playstatistics_file)
            line = self.print_line(properties)
            print(line, file=playstatistics_file)
