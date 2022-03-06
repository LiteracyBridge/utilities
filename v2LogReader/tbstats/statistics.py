import sys

from tbstats.playstatistic import PlayStatistic


class Statistics():
    def __init__(self, properties: dict[str, str]):
        self._properties = properties
        self._play_statistics: dict[str, PlayStatistic] = {}

    def played(self, content_id: str, duration_ms: int, played_ms: int):
        play_statistic = self._play_statistics.setdefault(content_id, PlayStatistic(content_id))
        play_statistic.played(duration_ms, played_ms)
        play_statistic.emit_played_event(duration_ms=duration_ms, played_ms=played_ms, properties=self._properties,
                                         outf=sys.stdout)

    def surveyed(self, content_id: str, positive: bool = False, negative: bool = False):
        play_statistic = self._play_statistics.setdefault(content_id, PlayStatistic(content_id))
        play_statistic.surveyed(positive, negative)

    def emit(self, properties: dict[str, str], playstatistics_path):
        for ps in self._play_statistics.values():
            ps.emit_summary(self._properties, playstatistics_path)

    def get_playstatistics(self, collected_properties: dict[str, str]):
        """
        Gets the playstatistics as a list of dicts.
        :param collected_properties: properties about the collection, including properties about the collected
            deployment
        :return: A list of playstatistic rows
        """
        return [ps.as_dict(collected_properties) for ps in self._play_statistics.values()]
