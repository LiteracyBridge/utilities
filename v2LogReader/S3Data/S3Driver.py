import argparse
import functools
from datetime import datetime

from S3Data.S3Importer import S3Importer, Summary
from S3Data.S3Utils import list_objects


class S3Driver:
    def __init__(self, prefix: str, args: argparse.Namespace):
        from S3Data.S3Utils import DEFAULT_SOURCE_PREFIX
        if prefix[0] == '/':
            self._source_prefix = prefix[1:]
        else:
            self._source_prefix = f'{DEFAULT_SOURCE_PREFIX}/{prefix}'
        self._args = args
        self._source_bucket = args.source_bucket
        self._s3_objects = []
        self._now = datetime.now()
        # Pull boolean args out of kwargs.
        self._kwargs = {k: v for k, v in args.__dict__.items() if isinstance(v, bool)} if args else {}
        self._kwargs['verbose'] = args.verbose
        self._details = {}

    def find_objects(self) -> bool:
        self._s3_objects = [x for x in list_objects(bucket=self._source_bucket, prefix=self._source_prefix)]
        print(f'Objects found in s3://{self._source_bucket}/{self._source_prefix}:')
        print(*[x['Key'] for x in self._s3_objects])
        return len(self._s3_objects) > 0

    def process_objects(self):
        def line_breaker(prefix: str, items: list[str], sep=', ', max_len = 150) -> str:
            result = []
            partial = prefix
            prefix = ' ' * len(prefix)
            while len(items) > 0:
                partial += items.pop(0)
                while len(items) > 0 and len(partial) + len(sep) + len(items[0]) + (len(sep) if len(items) > 1 else 0) < max_len:
                    partial += sep + items.pop(0)
                if len(items)>0:
                    partial += sep
                result.append(partial)
                partial = prefix
            return '\n'.join(result)
        def size_str(size) -> str:
            if size < 1000:
                return f'{size}'[:5]+' B'
            elif size < 1000000:
                return f'{size/1000.0:f}'[:5]+' kB'
            elif size < 1000000000:
                return f'{size/1000000.0:f}'[:5]+' MB'
            else:
                return f'{size/1000000000.0:f}'[:5]+' GB'
        def zip_summary(summary: Summary) -> str:
            result=f'{summary.key}({size_str(summary.zip_len)}'
            if summary.have_statistics:
                if summary.collections or summary.deployments:
                    result += f',{"c"if summary.collections else ""}{"d" if summary.deployments else ""}'
                result += f',{summary.play_statistics:2},{summary.uf_messages:2}'
            result += ')'
            return result
        def rev_cmp(a,b):
            if a.have_statistics and not b.have_statistics:
                return -1
            if not a.have_statistics and b.have_statistics:
                return 1
            return b.zip_len - a.zip_len
        rev_k = functools.cmp_to_key(rev_cmp)

        for s3_obj in self._s3_objects:
            if s3_obj['Key'][-1] != '/': # ignore the s3 equivalent of directories.
                s3_importer = S3Importer(self._source_bucket, s3_obj, self._now, **self._kwargs)
                if s3_importer.is_valid:
                    s3_importer.do_import()
                    summary = s3_importer.summary
                    self._details.setdefault(summary.programid, {}).setdefault(summary.names, []).append(summary)

        # Summarize {programid: program_summary}
        summaries = {}
        for programid,names_list in self._details.items():
            program_summary = summaries.setdefault(programid, {'names':set(), 'keys':{}, 'data_len':0, 'tbs':set(), 'collections':0, 'deployments':0, 'play_statistics':0, 'uf_messages':0, 's3_errors':0, 'disposition':{}})
            for names,summary_list in names_list.items():
                program_summary['names'].add(names)
                for s in summary_list:
                    program_summary['keys'][s.key] = s
                    if s.have_statistics:
                        program_summary['tbs'].add(s.talkingbookid)
                        program_summary['collections'] += s.collections
                        program_summary['deployments'] += s.deployments
                        program_summary['play_statistics'] += s.play_statistics
                        program_summary['uf_messages'] += s.uf_messages
                        program_summary['surveys'] += s.surveys
                        program_summary['s3_errors'] += s.s3_errors
                        program_summary['disposition'][s.disposition] = program_summary['disposition'].get(s.disposition, 0) + 1

        for programid, program_summary in summaries.items():
            print(f'Summary for {programid}; processed {len(program_summary.get("keys"))} zip file(s), from {len(program_summary.get("names"))} TB-Loader(s).')
            print(f'    {program_summary.get("collections", "no")} collection(s), {program_summary.get("deployments", "no")} deployment(s), '
                  f'{program_summary.get("play_statistics", "no")} play statistic(s), {program_summary.get("uf_messages", "no")} uf message(s), '
                  f'{program_summary.get("surveys", "no")} survey(s), '
                  f'{program_summary.get("s3_errors", "no")} s3 error(s), ')
            dispositions = program_summary.get("disposition", {'unknown', ''})
            print(f'      disposition(s): {", ".join([f"{k}:{v}" for k,v in dispositions.items()])}')
            print(line_breaker('      tb(s): ', sorted(list(program_summary["tbs"]))))
            print(line_breaker('      by: ', sorted(list(program_summary["names"]))))
            # Print zips largest to smallest
            print(line_breaker('      zip(s): ', [zip_summary(x) for x in sorted(program_summary["keys"].values(), key=rev_k)]))
