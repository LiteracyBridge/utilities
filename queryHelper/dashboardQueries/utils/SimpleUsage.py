import csv
import io
import re
import time
from re import Match
from typing import List, Optional, Tuple

from .db import get_db_connection, query_to_csv

"""
This Module validates a query against a set of known columns, and can generate valid SQL equivalent to the query.

Class SimpleQueryValidator is instantiated with a list of valid columns. It then can parse a simplified form
of SQL against those columns. The query is validated as querying only those columns, with optional aggregations
of sum(column) and count(column), optionally divided by sum(column) or count(column). Note that the input is
simply "count(column)", but the output is "COUNT(DISTINCT column)". Output columns can be named with
"as name", but if omitted aggregated or normalized columns will be given (semi-)meaningful names.   
"""

_aggregations = {'sum': ('SUM(', ''), 'count': ('COUNT(DISTINCT ', 'num_')}

# A query is a list of column-specs, separated by commas.
# A column-spec is a column-name, or an aggregated-column, or a normalized-aggregation
COLUMN_SPEC = re.compile(r'(?ix)^\s*'  # ignore case, whitespace
                         r'(?:(?P<agg>count|sum) \s* \( \s*)?'  # optional aggregation
                         r'(?P<col>\w+) \s*'  # column 
                         # if aggregated, closing paren
                         r'(?(agg) \) \s*'
                         # Also, if aggregated, optional normalization of aggregation
                         r'(/ ((?P<norm>count|sum) \s* \( \s* (?P<norm_col>\w+) \s* \) ))?)'
                         # Optional ' as '
                         r'(\s* as \s+ (?P<as_col>\w+) \s*)?$')

# A list of the columns that the user may request.
CHOOSABLE_COLUMNS = [
    'deploymentnumber',
    'deployment',
    'deploymentname',
    'startdate',

    'contentpackage',
    'languagecode',
    'language',

    'partner',
    'affiliate',

    'country',
    'region',
    'district',
    'communityname',
    'groupname',
    'agent',
    'recipientid',
    'talkingbookid',
    'deployment_uuid',

    'category',
    'playlist',
    'sdg_goals',
    'sdg_targets',
    'contentid',
    'title',
    'format',
    'duration_seconds',
    'position',

    'timestamp',
    'deployment_timestamp',

    'played_seconds',
    'completions',
    'threequarters',
    'half',
    'quarter',
    'started',

    'tbcdid'
]

VIEW_QUERY = '''
CREATE OR REPLACE TEMP VIEW temp_usage AS (
  SELECT * FROM usage_info 
  WHERE recipientid IN (SELECT recipientid FROM recipients WHERE project ILIKE %s) 
    AND played_seconds>0
);
'''
VIEW_QUERY_DEPL = '''
CREATE OR REPLACE TEMP VIEW temp_usage AS (
  SELECT * FROM usage_info 
  WHERE recipientid IN (SELECT recipientid FROM recipients WHERE project ILIKE %s) 
    AND played_seconds>0
    AND deploymentnumber = %s
);
'''

TEMP_VIEW = 'temp_usage'


# noinspection SqlNoDataSourceInspection
class SQV2:
    def __init__(self, allowed_columns: List[str], table: str, augment: str = ''):
        self._allowed_columns = allowed_columns
        self._table = table
        self._augment = augment

    def get_query(self, columns: List[str], expressions: List[str]):
        grouping = f' GROUP BY {",".join(columns)} ORDER BY {",".join(columns)}' if columns else ''
        query_str = f'SELECT DISTINCT {",".join(expressions)} FROM {self._table}{grouping};'
        return query_str

    def parse(self, simple_query: str) -> Tuple[Optional[str], List[str]]:
        aggs = {'count': 'COUNT(DISTINCT {})', 'sum': 'SUM({})'}
        tags = {'count': 'num_', 'sum': ''}

        def _Q(col=None, agg=None, norm=None, norm_col=None, as_col=None):
            if col not in self._allowed_columns:
                errors.append(f'Not column: "{col}"')
                return
            name = col  # may be overridden by 'as' or aggregation
            if not agg:  # simple column
                column_query = col
            else:  # aggregated column
                column_query = aggs[agg].format(col) + (('/' + aggs[norm].format(norm_col)) if norm else '')
            if as_col:  # name specified
                name = as_col
                column_query += f" AS {as_col}"
            elif agg:  # aggregate name inferred
                name = f'{tags[agg]}{col}' + (f'_per_{norm_col}' if norm else '')
                column_query += f' AS {name}'

            if name not in names:
                if not agg and col not in columns:
                    columns.append(col)
                names.append(name)
                expressions.append(column_query)

        # What the result columns are named, column name, decorated by aggregators, or via 'as'
        names = []
        # The non-aggregated columns. Used for 'GROUP BY' and 'ORDER BY'
        columns = []
        # The expressions for each column
        expressions = []
        errors = []
        for p in simple_query.split(',') + self._augment.split(','):
            if not p:
                continue
            m: Match = COLUMN_SPEC.match(p)
            if m:
                _Q(m['col'], m['agg'], m['norm'], m['norm_col'], m['as_col'])
            else:
                errors.append(f'Not a select expression: "{p}"')

        query_str = self.get_query(columns=columns, expressions=expressions) if not errors else None
        return query_str, errors

def get_usage(program: str, columns: str, deployment: str, debug: bool = False):
    start = time.time_ns()

    sqv2 = SQV2(CHOOSABLE_COLUMNS, TEMP_VIEW, augment='sum(completions),sum(played_seconds)')
    query, errors = sqv2.parse(columns)
    if errors:
        return str(errors)

    cursor = get_db_connection().cursor()

    # Create a convenience view limited to the data of interest.
    if deployment:
        print('Program filter: "{}" with: {}, {}'.format(VIEW_QUERY_DEPL, program, deployment))
        cursor.execute(VIEW_QUERY_DEPL, (program, deployment))
    else:
        cursor.execute(VIEW_QUERY, (program,))

    # Run the query
    num_rows = 0
    file_like = io.StringIO()

    print('Main query: "{}"'.format(query))
    cursor.execute(query)
    num_columns = len(cursor.description)
    writer = csv.writer(file_like, quoting=csv.QUOTE_MINIMAL)
    writer.writerow([x.name for x in cursor.description])
    for record in cursor:
        num_rows += 1
        writer.writerow([str(record[ix]) for ix in range(0, num_columns)])
    end = time.time_ns()
    print('{} rows in {} mSec'.format(num_rows, (end - start) / 1000000))
    if debug and num_rows < 10:
        print('values: {}'.format(file_like.getvalue()))

    return {'column_descriptions': {x.name: x.type_code for x in cursor.description},
            'row_count': num_rows, 'msec': (end - start) / 1000000,
            'query': query, 'values': file_like.getvalue()}


def get_usage2(programid: str, columns: str, deployment_number = None, debug: bool = False):
    start = time.time_ns()

    sqv2 = SQV2(CHOOSABLE_COLUMNS, TEMP_VIEW, augment='sum(completions),sum(played_seconds)')
    query, errors = sqv2.parse(columns)
    if errors:
        # http error result code
        return str(errors), 400

    cursor = get_db_connection().cursor()

    # Create a convenience view limited to the data of interest.
    if deployment_number:
        print('Program filter: "{}" with: {}, {}'.format(VIEW_QUERY_DEPL, programid, deployment_number))
        cursor.execute(VIEW_QUERY_DEPL, (programid, deployment_number))
    else:
        cursor.execute(VIEW_QUERY, (programid,))

    # Run the query
    usage, num_rows = query_to_csv(query, cursor=cursor)
    end = time.time_ns()
    print('{} rows in {} mSec'.format(num_rows, (end - start) / 1000000))
    if debug and num_rows < 10:
        print('values: {}'.format(usage))

    return usage


if __name__ == '__main__':

    def rem(query: str):
        sqv2: SQV2 = SQV2(test_columns, table='temp_view',
                          augment='sum(completions),count(country),community as cmty,district')
        query_str, errors = sqv2.parse(query)
        if query_str:
            print(query_str)
        if errors:
            print(errors)
        # compare_validators(query)


    q1 = 'category,community,sum(played_seconds)/count(talkingbookid) as sec_per_tb,count(talkingbookid),sum(completions)/count(talkingbookid)'
    q2 = 'country, district, community, talkingbookid, category, sum(played_seconds), sum(completions)'
    q3 = 'category as cat'
    q4 = 'sum(played_seconds) as tot_seconds, sum(played_seconds)/count(talkingbookid) as seconds_per_tb'
    q5 = 'category community'
    q6 = 'country,count(talkingbookid)*count(agent)'
    q7 = 'planet,country,community'

    test_columns = ['district', 'country', 'community', 'category', 'talkingbookid', 'played_seconds', 'completions']

    rem(q1)
    rem(q2)
    rem(q3)
    rem(q4)
    rem(q5)
    rem(q6)
    rem(q7)
