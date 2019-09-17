import io
import token
from tokenize import tokenize

"""
This Module validates a query against a set of known columns, and can generate valid SQL equivalent to the query.

Class SimpleQueryValidator is instantiated with a list of valid columns. It then can parse a simplified form
of SQL against those columns. The query is validated as querying only those columns, with optional aggregations
of sum(column) and count(column), optionally divided by sum(column) or count(column). Note that the input is
simply "count(column)", but the output is "COUNT(DISTINCT column)". Output columns can be named with
"as name", but if omitted aggregated or normalized columns will be given (semi-)meaningful names.   
"""

_aggregations = {'sum': ('SUM(', ''), 'count': ('COUNT(DISTINCT ', 'num_')}


class QueryColumn:
    """
    Class to hold one column description. Either a table column, or an aggregation of a column (sum or count),
     possibly normalized by another aggregation.
    """

    def __init__(self, column=None, agg=None, by_agg=None, by_column=None, as_name=None):
        self._column = column
        self._agg = agg
        self._by_agg = by_agg
        self._by_column = by_column
        self._as = as_name

    def __eq__(self, o: object) -> bool:
        if isinstance(o, self.__class__):
            return self._column == o._column and self._agg == o._agg and \
                   self._by_agg == o._by_agg and self._by_column == o._by_column and self._as == o._as
        return False

    def __hash__(self) -> int:
        return hash((self._column, self._agg, self._by_column, self._by_agg, self._as))

    # Generate the sql for the column
    @property
    def query(self):
        if self.is_pod:
            # plain-old-data is just the column name
            result = self._column
        else:
            aggregation = _aggregations.get(self._agg)
            result = aggregation[0] + self._column + ')'
            if self._by_agg:
                # normalized by another aggregation
                aggregation = _aggregations.get(self._by_agg)
                result += '/' + aggregation[0] + self._by_column + ')'

        # Add 'AS ...' if user provided an override, or for aggregations (don't want columns named 'count')
        if self.as_name:
            result += ' AS ' + self.as_name

        return result

    @property
    def as_name(self):
        """
        :return: - the user's override if provided (and it differs from the column name), or
                 - a constructed name for aggregations
                 Note that plain-old-data that was not given an "as name" decoration return
                 None here.
        """
        if self.is_pod:
            return self._as if self._as != self._column else None
        else:
            return self.name

    @property
    def name(self):
        """
        :return: the name that the column will be called (to the best of our ability)
        """
        if self._as:
            name = self._as
        elif self.is_pod:
            # plain-old-data is just the column name
            name = self._column
        else:
            aggregation = _aggregations.get(self._agg)
            name = aggregation[1] + self._column
            if self._by_agg:
                # normalized by another aggregation
                aggregation = _aggregations.get(self._by_agg)
                name += '_per_' + self._by_column
        return name

    @property
    def group_by(self):
        """
        :return: the column name if it should participate in a GROUP BY clause, else None.
        """
        if self.is_pod:
            return self._column
        return None

    @property
    def column(self):
        """
        :return: the column name.
        """
        return self._column

    @property
    def is_pod(self):
        """
        :return: whether the column is POD (Plain Old Data)
        """
        return not self._agg

    def set_as_name(self, name):
        self._as = name


class _UnexpectedToken(Exception):
    """ A typed Exception so that we can catch only the ones we raise ourselves.
    """

    def __init__(self, msg):
        self._msg = msg

    @property
    def message(self):
        return self._msg


class SimpleQueryValidator:
    def __init__(self, columns):
        self._columns = columns
        self._tokens = None
        self._next_token_ix = None
        self._cur_token = None

    def _next_token(self):
        """ Fetches the next token. Sets instance variables for the value and column.
        :return: the next token from parsed stream of tokens, if any, or None.
        """
        if self._next_token_ix >= len(self._tokens):
            self._cur_token = None
            return None
        (num, val, col) = self._tokens[self._next_token_ix]
        self._next_token_ix += 1
        self._cur_token_num = num
        self._cur_token = val
        self._cur_token_col = col
        return self._cur_token

    def _tokenize(self, query_string):
        """ Parse the given string into tokens. Set instance variables so that _next_token() can return the
            next token.
        :param query_string: to be parsed
        :return: None
        """
        self._tokens = []
        self._next_token_ix = 0
        self._token = None
        for toknum, tokval, tokstart, _, _ in tokenize(io.BytesIO(query_string.encode('utf-8')).readline):
            if toknum not in [token.ENDMARKER, token.NEWLINE, token.ENCODING]:
                self._tokens.append((toknum, tokval, tokstart[1]))

    def parse(self, query_string):
        """ Given a string that's the SELECT part of a SQL statement, parse and validate all the columns.
        :param query_string: a string like 'foo,sum(bar) as x,count(baz),sum(a)/count(b) as y'
        :return: An error string, or a tuple of  (query-string, [group-by-column,...])
        """
        self._tokenize(query_string.lower())
        try:
            columns = []
            while self._next_token():
                columns.append(self._query_item())
            result = columns
        except _UnexpectedToken as err:
            result = err.message
        finally:
            self._tokens = None
        return result

    def _query_item(self) -> QueryColumn:
        """
        :return: a QueryColumn that describes a simple table column, or an aggregation function. The function may
        be normalized (divided) by another aggregation function.
        """
        column = None
        agg1 = (None, None)
        agg2 = (None, None)
        ok = False
        if self._cur_token in self._columns:
            # Ordinary table column
            column = self._cur_token
            expect = "',' or 'as'"
            ok = self._next_token() == ',' or self._cur_token == 'as' or self._cur_token is None
        elif self._cur_token in _aggregations:
            # At least one aggregation, of the form "agg-function ( column )"
            agg1 = self._aggregation()
            expect = "'/' or ',' or 'as'"
            if self._cur_token == '/':
                # Normalized by another aggregation
                self._next_token()
                agg2 = self._aggregation()
            ok = self._cur_token == ',' or self._cur_token == 'as' or self._cur_token is None
        else:
            expect = 'column or aggregation'

        if ok:
            as_name = None
            if self._cur_token == 'as':
                self._next_token()
                expect = 'a name'
                if self._cur_token_num == token.NAME:
                    as_name = self._cur_token
                    self._next_token()
                else:
                    self._expected(expect)
            return QueryColumn(column=column or agg1[0], agg=agg1[1], by_column=agg2[0], by_agg=agg2[1],
                               as_name=as_name)
        self._expected(expect)

    def _aggregation(self):
        """ Parse an aggregation function.
        :return: a tuple (column, aggregation-function)
        """
        aggregation = self._cur_token
        expect = '('
        if self._next_token() == '(':
            expect = 'column'
            if self._next_token() in self._columns:
                column = self._cur_token
                expect = ')'
                if self._next_token() == ')':
                    self._next_token()
                    return column, aggregation
        self._expected(expect)

    def _expected(self, expected):
        """ Helper to generate a message and exception when an unexpected token is encountered.
        :param expected: a description of what was expected
        :return: never returns; raises _UnexpectedToken
        """
        if "'" not in expected:
            expected = "'" + expected + "'"
        msg = 'Error in column {}. Found \'{}\' but expected {}.'.format(self._cur_token_col, self._cur_token, expected)
        raise _UnexpectedToken(msg)

    @staticmethod
    def make_query(columns, table):
        select = ','.join([x.query for x in columns])
        groups = [x.column for x in columns if x.is_pod]
        query = 'SELECT DISTINCT ' + select + ' FROM ' + table
        if len(groups):
            column_list = ','.join(groups)
            query += ' GROUP BY ' + column_list + ' ORDER BY ' + column_list
        query += ';'
        return query


if __name__ == '__main__':
    def pr(result):
        if isinstance(result, str):
            print('Error: {}'.format(result))
        else:
            query = sqv.make_query(result, 'temp_usage')
            print(query)
            columns = [x.name for x in result]
            print(columns)


    sqv: SimpleQueryValidator = SimpleQueryValidator(
        ['district', 'country', 'community', 'category', 'talkingbookid', 'played_seconds', 'completions'])
    res = sqv.parse(
        'category,community,sum(played_seconds)/count(talkingbookid) as sec_per_tb,count(talkingbookid),sum(completions)/count(talkingbookid)')
    pr(res)

    cpl_per_tb = QueryColumn(column='completions', agg='sum', by_column='talkingbookid', by_agg='count')
    has_cpt = cpl_per_tb in res

    pr(sqv.parse('country, district, community, talkingbookid, category, sum(played_seconds), sum(completions)'))
    pr(sqv.parse('category as cat'))
    pr(sqv.parse('sum(played_seconds) as tot_seconds, sum(played_seconds)/count(talkingbookid) as seconds_per_tb'))

    # Should generate an error on missing ','
    res = sqv.parse('category community')
    pr(res)

    # Should generate an error on '*'
    pr(sqv.parse('country,count(talkingbookid)*count(agent)'))
