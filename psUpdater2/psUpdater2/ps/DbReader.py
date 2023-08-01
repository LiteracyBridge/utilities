from typing import Union, Tuple, Optional, List

from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection

from ps.db import get_db_engine, get_db_connection
from ps.spec import ProgramSpec


# noinspection SqlNoDataSourceInspection
class _DbReader:
    def __init__(self, programid: str):
        self._program_id = programid

        self._program_spec: Optional[ProgramSpec] = None

    def read_from_database(self, conn: Connection) -> ProgramSpec:
        """
        Executes the SQL select statements to create the program spec.
        """

        # noinspection PyShadowingNames
        def query_table(columns: List[str], table: str, programid_column: str = 'project', group_by: str = '',
                        order_by: str = ''):
            # Programid_column is provided by this application, not by the user. Safe to embed in a SQL string.
            return conn.execute(
                text(
                    f"SELECT {','.join(columns)} FROM {table} WHERE {programid_column} " +
                    f"= :program_id {group_by} {order_by};"),
                {'program_id': self._program_id})

        program_spec: ProgramSpec = ProgramSpec(self._program_id)

        result = query_table(['*'], 'content')
        for row in result:
            program_spec.add_content(dict(row))

        result = query_table(['*'], 'recipients')
        for row in result:
            program_spec.add_recipient(dict(row))

        # 'program_id' is unique in the programs table, so there will be at most one row. But maybe none, if the
        # program hasn't been fully initialized yet.
        result = query_table(['*'], 'programs', programid_column='program_id')
        program_rows = [dict(row) for row in result.all()]
        if len(program_rows) == 1:
            program_spec.add_general(program_rows[0])
        # similarly for projects table.
        result = query_table(['*'], 'projects', programid_column='projectcode')
        program_rows = [dict(row) for row in result.all()]
        if len(program_rows) == 1:
            name = program_rows[0].get('project')
            program_spec.general.name = name

        self._program_spec = program_spec
        return program_spec

    def do_import_from_db(self, engine: Engine = None, connection: Connection = None) -> Tuple[Union[bool, Optional[ProgramSpec]], List[str]]:
        if connection is not None:
            self.read_from_database(connection)
        else:
            if engine is None:
                engine = get_db_engine()
            with get_db_connection(engine = engine) as conn:
                self.read_from_database(conn)
        return self._program_spec, []


def read_from_db(programid: str, engine: Engine = None, connection: Connection = None) -> \
        Tuple[Union[bool, Optional[ProgramSpec]], List[str]]:
    return _DbReader(programid).do_import_from_db(engine, connection)
