import dataclasses
from collections import namedtuple
from typing import List, Optional, Dict, Any, Tuple

import boto3
import ps.db as db
from ps import ProgramSpec, Deployment, get_db_engine, Playlist, Message, Recipient, General
from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection

# dyanmodb programs table keeps a cached copy of program name, so update it along with SQL name.
dynamodb = None
programs_table = None
REGION_NAME = 'us-west-2'
PROGRAMS_TABLE_NAME = 'programs'

# Helpers for tracking differences between program spec and database.
Diff = namedtuple("Diff", "db ps")


def _rows_differ(sql_row, ps_row, *, required_columns: List[str], ignored_columns: List[str], diffs) -> bool:
    if sql_row != ps_row:
        # There is a difference. Do we care about it?
        diff_members = [k for k, v in sql_row.items() if k not in ignored_columns and k in ps_row and v != ps_row[k]]
        # Are they really different?
        diff_members = [k for k in diff_members if str(sql_row[k]) != str(ps_row[k])]
        if len(diff_members) > 0:
            delta = {k: Diff(sql_row[k], ps_row[k]) for k in diff_members}
            for k, v in delta.items():
                if k not in required_columns and not v.db and not v.ps:
                    pass
                else:
                    diffs.append(delta)
                    return True
    return False


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class _DbWriter:
    def __init__(self, program_spec: ProgramSpec):
        self._program_spec: ProgramSpec = program_spec
        # created at the start of import
        self._connection: Optional[Connection] = None
        # database ids of deployment numbers
        self._depl_ids = {}

    def _category_code_converter(self, categoryname: str) -> Optional[str]:
        """
        Given a category name (like "Nutrition"), return the category code (like "2-9").

        This is somewhat broken. The ACM has much better lookup code; we should simply
        store this as a string, and let the ACM look it up. However, that would mean that the
        Amplio Suite couldn't show as nice a name.

        :param categoryname: to look up
        :return: A random matching category code.
        """
        try:
            command = text('SELECT categorycode FROM supportedcategories WHERE categoryname=:categoryname;')
            values = {'categoryname': categoryname}
            result = self._connection.execute(command, values)
            for row in result:
                return row[0]
        except Exception as ex:
            pass
        return None

    def _save_name(self, programid: str, name: str):
        """
        If the name differs from the value in dynamodb, update the name in dynamodb and in postgres.
        :param programid: the program for which to save the name.
        :param name: the (possibly) new name.
        :return: None
        """

        def update_dynamo_name() -> None:
            update_expr = 'SET program_name = :n'
            expr_values = {':n': name}
            try:
                programs_table.update_item(
                    Key={'program': programid},
                    UpdateExpression=update_expr,
                    ExpressionAttributeValues=expr_values
                )
            except Exception as err:
                print(f'exception creating or updating name for {programid}: {err}')
                return

        def get_dynamo_name() -> str:
            program_row = programs_table.get_item(Key={'program': programid}).get('Item', {})
            return program_row.get('program_name')

        global dynamodb, programs_table
        if dynamodb is None:
            dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
            programs_table = dynamodb.Table(PROGRAMS_TABLE_NAME)

        dynamo_name = get_dynamo_name()
        if dynamo_name != name:
            update_dynamo_name()
            values = {'name': name, 'program_id': programid}
            command = text(f'UPDATE projects SET project = :name WHERE projectcode = :program_id;')
            result = self._connection.execute(command, values)

    def _save_general(self):
        field_names = [x for x in General.sql_columns() if x != 'name']
        conflict_setters = ','.join([f'{x}=EXCLUDED.{x}' for x in field_names if x != 'program_id'])
        command = text(f'INSERT INTO programs ({",".join(field_names)}) '
                       f'VALUES (:{",:".join(field_names)}) '
                       'ON CONFLICT ON CONSTRAINT programs_uniqueness_key DO UPDATE SET ' +
                       conflict_setters + ';')
        values = self._program_spec.general.sql_row
        result = self._connection.execute(command, values)
        if values.get('name'):
            self._save_name(values.get('program_id'), values.get('name'))
        print(f'{result.rowcount} new programs record for {self._program_spec.program_id}.')

    def _merge_recipients(self):
        def differ(sql_row, ps_row: Recipient) -> bool:
            return _rows_differ(dict(sql_row), ps_row.todict,
                                required_columns=Recipient.required_columns, ignored_columns=deprecated_columns,
                                diffs=diffs)

        # [ { column: (sql: 'sql_v', csv: 'csv_v') } ]
        diffs: List[Dict[str, Any]] = []
        deprecated_columns = ['affiliate', 'partner', 'component']

        # Recipients from the spreadsheet w/o recipientid are to be added. Those w/ should be checked for update.
        ps_recips: Dict[str, Recipient] = {}
        updates = []
        additions = []
        for r in self._program_spec.recipients:
            if r.new_recipientid:
                # Newly computed recipientid, so it is a new recipient.
                additions.append(
                    {**dataclasses.asdict(r), 'project': self._program_spec.program_id, 'recipientid': r.recipientid})
            else:
                # Has a recipient id, save to check against the recipients in the database.
                ps_recips[r.recipientid] = r

        # Examine the recipients in the database and compare against recipients in the program spec
        command = text('SELECT * FROM recipients WHERE project=:program_id;')
        values = {'program_id': self._program_spec.program_id}
        result = self._connection.execute(command, values)
        print(f'{result.rowcount} existing recipients for {self._program_spec.program_id}.')
        for sql_row in result:
            row_id = sql_row['recipientid']
            # Only look at the db_rows for which we have csv rows. Ignore recipients no longer active in the program
            # spec. (They'e kept because statistics refer to them.)
            if row_id in ps_recips:
                if differ(sql_row, ps_recips[row_id]):
                    # updates.append({'project': self._program_spec.program_id, **dataclasses.asdict(ps_recips[row_id])})
                    updates.append({'project': self._program_spec.program_id, **ps_recips[row_id].sql_row})
        # To give visibility into what's being changed.
        for d in diffs:
            print(d)

        # Add the new recipients
        field_names = Recipient.sql_columns()
        if len(additions) > 0:
            command_a = text(f'INSERT INTO recipients (project,{",".join(field_names)}) '
                             f'VALUES (:project,:{",:".join(field_names)});')
            result_a = self._connection.execute(command_a, additions)
            print(f'{result_a.rowcount} recipients records added for {self._program_spec.program_id}.')
        # Update the changed ones
        if len(updates) > 0:
            command_u = text(f'UPDATE recipients SET ({",".join(field_names)}) '
                             f'= (:{",:".join(field_names)}) WHERE recipientid=:recipientid;')
            result_u = self._connection.execute(command_u, updates)
            print(f'{result_u.rowcount} recipients records updated for {self._program_spec.program_id}.')

    def _save_deployments_and_get_ids(self):
        def differ(sql_row, ps_row: Deployment) -> bool:
            """
            Does the new data (csv_row) differ from the database (sql_row) in any way that we care about?
            :param sql_row: Existing data.
            :param csv_row: New data.
            :return: True if we should update the database from the new data.
            """
            db_dict = Deployment.parse(sql_row)
            ps_dict = ps_row.todict
            return _rows_differ(db_dict, ps_dict,
                                required_columns=Deployment.required_columns, ignored_columns=['deployment'],
                                diffs=diffs)

        # [ { column: (db: 'db', ps: 'ps_v') } ]
        diffs: List[Diff] = []

        command = text('SELECT * FROM deployments WHERE project=:program_id;')
        values = {'program_id': self._program_spec.program_id}
        result = self._connection.execute(command, values)
        db_deployments: Dict[int, Dict[str, Any]] = {int(r['deploymentnumber']): r for r in
                                                     [dict(row) for row in result]}

        ps_deployments: Dict[int, Deployment] = {d.deploymentnumber: d for d in self._program_spec.deployments}
        updates = []
        additions = []
        depl: Deployment
        for num, depl in ps_deployments.items():
            # Is the deployment new to the database?
            if num not in db_deployments:
                deployment = depl.deployment or f'{self._program_spec.program_id}-{depl.startdate.year % 100}-{num}'
                additions.append({'program_id': self._program_spec.program_id, 'deployment': deployment,
                                  'deploymentname': depl.deploymentname,
                                  'deploymentnumber': num, 'startdate': depl.startdate, 'enddate': depl.enddate})
            else:
                depl_id: Optional[int] = None
                if self._use_deployment_ids:
                    depl_id = db_deployments[num]['id']
                    self._depl_ids[num] = depl_id
                if differ(db_deployments[num], depl):
                    updates.append({'program_id': self._program_spec.program_id, 'deploymentname': depl.deploymentname,
                                    'deploymentnumber': num, 'startdate': depl.startdate, 'enddate': depl.enddate,
                                    'id': depl_id})

        if len(additions) > 0:
            command_a = text(
                'INSERT INTO deployments (project, deployment, deploymentname, deploymentnumber, startdate, enddate) '
                'VALUES (:program_id, :deployment, :deploymentname, :deploymentnumber, :startdate, :enddate);')
            result_a = self._connection.execute(command_a, additions)
            print(f'{result_a.rowcount} deployment records added for {self._program_spec.program_id}.')
            # Query the deployments after the insert, and get the ids of the newly added rows.
            result = self._connection.execute(command, values)
            for row in result:
                num = int(row.deploymentnumber)
                if self._use_deployment_ids and num not in self._depl_ids:
                    self._depl_ids[num] = int(row.id)

        if len(updates) > 0:
            if self._use_deployment_ids:
                command_u = text('UPDATE deployments SET project=:program_id, deploymentname=:deploymentname, '
                                 'deploymentnumber=:deploymentnumber, startdate=:startdate, enddate=:enddate '
                                 'WHERE id=:id;')
            else:
                command_u = text(
                    'UPDATE deployments SET startdate=:startdate, enddate=:enddate, deploymentname=:deploymentname'
                    'WHERE project=:program_id AND deploymentnumber=:deploymentnumber;')
            result_u = self._connection.execute(command_u, updates)
            print(f'{result_u.rowcount} deployment records updated for {self._program_spec.program_id}.')

    def _save_playlist(self, playlist: Playlist, deployment_id: int, deploymentnumber: int) -> int:
        """
        Save information about one playlist.
        :param deployment_id: The deployment that this playlist appears in.
        :param db_playlist: Properties of the playlist: program_code, position, title
        :return: The generated playlist_id of the playlist record in the database.
        """
        columns = ['program_id', 'position', 'title']
        if self._use_deployment_ids:
            columns.append('deployment_id')
        else:
            columns.append('deploymentnumber')
        if not self._audience_in_messages:
            columns.append('audience')
        command = text(f'INSERT INTO playlists ({",".join(columns)}) VALUES (:{",:".join(columns)}  );')
        values = {'program_id': self._program_spec.program_id, 'deployment_id': deployment_id,
                  'position': playlist.position,
                  'title': playlist.title, 'audience': playlist.audience, 'deploymentnumber': deploymentnumber}

        self._connection.execute(command, values)

        # Retrieve the newly allocated id.
        playlist_id = -1
        if self._use_deployment_ids:
            command = text('SELECT id FROM playlists WHERE program_id=:program_id AND '
                           'cast(deployment_id as int)=:deployment_id AND title=:title;')
        else:
            command = text('SELECT id FROM playlists WHERE program_id=:program_id AND '
                           'cast(deploymentnumber as int)=:deploymentnumber AND position=:position AND title=:title;')
        result = self._connection.execute(command, values)
        for row in result:
            playlist_id = row[0]

        return playlist_id

    def _save_message(self, message: Message, playlist_id: int) -> None:
        """
        Save the information about one message.
        :param playlist_id: The id of the playlist containing this message.
        :param db_message: Properties of the message.
        :return: The generated id of the message in the database.
        """
        columns = [
            'program_id', 'playlist_id', 'position', 'title', 'format', 'default_category_code',
            'variant', 'sdg_goal_id', 'sdg_target', 'sdg_target_id', 'key_points'
        ]
        if self._audience_in_messages:
            columns.append('audience')
        if not self._use_message_languages:
            columns.append('languages')
        command = text(f'INSERT INTO messages ({",".join(columns)}) VALUES (:{",:".join(columns)}  );')
        # just the target part, without the goal part.
        sdg_target = message.sdg_target[
                     message.sdg_target.find(
                         '.') + 1:] if message.sdg_target and '.' in message.sdg_target else message.sdg_target

        # This would be better as a string column of the category name, because user may need to disambiguate.
        default_category_code = self._category_code_converter(message.default_category_code)

        values = {'program_id': self._program_spec.program_id, 'playlist_id': playlist_id, 'position': message.position,
                  'title': message.title, 'format': message.format,
                  'default_category_code': default_category_code, 'variant': message.variant,
                  'sdg_goal_id': message.sdg_goal, 'sdg_target': sdg_target,
                  'sdg_target_id': message.sdg_target, 'key_points': message.key_points,
                  'audience': message.audience, 'languages': message.languages}

        self._connection.execute(command, values)

        if message.languages and self._use_message_languages:
            # Retrieve the newly allocated id.
            message_id = -1
            command = text('SELECT id FROM messages WHERE program_id=:program_id AND playlist_id=:playlist_id '
                           'AND position=:position;')
            result = self._connection.execute(command, values)
            for row in result:
                message_id = row[0]

            language_list = [x.strip() for x in message.languages.split(',')]
            for language in language_list:
                command = text(
                    'INSERT INTO message_languages(message_id, language_code) VALUES(:message_id, :language_code);')
                values = {'message_id': message_id, 'language_code': language}
                self._connection.execute(command, values)

    def _save_content(self):
        for deployment in self._program_spec.deployments:
            deployment_id = self._depl_ids[deployment.deploymentnumber]
            for playlist in deployment.playlists:
                playlist_id = self._save_playlist(playlist=playlist, deployment_id=deployment_id,
                                                  deploymentnumber=deployment.deploymentnumber)
                for message in playlist.messages:
                    self._save_message(message=message, playlist_id=playlist_id)

    # noinspection SqlNoDataSourceInspection
    def _update_database(self):
        def get_counts() -> Dict[str, int]:
            # Tables for which to get counts, and program_id column in that table.
            tables = {'deployments': 'project', 'messages': 'program_id', 'playlists': 'program_id'}
            command = ''
            for t, p in tables.items():
                if command: command += ' UNION '
                command += f"SELECT '{t}', COUNT({p}) FROM {t} WHERE {p}=:program_id"
            values = {'program_id': self._program_spec.program_id}
            result = self._connection.execute(text(command), values)
            return {x[0]: x[1] for x in result}

        self._use_deployment_ids = not db.table_has_column('playlists', 'deploymentnumber')
        self._use_message_languages = not db.table_has_column('messages', 'languages')
        self._audience_in_messages = db.table_has_column('messages', 'audience')

        counts_before = get_counts()
        print(
            f"{', '.join([f'{v} {k}' for k, v in counts_before.items()])} before save for {self._program_spec.program_id}")

        command = text('DELETE FROM playlists WHERE program_id=:program_id;')
        values = {'program_id': self._program_spec.program_id}
        self._connection.execute(command, values)

        self._save_deployments_and_get_ids()
        self._save_content()

        counts_after = get_counts()
        if any([counts_before[t] > counts_after[t] for t in counts_before.keys()]):
            print('*************\nCounts decreased!!')
        print(
            f"{', '.join([f'{v} {k}' for k, v in counts_after.items()])} after save for {self._program_spec.program_id}")

        # self._infer_program_info()
        self._merge_recipients()
        self._save_general()

    def _write_to_db(self, engine: Engine = None, connection: Connection = None, **kwargs) -> Tuple[bool, List[str]]:
        """
        Saves this program specification to the database.
        :param engine: The optional SQL Alchemy engine through which to access the database. If not provided,
            the pre-existing engine from the db module will be used.
        :param connection: The optional SQL connection through wiich to access the database. If provided, the caller
            will manage the transaction, either explicitly or implicitly.
            If not provided, one is obtained from the engine. In this case if "disposition" is provided as an
            argument, the transaction is committed if disposition has a truthy value.
        """
        commit = kwargs.get('disposition', 'commit')
        if connection is not None:
            self._connection = connection
            self._update_database()
        else:
            if engine is None:
                engine = get_db_engine()
            with db.get_db_connection(engine=engine) as conn:
                self._connection = conn
                transaction = self._connection.begin()
                self._update_database()
                if commit:
                    transaction.commit()
                    print(f'Changes commited for {self._program_spec.program_id}')
                else:
                    transaction.rollback()
                    print(f'Changes rolled back for {self._program_spec.program_id}')
        return False, []


def export_to_db(program_spec: ProgramSpec, engine: Engine = None, connection: Connection = None, **kwargs) -> \
        Tuple[bool, List[str]]:
    return _DbWriter(program_spec)._write_to_db(engine, connection, **kwargs)
