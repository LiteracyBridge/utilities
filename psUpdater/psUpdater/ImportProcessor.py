import dataclasses
import json
from collections import namedtuple
from dataclasses import asdict
from datetime import datetime
from typing import Dict, List, Optional, Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

import db

import Spec
from Spec import Recipient

# Helpers for tracking differences between .csv to database.
Diff = namedtuple("Diff", "sql csv")


def _rows_differ(db_row, csv_row, *, required_columns: List[str], ignored_columns: List[str], diffs) -> bool:
    db_row = db_row if isinstance(db_row, dict) else dataclasses.asdict(db_row)
    csv_row = csv_row if isinstance(csv_row, dict) else dataclasses.asdict(csv_row)
    if db_row != csv_row:
        # There is a difference. Do we care about it?
        diff_members = [k for k, v in db_row.items() if k not in ignored_columns and k in csv_row and v != csv_row[k]]
        # Are they really different?
        diff_members = [k for k in diff_members if str(db_row[k]) != str(csv_row[k])]
        if len(diff_members) > 0:
            delta = {k: Diff(db_row[k], csv_row[k]) for k in diff_members}
            for k, v in delta.items():
                if k not in required_columns and not v.sql and not v.csv:
                    pass
                else:
                    diffs.append(delta)
                    return True
    return False


# noinspection SqlDialectInspection,SqlNoDataSourceInspection,SqlResolve
class ImportProcessor:
    def __init__(self, program_id: str, program: Spec.Program):
        self._program: Spec.Program = program
        self.program_id: str = program_id

        self._db_program: Optional[Spec.General] = None
        self._db_deployments: Dict[int, Spec.DbDeployment] = {}
        self._ids = {}
        self._deployments_with_content = []

        self._connection: Optional[Connection] = None

    def _get_category_code(self, categoryname: str) -> Optional[str]:
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

    def _infer_program_info(self):
        deployments_count = len(self._db_deployments.keys())
        languages = set()
        regions = set()
        listening_models = set()
        recip: Recipient
        for recip in self._program.recipients:
            languages.add(recip.language)
            regions.add(recip.region)
            listening_models.add(recip.listening_model)
        sdgs = set()
        for content in self._program.content:
            if content.sdg_goals:
                sdgs.add(int(content.sdg_goals))
        deployments_first = datetime(2100, 1, 1).date()  # far future
        for depl in self._program.deployments:
            depl_start = Spec.asdate(depl.startdate)
            if (depl_start - deployments_first).days < 0:
                deployments_first = depl_start
        country = self._program.recipients[0].country if len(self._program.recipients) > 0 else 'USA'
        self._db_program = Spec.General(program_id=self.program_id, deployments_count=deployments_count,
                                        deployments_first=deployments_first,
                                        country=country,
                                        sustainable_development_goals=json.dumps(list(sdgs)),
                                        languages=json.dumps(list(languages)), region=json.dumps(list(regions)),
                                        listening_models=json.dumps(list(listening_models)))
        field_names = [x.name for x in dataclasses.fields(self._db_program)]
        command = text(f'INSERT INTO programs ({",".join(field_names)}) '
                       f'VALUES (:{",:".join(field_names)}) '
                       'ON CONFLICT DO NOTHING ;')
        values = asdict(self._db_program)
        result = self._connection.execute(command, values)
        print(f'{result.rowcount} new programs record for {self.program_id}.')

    def _merge_recipients(self):
        def differ(db_row, csv_row: Recipient) -> bool:
            return _rows_differ(dict(db_row), csv_row,
                                required_columns=Spec.recipient_required_fields, ignored_columns=deprecated_columns,
                                diffs=diffs)

        # [ { column: (sql: 'sql_v', csv: 'csv_v') } ]
        diffs: List[Dict[str, Any]] = []
        deprecated_columns = ['affiliate', 'partner', 'component']

        # Recipients from the spreadsheet w/o recipientid are to be added. Those w/ should be checked for update.
        csv_recips: Dict[str, Recipient] = {}
        updates = []
        additions = []
        for r in self._program.recipients:
            if not r.recipientid:
                # No recipientid so it is a new recipient.
                recipientid = self._program.compute_recipientid(r)
                additions.append({**dataclasses.asdict(r), 'project': self.program_id, 'recipientid': recipientid})
            else:
                # Has a recipient id, save to check against the recipients in the database.
                csv_recips[r.recipientid] = r

        # Examine the recipients in the database and compare against recipients in the .csv
        command = text('SELECT * FROM recipients WHERE project=:program_id;')
        values = {'program_id': self.program_id}
        result = self._connection.execute(command, values)
        print(f'{result.rowcount} existing recipients for {self.program_id}.')
        for db_row in result:
            row_id = db_row['recipientid']
            # Only look at the db_rows for which we have csv rows. Ignore recipients no longer active in the program
            # spec. (They'e kept because statistics refer to them.)
            if row_id in csv_recips:
                if differ(db_row, csv_recips[row_id]):
                    updates.append({'project': self.program_id, **dataclasses.asdict(csv_recips[row_id])})
        # To give visibility into what's being changed.
        for d in diffs:
            print(d)

        # Add the new recipients
        field_names = [x.name for x in dataclasses.fields(Recipient)]
        if len(additions) > 0:
            command_a = text(f'INSERT INTO recipients (project,{",".join(field_names)}) '
                             f'VALUES (:project,:{",:".join(field_names)});')
            result_a = self._connection.execute(command_a, additions)
            print(f'{result_a.rowcount} recipients records added for {self.program_id}.')
        # Update the changed ones
        if len(updates) > 0:
            command_u = text(f'UPDATE recipients SET ({",".join(field_names)}) '
                             f'= (:{",:".join(field_names)}) WHERE recipientid=:recipientid;')
            result_u = self._connection.execute(command_u, updates)
            print(f'{result_u.rowcount} recipients records updated for {self.program_id}.')

    def _get_deployment_ids(self) -> List[int]:
        command = text('SELECT deploymentnumber, id FROM deployments WHERE project=:program_id;')
        result = self._connection.execute(command, {'program_id': self.program_id})
        db_deployments: Dict[int, int] = {int(r['deploymentnumber']): r['id'] for r in
                                          [dict(row) for row in result]}

        missing = []
        for num in [d.deploymentnumber for d in self._program.deployments]:
            if num not in db_deployments:
                missing.append(num)
            elif self._use_deployment_ids:
                self._ids[num] = db_deployments[num]
        return missing

    def _ensure_deployments_and_get_ids(self):
        def differ(db_row, csv_row: Spec.Deployment) -> bool:
            """
            Does the new data (csv_row) differ from the database (db_row) in any way that we care about?
            :param db_row: Existing data.
            :param csv_row: New data.
            :return: True if we should update the database from the new data.
            """
            return _rows_differ(self._program.make_deployment(dict(db_row)), csv_row,
                                required_columns=Spec.deployment_required_fields, ignored_columns=['deployment'], diffs=diffs)

        # [ { column: (sql: 'sql_v', csv: 'csv_v') } ]
        diffs: List[Dict[str, Any]] = []

        command = text('SELECT * FROM deployments WHERE project=:program_id;')
        values = {'program_id': self.program_id}
        result = self._connection.execute(command, values)
        db_deployments: Dict[int, Dict[str, Any]] = {int(r['deploymentnumber']): r for r in
                                                     [dict(row) for row in result]}

        csv_deployments: Dict[int, Spec.Deployment] = {d.deploymentnumber: d for d in self._program.deployments}
        updates = []
        additions = []
        for num, depl in csv_deployments.items():
            if num not in db_deployments:
                additions.append({'program_id': self.program_id, 'deployment': depl.deployment,
                                  'deploymentnumber': num, 'startdate': depl.startdate, 'enddate': depl.enddate})
            else:
                depl_id: Optional[int] = None
                if self._use_deployment_ids:
                    depl_id = db_deployments[num]['id']
                    self._ids[num] = depl_id
                if differ(db_deployments[num], depl):
                    updates.append({'program_id': self.program_id, 'deploymentnumber': num,
                                    'startdate': depl.startdate, 'enddate': depl.enddate,
                                    'id': depl_id})

        if len(additions) > 0:
            command_a = text('INSERT INTO deployments (project, deployment, deploymentnumber, startdate, enddate) '
                             'VALUES (:program_id, :deployment, :deploymentnumber, :startdate, :enddate);')
            result_a = self._connection.execute(command_a, additions)
            print(f'{result_a.rowcount} deployment records added for {self.program_id}.')
            # Query the deployments after the insert, and get the ids of the newly added rows.
            result = self._connection.execute(command, values)
            for row in result:
                num = int(row.deploymentnumber)
                if self._use_deployment_ids and num not in self._ids:
                    self._ids[num] = int(row.id)

        if len(updates) > 0:
            if self._use_deployment_ids:
                command_u = text('UPDATE deployments SET project=:program_id, '
                                 'deploymentnumber=:deploymentnumber, startdate=:startdate, enddate=:enddate '
                                 'WHERE id=:id;')
            else:
                command_u = text(
                    'UPDATE deployments SET startdate=:startdate, enddate=:enddate '
                    'WHERE project=:program_id AND deploymentnumber=:deploymentnumber;')
            result_u = self._connection.execute(command_u, updates)
            print(f'{result_u.rowcount} deployment records updated for {self.program_id}.')

    def _save_playlist(self, deployment_id: int, db_playlist: Spec.DbPlaylist, deploymentnumber: int) -> int:
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
        values = {'program_id': self.program_id, 'deployment_id': deployment_id, 'position': db_playlist.position,
                  'title': db_playlist.title, 'audience': db_playlist.audience, 'deploymentnumber': deploymentnumber}

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

    def _save_message(self, playlist_id: int, db_message: Spec.DbMessage) -> None:
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
        values = {'program_id': self.program_id, 'playlist_id': playlist_id, 'position': db_message.position,
                  'title': db_message.title, 'format': db_message.format,
                  'default_category_code': db_message.default_category_code, 'variant': db_message.variant,
                  'sdg_goal_id': db_message.sdg_goal_id, 'sdg_target': db_message.sdg_target,
                  'sdg_target_id': db_message.sdg_target_id, 'key_points': db_message.key_points,
                  'audience': db_message.audience, 'languages': db_message.languages}

        self._connection.execute(command, values)

        if db_message.languages and self._use_message_languages:
            # Retrieve the newly allocated id.
            message_id = -1
            command = text('SELECT id FROM messages WHERE program_id=:program_id AND playlist_id=:playlist_id '
                           'AND position=:position;')
            result = self._connection.execute(command, values)
            for row in result:
                message_id = row[0]

            language_list = [x.strip() for x in db_message.languages.split(',')]
            for language in language_list:
                command = text(
                    'INSERT INTO message_languages(message_id, language_code) VALUES(:message_id, :language_code);')
                values = {'message_id': message_id, 'language_code': language}
                self._connection.execute(command, values)

    def _save_content(self) -> None:
        db_deployment: Spec.DbDeployment
        deployment_id: Optional[int] = None
        for deployment_num, db_deployment in self._db_deployments.items():
            # look up the deployment
            if self._use_deployment_ids:
                deployment_id = self._ids[deployment_num]
            db_playlist: Spec.DbPlaylist
            for db_playlist in db_deployment.db_playlists.values():
                # Create the playlist
                playlist_id = self._save_playlist(deployment_id, db_playlist=db_playlist,
                                                  deploymentnumber=deployment_num)
                db_message: Spec.DbMessage
                for db_message in db_playlist.db_messages:
                    # Create the message
                    self._save_message(playlist_id, db_message)

    def _update_database(self, content_only: bool = False):

        self._use_deployment_ids = not db.table_has_column('playlists', 'deploymentnumber')
        self._use_message_languages = not db.table_has_column('messages', 'languages')
        self._audience_in_messages = db.table_has_column('messages', 'audience')

        self._deployments_with_content = [n for n, depl in self._db_deployments.items() if len(depl.db_playlists) > 0]

        if content_only:
            missing = self._get_deployment_ids()
            if len(missing) > 0:
                raise Exception(f'Missing deployment(s) for content: {", ".join([str(x) for x in missing])}')

        command = text('SELECT * FROM content WHERE project=:program_id;')
        values = {'program_id': self.program_id}
        result = self._connection.execute(command, values)
        content = [x for x in result]
        num_messages = len(content)
        print(f'{len(content)} messages before save for {self.program_id}.')

        command = text('DELETE FROM playlists WHERE program_id=:program_id;')
        values = {'program_id': self.program_id}
        self._connection.execute(command, values)

        if not content_only:
            self._ensure_deployments_and_get_ids()
        self._save_content()

        command = text('SELECT * FROM content WHERE project=:program_id;')
        values = {'program_id': self.program_id}
        result = self._connection.execute(command, values)
        content = [x for x in result]
        if num_messages > len(content):
            print(f'*************\n{self.program_id}: {num_messages - len(content)} fewer message(s) after import!')
        print(f'{len(content)} messages after save for {self.program_id} (was {num_messages}).')

        if not content_only:
            self._infer_program_info()
            self._merge_recipients()

    def update_db_program_spec(self, db_connection: Connection, content_only: bool = False,
                               remove_empty_deployments: bool = False):
        self._connection = db_connection
        # split the "Content" into Playlists and Messages. Delete all the playlists & messages, and re-add them.
        self._db_deployments = Spec.flat_content_to_hierarchy(self._program,
                                                              category_code_converter=lambda c: self._get_category_code(
                                                                  c))
        return self._update_database(content_only=content_only)
