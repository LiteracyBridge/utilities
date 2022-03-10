import traceback
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

import ExportProcessor
import Spec

PUBLISHED_PREFIX: str = 'pub_'
UNPUBLISHED_PREFIX: str = 'unpub_'

CSV_ARTIFACTS = ['general', 'deployments', 'content', 'recipients']


class Exporter:
    def __init__(self, program_id: str, engine: Engine, program_spec: Spec.Program = None):
        self.program_id = program_id
        self.engine = engine
        if program_spec is None:
            self.program_spec = Spec.Program(program_id)
            self._opened = False
        else:
            self.program_spec = program_spec
            self._opened = True

    def read_from_database(self) -> Spec.Program:
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
                {'program_id': self.program_id})

        # This isn't an updating connection, so we don't need the context manager wrapped auto-restore-content one.
        with self.engine.connect() as conn:
            result = query_table(Spec.content_sql_2_csv.keys(), 'content')
            for row in result:
                self.program_spec.add_content(dict(row))

            result = query_table(Spec.recipient_sql_2_csv.keys(), 'recipients')
            for row in result:
                self.program_spec.add_recipient(dict(row))

            columns = [x for x in Spec.deployment_sql_2_csv.keys()]
            columns.append('deployment')
            group_by = f'group by {",".join(columns)}'
            columns.append(
                "deployment in (select distinct deployment from tbsdeployed where project=:program_id) as deployed")
            result = query_table(columns, 'deployments', group_by=group_by, order_by='ORDER BY deploymentnumber')
            for row in result:
                self.program_spec.add_deployment(dict(row))

            # 'program_id' is unique in the programs table, so there will be at most one row. But maybe none, if the
            # program hasn't been fully initialized yet.
            result = query_table(['*'], 'programs', programid_column='program_id')
            program_rows = [dict(row) for row in result.all()]
            if len(program_rows) == 1:
                self.program_spec.add_general(program_rows[0])
            self._opened = True
            return self.program_spec

    def do_save(self, path: Path = None, bucket: str = None, artifacts: List[str] = None,
                names: Dict[str, str] = None, metadata: Dict[str, str] = None) -> Tuple[bool, List[str]]:
        """
        Publishes the program spec from the PostgreSQL database to files/objects.
        :param artifacts: which artifacts are desired? 'published', 'general', 'deployments', 'content', 'recipients'
        :param metadata: metadata to add to s3 object(s)
        :param names: file/object names to use instead of defaults.
        :param path: If provided, the path to which files will be written.
        :param bucket: If provided, the bucket into which objects will be written.
        :return: 
        """

        def publish_csv(art: str) -> bool:
            """
            Publish a single .csv file. May be written to a local file and/or an S3 bucket.
            :param art: The name of the data, 'content', 'recipients', etc.
            :return: A list of errors that were detected. An empty list if no errors.
            """
            # Try to convert the data to a .csv format
            result = True
            try:
                csv_str = exporter.get_csv(art)
            except Exception as ex:
                result = False
                errors.append(f'Could not convert {art}  for {self.program_id} to .csv: {str(ex)}.')
                return result
            # A brand-new, empty program spec may not have a 'general' tab; that's fine.
            if csv_str is None and art == 'general':
                return result
            # Write to a file if desired.
            if path is not None:
                try:
                    csv_path: Path = Path(path, names[art])
                    with csv_path.open(mode='w') as csv_file:
                        csv_file.write(csv_str)
                except Exception as ex:
                    result = False
                    errors.append(f'Could not write {names[art]} for {self.program_id} as file: {str(ex)}')
            # Write to S3 if desirec.
            if bucket is not None:
                try:
                    s3_key = f'{self.program_id}/{names[art]}'
                    s3_put_result = s3.put_object(Body=csv_str, Bucket=bucket, Key=s3_key, Metadata=metadata)
                    if s3_put_result.get('ResponseMetadata', {}).get('HTTPStatusCode') != 200:
                        result = False
                        errors.append(
                            f"Couldn't publish '{names[art]}' for {self.program_id} to s3 in bucket '{bucket}'.")
                except Exception:
                    result = False
                    errors.append(
                        f"Couldn't publish '{names[art]}' for {self.program_id} to s3 in bucket " +
                        f"{bucket}': {traceback.format_exc()}.")
            return result

        if not self._opened:
            raise Exception("Attempt to save an un-opened program spec.")

        exporter: ExportProcessor.ExportProcessor = ExportProcessor.ExportProcessor(self.program_spec)

        if metadata is None:
            metadata = {'submission-date': datetime.now().isoformat()}

        if artifacts is None:
            artifacts = ['published', 'general', 'deployments', 'content', 'recipients']
        if names is None:
            names = {'published': f'{PUBLISHED_PREFIX}progspec.xlsx',
                     'general': f'{PUBLISHED_PREFIX}general.csv',
                     'deployments': f'{PUBLISHED_PREFIX}deployments.csv',
                     'content': f'{PUBLISHED_PREFIX}content.csv',
                     'recipients': f'{PUBLISHED_PREFIX}recipients.csv'
                     }
        errors: List[str] = []

        from botocore.client import BaseClient
        s3: Optional[BaseClient] = None
        if bucket is not None:
            import boto3
            s3 = boto3.client('s3')

        if 'published' in artifacts:
            data = exporter.get_spreadsheet()

            # Write to a file if desired
            if path is not None:
                xlsx_path: Path = Path(path, names['published'])
                with xlsx_path.open(mode='wb') as xls_file:
                    xls_file.write(data)

            # Write to S3 if desired
            if bucket is not None:
                key = f'{self.program_id}/{names["published"]}'
                try:
                    put_result = s3.put_object(Body=data, Bucket=bucket, Key=key, Metadata=metadata)
                    if put_result.get('ResponseMetadata', {}).get('HTTPStatusCode') != 200:
                        errors.append(
                            f"Couldn't publish '{names['published']}' for {self.program_id} " +
                            f"to s3 in bucket '{bucket}'.")
                except Exception:
                    errors.append(
                        f"Couldn't publish '{names['published']}' for {self.program_id} " +
                        f"to s3 in bucket '{bucket}': {traceback.format_exc()}.")

        # Publish the individual csv files
        for artifact in [a for a in artifacts if a in CSV_ARTIFACTS]:
            publish_csv(artifact)

        return len(errors) == 0, errors

    def publish(self, bucket: str) -> Tuple[bool, List[str]]:
        """
        Saves the spreadsheet and .csv files to an S3 bucket, from whence the ACM can retrieve them.
        :param bucket: The bucket to which to save.
        :return: a tuple of (bool, errors)
        """
        if not self._opened:
            raise Exception("Attempt to publish an un-opened program spec.")
        return self.do_save(bucket=bucket)

    def save_unpublished(self, bucket: str, metadata: Dict[str, str] = None) -> Tuple[bool, List[str]]:
        if not self._opened:
            raise Exception("Attempt to publish an un-opened program spec.")
        return self.do_save(bucket=bucket, artifacts=['published'], metadata=metadata,
                            names={'published': f'{UNPUBLISHED_PREFIX}progspec.xlsx'})

    def save(self, output_path: Path) -> Tuple[bool, List[str]]:
        if not self._opened:
            raise Exception("Attempt to publish an un-opened program spec.")
        return self.do_save(path=output_path)

    def do_export(self, output_path: Path = None, bucket: str = None, metadata: Dict[str, str] = None) -> \
            Tuple[bool, List[str]]:
        self.read_from_database()
        return self.do_save(path=output_path, bucket=bucket, metadata=metadata)
