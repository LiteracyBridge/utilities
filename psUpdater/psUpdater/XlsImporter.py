from io import BytesIO
from typing import List, Dict, Tuple, Optional

import openpyxl
from openpyxl.worksheet.worksheet import Worksheet
from sqlalchemy.engine import Engine

import Spec
import db
from ImportProcessor import ImportProcessor
from ImportValidator import Validator
from Spec import content_sql_2_csv, recipient_sql_2_csv, deployment_sql_2_csv


class Importer:
    def __init__(self, program_id: str):
        self._opened = False
        self._recipients: Optional[List[Dict[str, str]]] = None
        self._content: Optional[List[Dict[str, str]]] = None
        self._deployments: Optional[List[Dict[str, str]]] = None
        self._program_spec: Spec.Program = Spec.Program(program_id)
        self.program_id = program_id
        self._sheets: Optional[openpyxl.Workbook] = None

    @property
    def opened(self) -> bool:
        return self._opened

    @property
    def program_spec(self) -> Spec.Program:
        return self._program_spec

    def load(self, data: bytes) -> Tuple[bool, List[str]]:
        result = []
        try:
            _bytes = BytesIO(data)
            self._sheets = openpyxl.load_workbook(_bytes)
            return True, []
        except Exception as ex:
            result.append(f'Exception loading workbook: {ex}')
        return False, result

    def parse(self) -> Tuple[bool, List[str]]:
        def parse_sheet(sheet: Worksheet, sql_2_csv_map: Dict[str, str], additional_map: Dict[str, str] = None) -> \
                List[Dict[str, str]]:
            csv_2_sql_map = {v: k for k, v in sql_2_csv_map.items()}
            if additional_map is not None:
                csv_2_sql_map.update(additional_map)
            # Raw rows
            rows = [r for r in sheet]
            # header values from spreadsheet
            header = [r.value for r in rows[0]]
            rows = rows[1:]
            # what we'll call the values in the output dict. Unknown columns transfered as-is
            column_names = [csv_2_sql_map[h] if h in csv_2_sql_map else h for h in header]
            result = []
            for row in rows:
                # Excel will give us a row of empty values if we've looked at it hard. Skip rows with no data.
                # We don't do anything with formulae, and customers like to put a sum of talking books so skip those
                # as well.
                if all([cell.data_type in 'nf' for cell in row]):
                    continue

                # normalize
                values = [x.value.strip() if isinstance(x.value, str) else x.value for x in row]
                result.append(dict(zip(column_names, values)))
            return result

        try:
            additional_recipient_mappings = {'Model': 'listening_model', 'Language': 'language'}
            self._deployments = parse_sheet(self._sheets['Deployments'], deployment_sql_2_csv)
            self._content = parse_sheet(self._sheets['Content'], content_sql_2_csv)
            if 'Recipients' in self._sheets:
                self._recipients = parse_sheet(self._sheets['Recipients'], recipient_sql_2_csv,
                                               additional_map=additional_recipient_mappings)
            elif 'Components' in self._sheets:
                names_dict = parse_sheet(self._sheets['Components'], {'component': 'Component'})
                names = [x['component'] for x in names_dict]
                self._recipients = []
                for name in names:
                    self._recipients.extend(parse_sheet(self._sheets[name], recipient_sql_2_csv,
                                                        additional_map=additional_recipient_mappings))

            return True, []
        except Exception as ex:
            return False, [f'Exception parsing workbook: {ex}']

    def do_open(self, data: bytes = None) -> Tuple[bool, List[str]]:
        messages: List[str]
        ok, messages = self.load(data)
        if ok:
            ok, msgs = self.parse()
            messages.extend(msgs)
            if ok:
                validator: Validator = Validator(self._deployments,
                                                 self._recipients, self._content)
                ok, msgs = validator.validate()
                messages.extend(msgs)
                if ok:
                    validator.populate_programspec(self._program_spec)
                    self._opened = True
        return ok, messages

    def update_database(self, engine: Engine, commit: bool) -> Tuple[bool, List[str]]:
        if not self._opened:
            raise Exception("Attempt to update database with an un-opened program spec.")
        importer = ImportProcessor(self.program_id, self._program_spec)

        with db.get_db_connection(engine=engine) as conn:
            transaction = conn.begin()
            importer.update_db_program_spec(conn)
            if commit:
                transaction.commit()
                print(f'Changes commited for {self.program_id}')
            else:
                transaction.rollback()
                print(f'Changes rolled back for {self.program_id}')

        return True, []

    def do_import(self, what, *, data: bytes = None, engine: Engine, commit: bool) -> \
            Tuple[bool, List[str]]:
        ok, result = self.do_open(data)
        if ok:
            ok, msgs = self.update_database(engine, commit)
            result.extend(msgs)
        return ok, result
