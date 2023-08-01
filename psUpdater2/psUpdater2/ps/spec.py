import copy
import dataclasses
import datetime
import json
import re
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import List, Optional, Union, Dict, Any, Tuple

from sqlalchemy.engine import Engine, Connection

JSONABLE_TYPE_RE = re.compile(r'(?i)(typing.)?(list|dict)(\[\w*])?')

class _SpecClass:
    """
    A base class for Program Specification dataclass objects. The names of the fields in the
    dataclass objects must match the database column names in the corresponding PostgreSQL table(s).
    NOTE: underscores are ignored when mathing column names to member names.

    It provides several helpers for importing and exporting program spec items in various formats,
    customizable via a "metadata" member in the "field" property of a dataclass member. A field
    definition might look like this:
        @dataclass
        class PsItem(_SpecClass):
            supportentity: str = field(default='', metadata={'alias': 'Support Entity'})
    Here, the "supportentity" has a metadata entry 'alias'. The supported metadata values are:

    alias   - A string or list of strings. When importing a .xlsx any of the listed names will
            be accepted for the member. When exporting a .csv/.xlsx, the alias or first alias
            will be used for the column name. Not used for reading/writing JSON or SQL, which
            use the actual field name (which must match the SQL database name).
    import  - A boolean. If the value is False, the field is never imported. If there is no "import",
            the field is always imported (if present).
    export  - A boolean or a list. If "False", the field is never exported. If a list, the
            field is exported when the export target ("sql", "csv", "xlsx", or "json") is in the list.
    parser - A function to parse or normalize the field. Needs to know how to "do the right thing",
            and should be prepared to handle input from database, JSON, and XLSX (we currently don't
            import CSV).
    formatter - A function to format the field for outout, or a dict of functions by data destination
            ("sql", "csv", etc.). if "xlsx" is specified and "csv" is not, the xlsx formatter is
            also used for csv.

    """

    # region Parsers
    ####################################################################################################

    @staticmethod
    def parse_date(x: Any, default=None) -> datetime.date:
        """
        Tries to coerce a value to a date. In particular, forces a datetime to a date.
        :param x: Value to be coerced.
        :param default: value to be returned if the provided argument can't be coerced.
        :return: the provided value as a datetime.date, or the default value if that's not possible.
        """
        if isinstance(x, datetime.date):
            return x.date() if isinstance(x, datetime.datetime) else x
        try:
            datetime_pattern = re.compile(r'(\d{2,4}-\d{2}-\d{2})')
            ymd_format = '%Y-%m-%d'
            return datetime.datetime.strptime(datetime_pattern.search(x).group(), ymd_format).date()
        except Exception:
            pass
        return default

    @staticmethod
    def parse_int(v):
        return int(v or 0)

    @staticmethod
    def parse_str(v):
        if v is None: return None
        return str(v).strip()

    @staticmethod
    def parse_nullable_str(v):
        v = str(v)
        return v if v else None

    @staticmethod
    def parse_sdg(v):
        """
        Turns a string like "1. End poverty in all its forms everywhere." into "1".
        """
        if v is None:
            return None
        # This may be like "1.2 End hunger..." Drop the text part, and keep
        # just the number. Drop trailing '.'
        parts = str(v).split(' ')
        if parts:
            v = parts[0]
        if len(v)>1 and v[-1] == '.':
            v = v[0:-1]
        return v

    @staticmethod
    def parse_delimited_list(v):
        """
        Removes spaces from a comma (or semicolor) separated list of values, like:
        "en, fr" => "en,fr"
        "dga; en" => "dga,en"
        """
        if not v: return ''
        # Sequeeze out whitespace in the list
        return ','.join(re.split('[;,\\s]+', v))

    @staticmethod
    def parse_object(v, default=None):
        """
        Loads an object from a JSON string. If the value is not a string, it is returned
        as-is. 
        """
        if not isinstance(v, str):
            return v or (default if default is not None else v)
        # Ensure parsable. Sometimes the string was incorrectly encoded with single quotes.
        x = default
        try:
            x = json.loads(v)
            # if here, valid json
        except:
            vq = v.replace("'", '"')
            try:
                x = json.loads(vq)
                # if here, replacing ' with " made it valid json
            except:
                pass
        return x if x is not None else default

    # noinspection PyUnresolvedReferences
    @staticmethod
    def parse_field(field_def, v):
        parser = field_def.metadata.get('parser')
        if parser:
            return parser(v)

        if field_def.type == str:
            return _SpecClass.parse_str(v)
        elif field_def.type == int:
            return _SpecClass.parse_int(v)
        elif field_def.type == list or field_def.type == dict or JSONABLE_TYPE_RE.match(str(field_def.type)):
            return _SpecClass.parse_object(v)
        elif field_def.type == datetime.date or field_def.type == datetime.datetime.date:
            return _SpecClass.parse_date(v)
        # identity
        return v

    @classmethod
    def parse(cls: dataclass, rec: Dict) -> Dict[str, Any]:
        """
        Given a Dict of values, normalize them to construct a member of the given
        dataclass.
        :param rec: The record to be normalized.
        :param cls: The dataclass for which the record is to be normalized.
        :return: The normalized record
        """
        def value_or_default(k: str, v: Any) -> Any:
            fd = field_defs[k]
            if v is None:
                # noinspection PyProtectedMember,PyUnresolvedReferences
                if not isinstance(fd.default, dataclasses._MISSING_TYPE):
                    return fd.default
                elif not isinstance(fd.default_factory, dataclasses._MISSING_TYPE):
                    return fd.default_factory()
                else:
                    return None

            new_v = _SpecClass.parse_field(fd, v)
            return new_v

        # Remap from external names to internal, eg, 'Deployment #' -> 'deploymentnumber'
        mapped_rec = {in_name: rec[ext_name] for ext_name, in_name in cls.external_to_internal_map.items() if
                      ext_name in rec}
        field_defs = {field_def.name: field_def for field_def in fields(cls) if
                      field_def.metadata.get('import', True)}
        return {k: value_or_default(k, v) for k, v in mapped_rec.items() if k in field_defs}

    # endregion

    # region Formatters (output)
    ####################################################################################################
    @staticmethod
    def format_csv_str(value):
        if value is None:
            return ''
        return str(value)

    @staticmethod
    def format_delimited_list(value):
        if not value:
            return ''
        if isinstance(value, list):
            return ','.join(str(v) for v in value)
        if isinstance(value, str):
            return ','.join(re.split('[;,\\s]+', value))
        return ''

    @staticmethod
    def format_field(field_def, value, target):
        formatter_given = 'formatter' in field_def.metadata  # Is there an explicit formatter for this field (and possibly target)?
        formatter = field_def.metadata.get('formatter')
        if isinstance(formatter, dict):
            # (some) per-target formatter(s)
            if target in formatter:
                # There is an explicit formatter for this target
                formatter = formatter[target]
                formatter_given = True
            elif target == 'csv' and 'xlsx' in formatter:
                formatter = formatter['xlsx']
                formatter_given = True
            else:
                formatter_given = False
        if formatter_given:
            return formatter(value)

        if field_def.type == int:
            if value is None:
                return '' if target != 'sql' else None
            return int(value)
        elif isinstance(value, list) or isinstance(value, dict):
            return json.dumps(value)
        elif isinstance(value, set):
            return json.dumps(list(value))
        elif target in ['csv', 'xlsx']:
            return _SpecClass.format_csv_str(value)
        elif value is None:
            return '' if target != 'sql' else None
        else:
            return str(value)

    # endregion

    #region Internal <-> External mapping
    @classmethod
    @property
    def internal_to_csv_map(dc: dataclass) -> Dict[str, str]:
        """
        Produces a mapping of internal names to csv names. Use this map when exporting to csv.
        :param dc: A dataclass for which to produce the mapping.
        :return: the mapping.
        """

        def alias(field_def):
            a = field_def.metadata.get('alias')
            if isinstance(a, str):
                return a
            elif isinstance(a, Iterable):
                return next(iter(a))
            else:
                return field_def.name.replace('_', ' ').title()

        return {field_def.name: alias(field_def) for field_def in fields(dc) if _SpecClass.should_export(field_def, 'csv')}

    @classmethod
    @property
    def external_to_internal_map(dataclass: dataclass) -> Dict[str, str]:
        """
        Produces a mapping of external names to internal names. Use this mapping when importing.
        :param dataclass: A dataclass for which to produce the mapping.
        :return: the mapping.
        """

        def aliases(field_def) -> List[str]:
            a = field_def.metadata.get('alias')
            if isinstance(a, str):
                result = [a]
            elif isinstance(a, Iterable):
                result = [x for x in a]
            else:
                result = [field_def.name.replace('_', ' ').title()]
            result.append(field_def.name)
            return result

        dc_fields = [field_def for field_def in fields(dataclass) if field_def.metadata.get('import', True)]
        return {alias: field_def.name for field_def in dc_fields for alias in aliases(field_def)}

    @classmethod
    @property
    def csv_to_internal_map(cls: dataclass) -> Dict[str, str]:
        """
        Produces a mapping of csv names to internal names. Use this mapping when importing from csv.
        :param cls: A dataclass for which to produce the mapping.
        :return: the mapping.
        """

        def aliases(field_def) -> List[str]:
            a = field_def.metadata.get('alias')
            if isinstance(a, str):
                result = [a]
            elif isinstance(a, Iterable):
                result = [x for x in a]
            else:
                result = [field_def.name.replace('_', ' ').title()]
            result.append(field_def.name)
            return result

        dc_fields = [field_def for field_def in fields(cls) if cls.should_export(field_def, 'csv')]
        return {alias: field_def.name for field_def in dc_fields for alias in aliases(field_def)}

    @classmethod
    @property
    def required_columns(cls:dataclass) -> List[str]:
        def required(field_def):
            # noinspection PyProtectedMember,PyUnresolvedReferences
            return ((isinstance(field_def.default, dataclasses._MISSING_TYPE) and
                     isinstance(field_def.default_factory, dataclasses._MISSING_TYPE)) or
                    field_def.metadata.get('required', False))

        # field_defs = {dcf.name: dcf for dcf in fields(dc)}
        return [field_def.name for field_def in fields(cls) if required(field_def)]

    @classmethod
    def sql_columns(cls: dataclass) -> List[str]:
        return [field_def.name for field_def in fields(cls) if cls.should_export(field_def, 'sql')]

    #endregion

    @staticmethod
    def should_export(field_def: field, target: str = None, **kwargs) -> bool:
        md = field_def.metadata.get('export', True)
        if isinstance(md, bool):
            return md
        if isinstance(md, list):
            if target not in md:
                return False
        # {'export': 'XYZ', 'value': Any} means "keep the value for XYZ if it has a value"
        if 'value' in kwargs and ('value' in md or 'not_none' in md):
            return kwargs['value'] is not None
        return True  # target was in md

    @property
    def todict(self) -> Dict[str, Any]:
        # Get the values with 'getattr' rather than 'asdict()' because some progspec types have references to their
        # parent; ie recursive structures. With this, we also get access the fields we care about.
        # noinspection PyDataclass
        return {dcf.name: getattr(self, dcf.name) for dcf in fields(self) if _SpecClass.should_export(dcf, 'csv')}

    # region SQL export
    @property
    def sql_row(self) -> Dict[str, Any]:
        result = {}
        for field_def in fields(self):
            value = getattr(self, field_def.name)
            if _SpecClass.should_export(field_def, 'sql', value=value):
                result[field_def.name] = _SpecClass.format_field(field_def, value, 'sql')
        return result

    # endregion

    # region JSON Export
    @property
    def json_object(self) -> dict:
        """
        Creates an exportable JSON object from a dataclass object.
        :param dc_object: An instance of a dataclass object.
        """
        result = {}
        for field_def in fields(self):
            value = getattr(self, field_def.name)
            if _SpecClass.should_export(field_def, 'json', value=value):
                result[field_def.name] = str(value) if isinstance(value, datetime.date) else value
        return result

    # endregion

    # region CSV Export
    @staticmethod
    def csv_column_name(field_def):
        """
        Return the name of the field in a .csv file, or None if the field should not
        be exported to a .csv
        """
        if export := field_def.metadata.get('export'):
            if export == False or 'csv' not in export:
                return None
        # field name unless there's a csv override
        return field_def.metadata.get('csv', field_def.name)

    @classmethod
    @property
    def csv_header(cls) -> Tuple:
        csv_fields = [_SpecClass.csv_column_name(field_def) for field_def in fields(cls) if
                      _SpecClass.csv_column_name(field_def)]
        return tuple(csv_fields)

    @property
    def csv_row(self) -> Tuple:
        # from ps.utils import csv_row
        # return csv_row(self.todict, self)
        values = self.todict
        result = []
        field_defs = {field_def.name: field_def for field_def in fields(self)}
        for k, v in values.items():
            field_def = field_defs.get(k)
            if _SpecClass.csv_column_name(field_def):
                v = _SpecClass.format_field(field_def, v, 'xlsx')
                # if field_def.metadata.get('as_json') or field_def.metadata.get('as_depls'):
                #     v = json.dumps(v)
                result.append(v)
        return tuple(result)
    # endregion


# region Recipient
####################################################################################################
@dataclass
class Recipient(_SpecClass):
    program_id: str = field(metadata={'export': [False]})
    country: str
    language: str = field(metadata={'alias': 'Language Code'})
    region: str = ''
    district: str = ''
    communityname: str = field(default='', metadata={'alias': 'Community'})
    groupname: str = field(default='', metadata={'alias': 'Group Name'})
    agent: str = ''
    variant: str = ''
    listening_model: str = ''
    group_size: int = 0
    numhouseholds: int = field(default=0, metadata={'alias': '# HH'})
    numtbs: int = field(default=0, metadata={'alias': '# TBs'})
    supportentity: str = field(default='', metadata={'alias': 'Support Entity'})
    agent_gender: str = field(default=None, metadata={'parser':_SpecClass.parse_nullable_str})
    direct_beneficiaries: int = None
    direct_beneficiaries_additional: dict = field(default_factory=dict, metadata={'as_json': True})
    indirect_beneficiaries: int = None
    deployments: list[int] = field(default_factory=list, metadata={'parser': _SpecClass.parse_object, 'as_depls': True})
    recipientid: str = field(default=None, metadata={'alias': ['RecipientID', 'RecipientId']})
    affiliate: str = field(default=None, metadata={'export': [False]})
    partner: str = field(default=None, metadata={'export': [False]})
    component: str = field(default=None, metadata={'export': [False]})
    new_recipientid: bool = field(default=False, metadata={'export': [False], 'import': False})

    def __post_init__(self):
        if not self.recipientid:
            self.recipientid = self.compute_recipientid()
            self.new_recipientid = True

    def _after_clone_into_new_program(self, program_id):
        self.program_id = program_id
        self.recipientid = self.compute_recipientid()
        self.new_recipientid = True

    def compute_recipientid(self) -> str:
        # all the parts that must be different between recipients
        ids = (
            self.program_id, self.country, self.region, self.district, self.communityname, self.agent,
            self.groupname,
            self.language, self.variant)
        name = '-'.join([r if r else '' for r in ids])
        # Uncomment next line to add some salt ⃰ to the string.
        #   ⃰yes, it's not really salt, because we don't save it per recipient. It simply adds some randomness.
        # name += str(random.random())
        import hashlib
        str_hash = hashlib.sha1(name.encode('utf-8'))
        digx = str_hash.hexdigest()
        id16 = digx[:16]
        return id16

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, Recipient):
            return False
        op: Recipient = o
        eq: bool = (self.country == op.country and
                    self.language == op.language and
                    self.region == op.region and
                    self.district == op.district and
                    self.communityname == op.communityname and
                    self.groupname == op.groupname and
                    self.agent == op.agent and
                    self.variant == op.variant and
                    self.listening_model == op.listening_model and
                    self.group_size == op.group_size and
                    self.numhouseholds == op.numhouseholds and
                    self.numtbs == op.numtbs and
                    self.supportentity == op.supportentity and
                    (self.agent_gender == op.agent_gender or not self.agent_gender and not op.agent_gender) and
                    self.direct_beneficiaries == op.direct_beneficiaries and
                    self.direct_beneficiaries_additional == op.direct_beneficiaries_additional and
                    self.indirect_beneficiaries == op.indirect_beneficiaries and
                    self.deployments == op.deployments and
                    self.recipientid == op.recipientid)
        return eq


Recipient.id_fields = ['country', 'region', 'district', 'communityname', 'groupname', 'agent', 'language', 'variant']


# endregion

# region Message
####################################################################################################
@dataclass
class Message(_SpecClass):
    # 'alias' is what we might find in the .csv file.
    # Exported to a csv, but not to json or the db.
    deployment_num: int = field(default=None, metadata={'export': ['csv'], 'alias': 'Deployment #', })
    playlist_title: str = field(default=None, metadata={'export': ['csv'], 'alias': 'Playlist Title'})
    title: str = field(default=None,
                       metadata={'alias': ['Message Title', 'message_title'], 'required': True, 'csv': 'message_title'})
    key_points: str = field(default='')
    # multiple aliases, in a list.
    languages: str = field(default=None,
                           metadata={'alias': ['Language Code', 'Languages', 'languagecode'], 'csv': 'languagecode',
                                     'parser': _SpecClass.parse_delimited_list,
                                     'formatter': _SpecClass.format_delimited_list})
    variant: str = field(default=None, metadata={'parser': _SpecClass.parse_delimited_list,
                                                 'formatter': _SpecClass.format_delimited_list, 'as_csl': True})
    format: str = None
    audience: str = None
    default_category_code: str = field(default=None, metadata={'alias': ['Default Category', 'default_category'],
                                                               'csv': 'default_category'})
    # With an sdg goal/target, we care about only the numeric part "1.2 Eliminate hunger..."
    sdg_goal: str = field(default=None, metadata={'alias': ['SDG Goals', 'SDG Goal', 'sdg_goals'],
                                                  'parser': _SpecClass.parse_sdg, 'csv': 'sdg_goals'})
    sdg_target: str = field(default=None,
                            metadata={'alias': ['SDG Targets', 'SDG Target', 'sdg_targets'],
                                      'parser': _SpecClass.parse_sdg, 'csv': 'sdg_targets'})
    # Not exported at all, and not imported, either.
    parent: object = field(default=None, metadata={'export': [False], 'import': False, 'csv': None})
    position: int = field(default=None, metadata={'export': [False], 'alias': 'message_pos', 'csv': None})

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, Message):
            return False
        op: Message = o
        eq: bool = (self.deployment_num == op.deployment_num and
                    self.playlist_title == op.playlist_title and
                    self.title == op.title and
                    self.key_points == op.key_points and
                    self.languages == op.languages and
                    (self.variant == op.variant or not self.variant and not op.variant) and
                    (self.format == op.format or not self.format and not op.format) and
                    (self.audience == op.audience or not self.audience and not op.audience) and
                    self.default_category_code == op.default_category_code and
                    self.sdg_goal == op.sdg_goal and
                    self.sdg_target == op.sdg_target and
                    self.position == op.position)
        return eq

    # @property
    # def playlist_title(self):
    #     # noinspection PyUnresolvedReferences
    #     return self.parent.title
    #
    # @property
    # def deployment_num(self):
    #     # noinspection PyUnresolvedReferences
    #     return self.parent.parent.deploymentnumber


Message.id_fields = ['deployment_num', 'playlist_title', 'title']


# endregion

# region Playlist
####################################################################################################
@dataclass
class Playlist(_SpecClass):
    title: str = field(metadata={'alias': ['Playlist Title', 'playlist_title']})
    messages: List[Message] = field(default_factory=list, metadata={'export': [False], 'import': False})
    parent: object = field(default=None, metadata={'export': [False], 'import': False})
    position: int = field(default=None, metadata={'export': [False], 'alias': 'playlist_pos'})

    def add_message(self, message: Union[Dict, Message]):
        if isinstance(message, dict):
            message_dict = Message.parse(message)
            message = Message(**message_dict)

        message.parent = self
        message.position = len(self.messages) + 1
        message.playlist_title = self.title
        # noinspection PyUnresolvedReferences
        message.deployment_num = self.parent.deploymentnumber
        self.messages.append(message)

        return message

    @property
    def audience(self) -> str:
        # get the concensus audience
        a_list = {}
        for m in self.messages:
            if m.audience in a_list:
                a_list[m.audience] = a_list[m.audience] + 1
            else:
                a_list[m.audience] = 1
        s_list = sorted([(v, k) for k, v in a_list.items()], key=lambda i: i[0])
        return s_list[-1][1]

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, Playlist):
            return False
        op: Playlist = o
        eq: bool = (self.title == op.title and
                    self.messages == op.messages and
                    self.position == op.position)
        return eq


# endregion

# region Deployment
####################################################################################################
@dataclass
class Deployment(_SpecClass):
    deploymentnumber: int = field(metadata={'alias': ['Deployment #', 'deployment_num']})
    startdate: datetime.date = field(default=None, metadata={'alias': ['Start Date', 'deployment_startdate']})
    enddate: datetime.date = field(default=None, metadata={'alias': ['End Date', 'deployment_enddate']})
    deploymentname: str = field(default=None, metadata={'alias': ['Deployment Name', 'deployment_name']})
    # Export only to json, and then only if there's a value (don't export None).
    deployment: str = field(default=None, metadata={'export': ['json', 'value']})
    deployed: bool = field(default=None, metadata={'export': ['json', 'value'], 'import': 'computed', 'csv': None})
    playlists: List[Playlist] = field(default_factory=list, metadata={'export': [False], 'import': False, 'csv': None})

    def add_playlist(self, playlist: Union[Dict, Playlist]) -> Playlist:
        if isinstance(playlist, dict):
            playlist_dict = Playlist.parse(playlist)
            playlist = Playlist(**playlist_dict)

        playlist.parent = self
        playlist.position = len(self.playlists) + 1
        self.playlists.append(playlist)
        return playlist

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, Deployment):
            return False
        op: Deployment = o
        eq: bool = (self.deploymentnumber == op.deploymentnumber and
                    self.startdate == op.startdate and
                    self.enddate == op.enddate and
                    self.deploymentname == op.deploymentname and
                    self.playlists == op.playlists)
        return eq


# endregion

# region General
####################################################################################################
@dataclass
class General(_SpecClass):
    program_id: str = field(metadata={'alias': ['Program ID', 'program_id', 'programid', 'project']})
    name: str = None
    country: str = None
    region: List[str] = field(default_factory=list, metadata={'alias': ['region', 'Regions'], 'as_json': True})
    languages: List[str] = field(default_factory=list, metadata={'as_json': True})
    deployments_count: int = 0
    deployments_length: str = 'one-quarter'
    deployments_first: datetime.date = field(default=datetime.datetime.now().date())
    listening_models: List[str] = field(default_factory=list, metadata={'as_json': True})
    feedback_frequency: str = 'one-quarter'
    sustainable_development_goals: list[int] = field(default_factory=list, metadata={'as_json': True})
    direct_beneficiaries_map: dict = field(
        default_factory=lambda: {"male": "Number of Male", "female": "Number of Female", "youth": "Number of Youth"},
        metadata={'as_json': True})
    direct_beneficiaries_additional_map: dict = field(default_factory=dict, metadata={'as_json': True})
    salesforce_id: str = field(default='', metadata={'export': ['json', 'csv']})
    tableau_id: str = field(default=None, metadata={'export': ['json', 'csv']})
    affiliate: str = 'Amplio'
    partner: str = 'Amplio Partner'


# endregion

# region ProgramSpec
####################################################################################################
class ProgramSpec:
    def __init__(self, program_id):
        self.program_id = program_id
        self._deployments: List[Deployment] = []
        self._recipients: List[Recipient] = []
        self._general: Optional[General] = None

    @property
    def general(self):
        return self._general

    @property
    def deployments(self):
        return self._deployments

    @property
    def content(self):
        return [msg for depl in self._deployments for pl in depl.playlists for msg in pl.messages]

    @property
    def recipients(self):
        return self._recipients

    def add_general(self, general: Union[Dict, General]) -> General:
        if isinstance(general, dict):
            general_dict = General.parse(general)
            general = General(**general_dict)
        self._general = general
        return general

    def add_content(self, content: Dict) -> None:
        depl_dict = Deployment.parse(content)
        depls = [d for d in self._deployments if d.deploymentnumber == depl_dict['deploymentnumber']]
        deployment = depls[0] if depls else self.add_deployment(content)
        pl_dict = Playlist.parse(content)
        if 'title' in pl_dict and pl_dict['title']:
            # The content also defines a playlist
            pls = [pl for pl in deployment.playlists if pl.title == pl_dict['title']]
            playlist = pls[0] if pls else deployment.add_playlist(content)
            message_dict = Message.parse(content)
            if 'title' in message_dict and message_dict['title']:
                # The content also defines a message
                playlist.add_message(content)

    def add_deployment(self, deployment: Union[Dict, Deployment]) -> Deployment:
        if isinstance(deployment, dict):
            deployment_dict = Deployment.parse(deployment)
            deployment = Deployment(**deployment_dict)
        self.deployments.append(deployment)
        return deployment

    def add_recipient(self, recipient: Union[Dict, Recipient]) -> Recipient:
        if isinstance(recipient, dict):
            recipient_dict = Recipient.parse(recipient)
            recipient_dict['program_id'] = self.program_id
            recipient = Recipient(**recipient_dict)
        self.recipients.append(recipient)
        return recipient

    def infer_general_info(self):
        if self._general is not None:
            raise 'General has already been set'
        # Gather languages, regions, and listening models from recipients.
        languages = set()
        regions = set()
        listening_models = set()
        recip: Recipient
        for recip in self._recipients:
            languages.add(recip.language)
            regions.add(recip.region)
            listening_models.add(recip.listening_model)
        # Gather sdgs from messages.
        sdgs = set()
        content: Message
        for content in self.content:
            if content.sdg_goal:
                sdgs.add(int(content.sdg_goal))
        # Find the earliest known deployment start
        deployments_first = datetime.datetime(2100, 1, 1).date()  # far future
        depl: Deployment
        for depl in self._deployments:
            depl_start = _SpecClass.parse_date(depl.startdate)
            if (depl_start - deployments_first).days < 0:
                deployments_first = depl_start
        # Find most mentioned country.
        country = Counter([recip.country for recip in self._recipients]).most_common(1) or 'USA'
        # country = self._program.recipients[0].country if len(self._program.recipients) > 0 else 'USA'
        general_dict = {
            'deployments_count': len(self._deployments),
            'languages': json.dumps(languages),
            'regions': json.dumps(regions),
            'listening_models': json.dumps(listening_models),
            'sustainable_development_goals': json.dumps(sdgs),
            'deployments_first': deployments_first,
            'country': country,
        }
        self.add_general(general_dict)

    def write_to_json(self, json_args=None, to_string:bool=True) -> str:
        """
        Exports a program spec as JSON.
        :param json_args: to be passed to the json converter, eg: json_args={'indent':2}
        :return: The program spec encoded as JSON.
        """
        from ps import write_to_json
        return write_to_json(self, json_args=json_args, to_string=to_string)

    def write_to_xlsx(self, path: Optional[Path] = None) -> bytes:
        """
        Exports a Program Specification to a spreadsheet.

        :param path: An optional Path to which the spreadsheet file will be written.
        :return: The bytes of the spreadsheet.
        """
        from ps import write_to_xlsx
        return write_to_xlsx(self, path)

    def write_to_csv(self, artifact: str, path: Optional[Path] = None) -> Optional[str]:
        """
        Export an artifact from a Program Specification. The artifact is one of ['general', 'deployments', 'content', 'recipients'],
        the three sheets of the Program Specification spreadsheet, and the three csv files used by the ACM.

        :param program_spec: The Program Specification from which a csv is to be exported.
        :param artifact: The artifact to be exported.
        :param path: An optional Path to which the csv will be written.
        :return: The csv data, as a string.
        """
        from ps import write_to_csv
        return write_to_csv(self, artifact, path)

    def write_to_db(self, engine: Engine = None, connection: Connection = None, **kwargs) -> \
            Tuple[bool, List[str]]:
        from ps import export_to_db
        return export_to_db(self, engine, connection, **kwargs)

    @staticmethod
    def create_from_json(programid: str, str_or_dict: Union[str, Dict], json_args=None) -> \
            Tuple[object, List[str]]:
        from ps import read_from_json
        return read_from_json(programid, str_or_dict, json_args)

    @staticmethod
    def create_from_xlsx(programid: str, data_or_path: Union[bytes, Path]) -> \
            Tuple[object, List[str]]:
        # in Python 3.11 that "Optional[object]" can become "Optional[Self]"
        from ps import read_from_xlsx
        return read_from_xlsx(programid, data_or_path)

    @staticmethod
    def create_from_db(programid: str, engine: Engine = None, connection: Connection = None) -> \
            Tuple[object, List[str]]:
        from ps import read_from_db
        return read_from_db(programid, engine, connection)

    @staticmethod
    def create_from_s3(programid: str, bucket: str = None, artifact: str = None, versionid: str = None) -> \
            Tuple[object, List[str]]:
        from ps import read_from_s3
        return read_from_s3(programid, bucket=bucket, artifact=artifact, versionid=versionid)

    @staticmethod
    def clone_from_other(other, program_id: str):
        new_spec = copy.deepcopy(other)
        new_spec.program_id = program_id
        if new_spec._general:
            new_spec._general.program_id = program_id
        for recip in new_spec.recipients:
            recip._after_clone_into_new_program(program_id)
        return new_spec

# endregion
