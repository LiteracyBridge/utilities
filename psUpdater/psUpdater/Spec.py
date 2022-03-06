import dataclasses
import json
import re
from dataclasses import dataclass, field, fields
from datetime import datetime, date
from typing import List, Dict, Any, Optional, Union

"""
Yet another ProgramSpec definition. Contains only what is needed for the spreadsheet importer/exporter.
"""


def asdate(x: Any, default=None) -> date:
    if isinstance(x, date):
        return x.date() if isinstance(x, datetime) else x
    try:
        datetime_pattern = re.compile(r'(\d{3}-\d{2}-\d{2})')
        ymd_format = '%Y-%m-%d'
        x = datetime.strptime(datetime_pattern.search(x).group(), ymd_format).date()
    except Exception:
        x = default
    return x


@dataclass
class Content:
    deployment_num: int
    playlist_title: str
    message_title: str
    key_points: str = ''
    languagecode: str = ''
    variant: str = ''
    format: str = ''
    audience: str = ''
    default_category: str = ''
    sdg_goals: str = ''
    sdg_targets: str = ''


content_sql_2_csv = {
    'deployment_num': 'Deployment #',
    'playlist_title': 'Playlist Title',
    'message_title': 'Message Title',
    'key_points': 'Key Points',
    'languagecode': 'Language Code',
    'variant': 'Variant',
    'format': 'Format',
    'audience': 'Audience',
    'default_category': 'Default Category',
    'sdg_goals': 'SDG Goals',
    'sdg_targets': 'SDG Targets'
}
content_required_fields: List[str] = ['deployment_num', 'playlist_title', 'message_title']
content_id_fields: List[str] = ['deployment_num', 'playlist_title', 'message_title']
content_fields = {x.name: (x.type, x.default) for x in fields(Content)}

if list([x.name for x in fields(Content)]) != list(content_sql_2_csv.keys()):
    raise Exception('Fields from "Content" don\'t match "content_sql_2_csv".')
if set([x.name for x in fields(Content) if isinstance(x.default, dataclasses._MISSING_TYPE)]) != set(
        content_required_fields):
    raise Exception('Fields from "Content" without defaults don\'t match "content_required_fields".')

# To map the other way:
# content_csv_2_sql = {v:k for k,v in content_sql_2_csv.items()}

is_first = True


@dataclass
class Recipient:
    country: str
    language: str
    region: str = ''
    district: str = ''
    communityname: str = ''
    groupname: str = ''
    agent: str = ''
    variant: str = ''
    listening_model: str = ''
    group_size: int = 0
    numhouseholds: int = 0
    numtbs: int = 0
    supportentity: str = ''
    agent_gender: str = None
    direct_beneficiaries: int = None
    direct_beneficiaries_additional: str = '{}'
    indirect_beneficiaries: int = None
    deployments: str = None
    recipientid: str = None
    affiliate: str = None
    partner: str = None
    component: str = None

    def __eq__(self, o: object) -> bool:
        global is_first
        if not isinstance(o, Recipient):
            return False
        op: Recipient = o
        eq: bool = self.country == op.country and \
                   self.language == op.language and \
                   self.region == op.region and \
                   self.district == op.district and \
                   self.communityname == op.communityname and \
                   self.groupname == op.groupname and \
                   self.agent == op.agent and \
                   self.variant == op.variant and \
                   self.listening_model == op.listening_model and \
                   self.group_size == op.group_size and \
                   self.numhouseholds == op.numhouseholds and \
                   self.numtbs == op.numtbs and \
                   self.supportentity == op.supportentity and \
                   self.agent_gender == op.agent_gender and \
                   self.direct_beneficiaries == op.direct_beneficiaries and \
                   self.direct_beneficiaries_additional == op.direct_beneficiaries_additional and \
                   self.indirect_beneficiaries == op.indirect_beneficiaries and \
                   self.deployments == op.deployments and \
                   self.recipientid == op.recipientid
        if not eq and is_first:
            print(f'recip unequal: {self} \n  other: {op}')
            is_first = False
        return eq


recipient_sql_2_csv = {
    'country': 'Country',
    'language': 'Language Code',
    'region': 'Region',
    'district': 'District',
    'communityname': 'Community',
    'groupname': 'Group Name',
    'agent': 'Agent',
    'variant': 'Variant',
    'listening_model': 'Listening Model',
    'group_size': 'Group Size',  # int
    'numhouseholds': '# HH',  # int
    'numtbs': '# TBs',  # int
    'supportentity': 'Support Entity',
    'agent_gender': 'Agent Gender',
    'direct_beneficiaries': 'Direct Beneficiaries',  # int
    'direct_beneficiaries_additional': 'Direct Beneficiaries Additional',  # json
    'indirect_beneficiaries': 'Indirect Beneficiaries',  # int
    'deployments': 'Deployments',  # json
    'recipientid': 'RecipientID',
    'affiliate': 'Affiliate',
    'partner': 'Partner',
    'component': 'Component'
}
recipient_required_fields: List[str] = ['country', 'language']
recipient_id_fields = ['country', 'region', 'district', 'communityname', 'groupname', 'agent', 'language', 'variant']
recipient_fields = {x.name: (x.type, x.default) for x in fields(Recipient)}
if list([x.name for x in fields(Recipient)]) != list(recipient_sql_2_csv.keys()):
    raise Exception('Fields from "Recipient" don\'t match "recipient_sql_2_csv".')
if set([x.name for x in fields(Recipient) if isinstance(x.default, dataclasses._MISSING_TYPE)]) != set(
        recipient_required_fields):
    raise Exception('Fields from "Recipient" without defaults don\'t match "recipient_required_fields".')


# recipient_csv_2_sql = {v:k for k,v in recipient_sql_2_csv.items()}

@dataclass
class Deployment:
    deploymentnumber: int
    startdate: date
    enddate: date
    deployment: str = ''


deployment_sql_2_csv = {
    'deploymentnumber': 'Deployment #',
    'startdate': 'Start Date',  # date
    'enddate': 'End Date',  # date
    'deployment': 'Deployment Name',
}
deployment_required_fields: List[str] = ['deploymentnumber', 'startdate', 'enddate']
deployment_id_fields: List[str] = ['deploymentnumber']
deployment_fields = {x.name: (x.type, x.default) for x in fields(Deployment)}
if list([x.name for x in fields(Deployment)]) != list(deployment_sql_2_csv.keys()):
    raise Exception('Fields from "Deployment" don\'t match "deployment_sql_2_csv".')
if set([x.name for x in fields(Deployment) if isinstance(x.default, dataclasses._MISSING_TYPE)]) != set(
        deployment_required_fields):
    raise Exception('Fields from "Deployment" without defaults don\'t match "deployment_required_fields".')


@dataclass
class General:
    program_id: str
    country: str = None
    # region: List[str] = field(default_factory=list)
    region: str = '[]'
    # languages: List[str] = field(default_factory=list)
    languages: str = '[]'
    deployments_count: int = 0
    deployments_length: str = 'one-quarter'
    deployments_first: date = field(default=datetime.now().date())
    # listening_models: List[str] = field(default_factory=list)
    listening_models: str = '[]'
    feedback_frequency: str = 'one-quarter'
    # sustainable_development_goals: List[str] = field(default_factory=list)
    sustainable_development_goals: str = '[]'
    direct_beneficiaries_map: str = \
        '{"male": "Number of Male", "female": "Number of Female", "youth": "Number of Youth"}'
    direct_beneficiaries_additional_map: str = '{}'
    affiliate: str = 'Amplio'
    partner: str = 'Amplio Partner'


general_sql_2_csv = {
    'program_id': 'Program ID',
    'country': 'Country',
    'region': 'Regions',
    'languages': 'Languages',
    'deployments_count': 'Deployments Count',
    'deployments_length': 'Deployments Length',
    'deployments_first': 'Deployments First',
    'listening_models': 'Listening Models',
    'feedback_frequency': 'Feedback Frequency',
    'sustainable_development_goals': 'Sustainable Development Goals',
    'direct_beneficiaries_map': 'Direct Beneficiaries Map',
    'direct_beneficiaries_additional_map': 'Direct Beneficiaries Additional Map',
    'affiliate': 'Affiliate',
    'partner': 'Partner',
}
general_required_fields: List[str] = ['program_id']
general_id_fields: List[str] = ['program_id']
general_fields = {x.name: (x.type, x.default) for x in fields(General)}
if list([x.name for x in fields(General)]) != list(general_sql_2_csv.keys()):
    raise Exception('Fields from "General" don\'t match "general_sql_2_csv".')
if set([x.name for x in fields(General) if isinstance(x.default, dataclasses._MISSING_TYPE)]) != set(
        general_required_fields):
    raise Exception('Fields from "General" without defaults don\'t match "general_required_fields".')

_int_columns = ['group_size', 'numhouseholds', 'numtbs', 'direct_beneficiaries', 'indirect_beneficiaries',
                'deployments_count']
_json_columns = ['direct_beneficiaries_additional', 'deployments', 'sustainable_development_goals',
                 'direct_beneficiaries_map', 'direct_beneficiaries_additional_map',
                 # 'languages', 'region',  'listening_models'
                 ]
date_columns = ['startdate', 'enddate', 'deployments_first']

MISSING_TYPE = general_fields['program_id'][1].__class__


def _normalize(k: str, v: Any, defaults: Dict[str, Any]) -> Any:
    if not v:
        if isinstance(defaults.get(k)[1], MISSING_TYPE):
            return None
        return defaults.get(k)[1]
    declared_type = defaults[k][0]
    if k in _int_columns or declared_type == int:
        return int(v or 0)
    elif k in _json_columns:
        # 'json' really means a string.
        if not isinstance(v, str):
            v = json.dumps(v)
        # A string that doesn't look like a json-encoded string?
        # if isinstance(v, str) and len(v)>2 and not (v[0] in "\"'" and v[0]==v[-1]):
        #     v = json.loads(v)
        return v or None
    elif k in date_columns or declared_type == date:
        return asdate(v)
    elif declared_type == str and not v:
        if defaults.get(k)[1] is None:
            v = None
        else:
            v = ''
    else:
        return str(v) if v else ''


def _normalize_record(record: Dict, record_def: Dict):
    return {k: _normalize(k, v, record_def) for k, v in record.items() if k in record_def}


class Program:
    def __init__(self, program_id):
        self.program_id = program_id
        self._deployments: List[Deployment] = []
        self._recipients: List[Recipient] = []
        self._content: List[Content] = []
        self._program: Optional[General] = None

    @property
    def program(self):
        return self._program

    @property
    def deployments(self):
        return self._deployments

    @property
    def recipients(self):
        return self._recipients

    @property
    def content(self):
        return self._content

    def compute_recipientid(self, recip: Recipient) -> str:
        # all the parts that must be different between recipients
        ids = (
            self.program_id, recip.country, recip.region, recip.district, recip.communityname, recip.agent,
            recip.groupname,
            recip.language, recip.variant)
        name = '-'.join([r if r else '' for r in ids])
        # Uncomment next line to add some salt ⃰ to the string.
        #   ⃰yes, it's not really salt, because we don't save it per recipient. It simply adds some randomness.
        # name += str(random.random())
        import hashlib
        str_hash = hashlib.sha1(name.encode('utf-8'))
        digx = str_hash.hexdigest()
        id16 = digx[:16]
        return id16

    @staticmethod
    def make_recipient(recipient: Dict[str, Any]) -> Recipient:
        normalized = _normalize_record(recipient, recipient_fields)
        return Recipient(**normalized)

    def make_deployment(self, deployment: Dict[str, Any]) -> Deployment:
        deploymentnumber = int(deployment['deploymentnumber'])
        startdate = asdate(deployment['startdate'])
        enddate = asdate(deployment['enddate'])
        depl = deployment.get('deployment')
        if not depl and startdate:
            depl = f"{self.program_id}-{startdate.strftime('%y')}-{deploymentnumber}"

        normalized = {'deploymentnumber': deploymentnumber, 'startdate': startdate, 'enddate': enddate,
                      'deployment': depl}
        return Deployment(**normalized)

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, Program):
            return False
        op: Program = o
        return self.program_id == op.program_id and \
               self._deployments == op._deployments and \
               self._content == op._content and \
               self._recipients == op._recipients and \
               self._program == op._program

    def add_general(self, program: Dict) -> None:
        normalized = _normalize_record(program, general_fields)
        self._program = General(**normalized)

    def add_content(self, content: Dict) -> None:
        normalized = _normalize_record(content, content_fields)
        # Sometimes goals and targets have the actual SDG text as well.
        sdg_goals = normalized.get('sdg_goals')
        if sdg_goals:
            parts = re.split(r'[^\d]', sdg_goals)
            if len(parts) > 1:
                sdg_goals = parts[0]
                normalized['sdg_goals'] = sdg_goals
        sdg_targets = normalized.get('sdg_targets')
        if sdg_targets:
            parts = re.split(r'[^\d.]', sdg_targets)
            if len(parts) > 1:
                sdg_targets = parts[0]
                normalized['sdg_targets'] = sdg_targets
        self._content.append(Content(**normalized))

    def add_recipient(self, recipient: Dict):
        self._recipients.append(self.make_recipient(recipient))

    def add_deployment(self, deployment: Dict):
        self._deployments.append(self.make_deployment(deployment))


@dataclass
class DbMessage:
    position: int
    title: str
    format: str = field(default=None)
    default_category_code: str = field(default=None)
    variant: str = field(default=None)
    sdg_goal_id: str = field(default=None)
    sdg_target: str = field(default=None)
    sdg_target_id: str = field(default=None)
    key_points: str = field(default='.')
    languages: str = field(default=None)
    audience: str = field(default=None)


@dataclass
class DbPlaylist:
    position: int
    title: str
    audience: str = field(default=None)
    db_messages: List[DbMessage] = field(default_factory=list)


@dataclass
class DbDeployment:
    deploymentnumber: int
    startdate: date = field(default=None)
    enddate: date = field(default=None)
    deployment: str = field(default=None)
    db_playlists: Dict[str, DbPlaylist] = field(default_factory=dict)


def flat_content_to_hierarchy(program_spec: Program, category_code_converter=lambda x: x) -> Dict[int, DbDeployment]:
    def unflatten_content(content: Content) -> None:
        """
        Add one row from a content.csv to the tree of messages for a program. Create the parent Playlist and Deployment
        objects as needed.
        :param: content The content row to be added.
        :return: None
        """
        row: Dict[str, Any] = dataclasses.asdict(content)
        # deployment_num,playlist_title,message_title,key_points,languagecode,variant,default_category,sdg_goals,sdg_targets
        deployment_num: int = int(content.deployment_num)
        # Retrieve or create Deployment.
        db_deployment = _db_deployments.setdefault(deployment_num, DbDeployment(deploymentnumber=deployment_num))
        # Retrieve or create Playlist.
        db_playlist: DbPlaylist = db_deployment.db_playlists.setdefault(content.playlist_title,
                                                                        DbPlaylist(
                                                                            len(db_deployment.db_playlists) + 1,
                                                                            content.playlist_title,
                                                                            content.audience))

        # Values for the database. Language is still special; get the list here, and create auxillary records later.
        language = row.get('languagecode', row.get('language', '[]'))
        # The database has both the bare sgd_target (like "3") and a sdg_target_id (like "4.3")
        sdg_goal_id: str = str(content.sdg_goals) or None
        sdg_target_id: str = str(content.sdg_targets) or None
        sdg_target: str = sdg_target_id.split('.')[1] if sdg_target_id else None

        # This would be better as a string column of the category name, because user may need to disambiguate.
        default_category_code = category_code_converter(content.default_category)

        # Create Message, and store it.
        db_message = DbMessage(position=len(db_playlist.db_messages) + 1, title=content.message_title,
                               format=content.format, default_category_code=default_category_code,
                               variant=content.variant,
                               sdg_goal_id=sdg_goal_id, sdg_target=sdg_target, sdg_target_id=sdg_target_id,
                               key_points=content.key_points,
                               languages=language, audience=content.audience)
        db_playlist.db_messages.append(db_message)

    _db_deployments: Dict[int, DbDeployment] = {}
    for d in program_spec.deployments:
        _db_deployments[d.deploymentnumber] = DbDeployment(d.deploymentnumber, d.startdate, d.enddate, d.deployment)

    for c in program_spec.content:
        unflatten_content(c)
    # ensure deploymentnumber order
    _db_deployments = {num: _db_deployments[num] for num in sorted(list(_db_deployments.keys()))}
    return _db_deployments


def progspec_to_json(program_spec: Program) -> List[Dict]:
    """
    From an opened exporter, return a list of deployments, each with a list of playlists, each with a list of messages.
    :param exporter: Opened reporter, with deployment data.
    :return: [ {deploymentnumber: n, playlists: [ {position: n, audience:'audience', messages: [ {...
    """
    deployments: List[Dict] = sorted([dataclasses.asdict(x) for x in
                           flat_content_to_hierarchy(program_spec).values()], key=lambda d:d.get('deploymentnumber'))
    for depl in deployments:
        playlists = sorted([x for x in depl['db_playlists'].values()], key=lambda pl:pl.get('position'))
        del depl['db_playlists']
        for pl in playlists:
            pl['messages'] = sorted(pl['db_messages'], key=lambda m:m.get('position'))
            del pl['db_messages']
        depl['playlists'] = playlists
    return deployments



def progspec_from_json(programid: str, progspec_data: Union[List,Dict]) -> Program:
    def content_from_hierarchy(deployment_num: int, pl: Dict) -> None:
        playlist_title = pl.get('title')
        if playlist_title:
            audience = pl.get('audience')
            messages = sorted([msg for msg in pl.get('messages') if msg.get('title')], key=lambda m:m.get('position'))
            for msg in messages:
                message_title = msg.get('title')
                key_points = msg.get('key_points')
                languagecode = msg.get('languages')
                variant = msg.get('variant')
                format = msg.get('format')
                default_category = msg.get('default_category_code')
                sdg_goals = msg.get('sdg_goal_id')
                sdg_targets = msg.get('sdg_target_id')
                audience = msg.get('audience', audience)
                result.add_content({
                    'deployment_num': deployment_num,
                    'deploymentnumber': deployment_num,
                    'playlist_title': playlist_title,
                    'message_title': message_title,
                    'key_points': key_points,
                    'languagecode': languagecode,
                    'languages': languagecode,
                    'variant': variant,
                    'format': format,
                    'audience': audience,
                    'default_category': default_category,
                    'sdg_goals': sdg_goals,
                    'sdg_targets': sdg_targets,
                })

    def deployment_from_hierarchy(depl: Dict) -> None:
        deploymentnumber = depl.get('deploymentnumber')
        result.add_deployment({k:v for k,v in depl.items() if k in deployment_fields.keys()})
        for pl in depl.get('playlists'):
            content_from_hierarchy(deploymentnumber, pl)

    deployments = None
    recipients = None
    if isinstance(progspec_data, dict):
        deployments = progspec_data.get('deployments')
        recipients = progspec_data.get('recipients')
    elif isinstance(progspec_data, list):
        item = progspec_data[0]
        if 'deploymentnumber' in item:
            deployments = progspec_data
        elif 'communityname' in item:
            recipients = progspec_data


    result: Program = Program(programid)
    if deployments:
        for depl in deployments:
            deployment_from_hierarchy(depl)
    return result
