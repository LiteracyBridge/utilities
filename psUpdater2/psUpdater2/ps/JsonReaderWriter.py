import json
from typing import Union, Dict, Tuple, Optional, List

from ps.spec import ProgramSpec, Deployment, Playlist


class _JsonWriter:
    def __init__(self, program_spec: ProgramSpec):
        self._program_spec = program_spec

    @staticmethod
    def _pl_as_json(pl: Playlist) -> Dict:
        result = pl.json_object
        result['messages'] = [msg.json_object for msg in pl.messages]
        return result

    def _depl_as_json(self, depl: Deployment) -> Dict:
        result = depl.json_object
        result['playlists'] = [self._pl_as_json(pl) for pl in depl.playlists]
        return result

    def do_export_to_json(self, json_args=None, to_string: bool=True) -> str:
        if json_args is None:
            json_args = {}
        result = {'program_id': self._program_spec.program_id,
                  'deployments': [self._depl_as_json(depl) for depl in self._program_spec.deployments],
                  'recipients': [recip.json_object for recip in self._program_spec.recipients]}
        if self._program_spec.general:
            result['general'] = self._program_spec.general.json_object
        return json.dumps(result, **json_args) if to_string else result


class _JsonReader:
    def __init__(self, program_id: str, str_or_dict: Union[str, Dict]):
        self._program_id: str = program_id
        self._str_or_dict: Union[str, Dict] = str_or_dict
        self._program_spec: Optional[ProgramSpec] = None

    def _import_deployment(self, depl: Dict):
        deployment: Deployment = self._program_spec.add_deployment(depl)
        for pl in depl['playlists']:
            playlist: Playlist = deployment.add_playlist(pl)
            for msg in pl['messages']:
                playlist.add_message(msg)

    def _import_recipient(self, recip: Dict):
        self._program_spec.add_recipient(recip)

    def do_import_from_json(self, json_args=None) -> Tuple[Union[bool, Optional[ProgramSpec]], List[str]]:
        if isinstance(self._str_or_dict, str):
            try:
                if json_args is None:
                    json_args = {}
                progspec_data = json.loads(self._str_or_dict, **json_args)
            except Exception as ex:
                return False, [str(ex)]
        else:
            progspec_data = self._str_or_dict

        # Examine the input data. It could be a dict {deployments: [depl1, depl2, ...], recipients: [recip1, recip2,...]}
        # or it could be a list [depl1, depl2, ...] or [recip1, recip2, ...].
        deployments: Optional[List[Dict[str, str]]] = None
        recipients: Optional[List[Dict[str, str]]] = None
        general: Optional[Dict[str, str]] = None
        if isinstance(progspec_data, dict):
            deployments = progspec_data.get('deployments')
            recipients = progspec_data.get('recipients')
            general = progspec_data.get('general')
        elif isinstance(progspec_data, list):
            item = progspec_data[0]
            if 'deploymentnumber' in item:
                deployments = progspec_data
            elif 'communityname' in item:
                recipients = progspec_data

        self._program_spec = ProgramSpec(self._program_id)
        if general:
            self._program_spec.add_general(general)
        if deployments:
            for depl in deployments:
                self._import_deployment(depl)
        if recipients:
            for recip in recipients:
                self._import_recipient(recip)
        return self._program_spec, []


def read_from_json(programid: str, str_or_dict: Union[str, Dict], json_args=None) -> \
        Tuple[Union[bool, Optional[ProgramSpec]], List[str]]:
    return _JsonReader(programid, str_or_dict).do_import_from_json(json_args=json_args)


def write_to_json(program_spec: ProgramSpec, json_args=None, to_string:bool=True) -> str:
    return _JsonWriter(program_spec).do_export_to_json(json_args=json_args,to_string=to_string)
