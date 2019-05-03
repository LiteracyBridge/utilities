import hashlib
import os
from pathlib import Path

import programspec.programspec


class RecipientUtils:
    def __init__(self, spec: programspec, acmdir):
        self._spec = spec
        self._acmdir = Path(acmdir)

    @property
    def communities_dir(self) -> Path:
        return Path(self._acmdir, 'TB-Loaders', 'communities')

    @property
    def retired_communities_dir(self) -> Path:
        return Path(self._acmdir, 'TB-Loaders', 'archive', 'retired_communities')

    # Given a string, compute a secure hash, truncate to a friendlier length.
    @staticmethod
    def compute_id(string):
        str_hash = hashlib.sha1(string.encode('utf-8'))
        digx = str_hash.hexdigest()
        id16 = digx[:16]
        return id16

    def write_recipients_file(self, outdir):
        columns = ['recipientid', 'project', 'partner', 'communityname', 'groupname', 'affiliate',
                   'component', 'country', 'region', 'district', 'numhouseholds', 'numtbs',
                   'supportentity', 'model', 'language', 'coordinates', 'agent', 'latitude', 'longitude']
        computed_props = {'project': lambda:self._spec.project,
                          'coordinates': lambda:None,
                          'latitude': lambda:None,
                          'longitude': lambda:None}
        numeric_props = {'numhouseholds', 'numtbs'}
        coordinate_props = {'coordinates', 'latitude', 'longitude'}
        property_map = {'communityname':'community', 'groupname':'group_name',
                        'numhouseholds':'num_hh', 'numtbs':'num_tbs',
                        'supportentity':'support_entity'}

        # Properly retrieve and quote a value for the recipients.csv file
        def val(col, recip):
            #v = ''
            if col in property_map:
                v = recip.properties.get(property_map[col])
            elif col in computed_props:
                v = computed_props[col]()
            else:
                v = recip.properties.get(col)

            if v is None or v == '':
                if col in numeric_props:
                    v = '0'
                elif col in coordinate_props:
                    v=''
                else:
                    v = '""'
            elif type(v) != str:
                v = str(v)

            return v

        recipients = Path(outdir, 'recipients.csv')
        with recipients.open(mode='w', newline='\n') as r:
            print(','.join(columns), file=r)
            for component in self._spec.components.values():
                for recipient in component.recipients:
                  if recipient.recipientid:
                    props = [val(c, recipient) for c in columns]
                    line = ','.join(props)
                    print(line, file=r)

    def write_recipients_map_file(self, outdir):
        recipients_map = Path(outdir, 'recipients_map.csv')
        with recipients_map.open(mode='w', newline='\n') as rm:
            print('project,directory,recipientid', file=rm)
            for component in self._spec.components.values():
                for recipient in component.recipients:
                    if recipient.recipientid and recipient.directory_name:
                        # In the recipients_map, the directory name is always upper-cased. This is because the
                        # directory name is taken from, and used for "community" and "village" in the database, and
                        # comes from the "community" and "village" on the Talking Book, where it is always upper cased.
                        aliases = set()
                        aliases.add(recipient.directory_name.upper())
                        recipient_id_file = self.read_existing_recipient_id_file(recipient.directory_name)
                        if recipient_id_file and 'alias' in recipient_id_file:
                            aliases = aliases | set([a.upper() for a in recipient_id_file['alias']])

                        for alias in aliases:
                            line = [self._spec.project, alias.upper(), recipient.recipientid]
                            print(','.join(line), file=rm)

        project = self._spec.project
        written = 0
        talkingbook_map = Path(outdir, 'talkingbook_map.csv')
        with talkingbook_map.open(mode='w', newline='\n') as rm:
            print('talkingbookid,recipientid,community,project', file=rm)
            for component in self._spec.components.values():
                for recipient in component.recipients:
                    if recipient.recipientid and recipient.directory_name and recipient.talkingbookid:
                        # In the talkingbook_map, the directory name is always upper-cased. This is because the
                        # directory name is taken from, and used for "community" and "village" in the database, and
                        # comes from the "community" and "village" on the Talking Book, where it is always upper cased.
                        community = recipient.directory_name.upper()
                        line = [recipient.talkingbookid, recipient.recipientid, community.upper(), project]
                        print(','.join(line), file=rm)
                        written += 1
        if written==0:
            Path(outdir, 'talkingbook_map.csv').unlink()


    # If there is an existing recipient.id file in the directory, read and parse it.
    # The file can have arbitrary attributes, where an attribute is a line with
    #    key = value
    # with leading and trailing spaces ignored for key and value. '#' introduces
    # a comment, and blank lines are ignored.
    # Additionally, the attributes can include an arbitrary number of 'alias'
    # attributes, because a community/group might have more than one.
    #
    # The results are returned as a (possibly empty) dictionary of properties.
    # If there is at least one alias property, all are returned as a list value.
    #
    # If there is no recipient.id file, the returned value is None.
    def read_existing_recipient_id_file(self, directory):
        existing_id = None
        id_path = Path(self.communities_dir, directory, 'recipient.id')
        if os.path.exists(id_path) and os.path.isfile(id_path):
            existing_id = {}
            id_file = open(id_path, 'r')
            for line in id_file:
                line = str(line).strip()
                if line.startswith('#'):
                    continue
                (k, v) = line.split('=', 1)
                k = k.strip().lower()
                v = v.strip()
                if k == 'alias':
                    if 'alias' not in existing_id:
                        existing_id['alias'] = []
                    existing_id['alias'].append(v)
                else:
                    existing_id[k] = v
            id_file.close()

        return existing_id

    # Given the directory for some recipient, compute the recipientid.
    def compute_recipientid(self, dir_name):
        return self.compute_id(str(self.communities_dir) + ' ' + dir_name)

    # Given the directory for some recipient, create a recipient.id file. Compute the recipientid.
    def create_recipient_id_file(self, dir_name, recipientid=None):
        if recipientid is None or len(recipientid)==0:
            recipientid = self.compute_id(str(self.communities_dir) + ' ' + dir_name)
        path = Path(self.communities_dir, dir_name)
        recipientid_path = path.joinpath('recipient.id')
        with recipientid_path.open(mode='w', newline='\n') as f:
            print('project={}'.format(self._spec.project.upper()), file=f)
            print('recipientid={}'.format(recipientid), file=f)
            print('alias={}'.format(dir_name.upper()), file=f)
        return recipientid

    # Given a recipient, create the TB-Loaders/communities/* directory for that recipient. Include languages,
    # system (.grp), and recipient.id file.
    def create_directory_for_recipient_in_path(self, recipient, path: Path):
        # Make the directory structure for the community/group
        path.mkdir(parents=True, exist_ok=True)
        lang = recipient.language.lower()
        langpath = path.joinpath('languages', lang)
        langpath.mkdir(parents=True, exist_ok=True)
        syspath = path.joinpath('system')
        syspath.mkdir(exist_ok=True)
        lang_grp = syspath.joinpath(lang + '.grp')
        lang_grp.touch(exist_ok=True)
        # Compute the recipient id if needed, and create the recipient id file
        recipientid = recipient.recipientid
        recipientid = self.create_recipient_id_file(path.name, recipientid)

        return recipientid
