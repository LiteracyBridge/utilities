import os
from pathlib import Path

from programspec import programspec
from programspec.recipient_utils import RecipientUtils


class RecipientFileUtils(RecipientUtils):
    def __init__(self, spec: programspec, acmdir):
        super().__init__(spec)
        self._acmdir = Path(acmdir)


    @property
    def communities_dir(self) -> Path:
        return Path(self._acmdir, 'TB-Loaders', 'communities')

    @property
    def retired_communities_dir(self) -> Path:
        return Path(self._acmdir, 'TB-Loaders', 'archive', 'retired_communities')

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


    def get_aliases_for_recipient(self, recipient):
        # In the recipients_map, the directory name is always upper-cased. This is because the
        # directory name is taken from, and used for "community" and "village" in the database, and
        # comes from the "community" and "village" on the Talking Book, where it is always upper cased.
        aliases = set(super().get_aliases_for_recipient(recipient))
        recipient_id_file = self.read_existing_recipient_id_file(recipient.directory_name)
        if recipient_id_file and 'alias' in recipient_id_file:
            aliases = aliases | set([a.upper() for a in recipient_id_file['alias']])
        return aliases