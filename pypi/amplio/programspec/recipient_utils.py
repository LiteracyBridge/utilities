import hashlib
import os
import random
import re
from pathlib import Path

from . import programspec

_file_substitutions = re.compile('[^\w-]+')

class RecipientUtils:
    def __init__(self, spec: programspec):
        self._spec = spec

    @property
    def communities_dir(self) -> Path:
        return Path('Dropbox (Amplio)', self._spec.project, 'TB-Loaders', 'communities')

    @property
    def retired_communities_dir(self) -> Path:
        return Path('Dropbox (Amplio)', self._spec.project, 'TB-Loaders', 'archive', 'retired_communities')

    # Given a string, compute a secure hash, truncate to a friendlier length.
    @staticmethod
    def compute_id(string):
        # Uncomment next line to add some salt ⃰ to the string.
        #   ⃰yes, it's not really salt, because we don't save it per recipient. It simply adds some randomness.
        #string += str(random.random())
        str_hash = hashlib.sha1(string.encode('utf-8'))
        digx = str_hash.hexdigest()
        id16 = digx[:16]
        return id16


    def compute_directory(self, recipient: programspec.Recipient):
        # community{-group}{-community_worker}
        r = recipient.id_tuple
        name = r[0]
        name += ('-' + r[1]) if r[1] else ''
        name += ('-' + r[2]) if r[2] else ''
        return _file_substitutions.sub('_', name)

    # Given the directory for some recipient, compute the recipientid.
    def compute_recipientid(self, dir_name):
        return self.compute_id(str(self.communities_dir) + ' ' + dir_name)

    def get_aliases_for_recipient(self, recipient):
        # In the recipients_map, the directory name is always upper-cased. This is because the
        # directory name is taken from, and used for "community" and "village" in the database, and
        # comes from the "community" and "village" on the Talking Book, where it is always upper cased.
        return [recipient.directory_name.upper()]
