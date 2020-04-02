import copy
import os
import re
import time
from pathlib import Path

from fuzzywuzzy import fuzz

from amplio.programspec import errors, programspec
from amplio.programspec.programspec_constants import DIRECTORIES, XDIRECTORIES, XLSX, RECIPIENTS
from recipient_file_utils import RecipientFileUtils

# In this module is it very useful to refer to recipients by (community, group, agent). For that purpose,
# we use the term "community_group".
#
# Please be consistent here.

# To find non-word, non-hyphen characters (replace with _ in filenames). 

_file_substitutions = re.compile('[^\w-]+')


def _recip_key(comm_or_recip, group_or_none: None, se_or_none: None):
    pass


# Cannonicalize strings to upper case before fuzzy matching.
def ufuzz(left, right):
    return fuzz.ratio(left.upper(), right.upper())

def format_cga(cga):
    '''  community  /group (if group) /agent (if agent) '''
    s = cga[0]
    s = s + ('/' + cga[1] if cga[1] else '')
    s = s + ('/' + cga[2] if cga[2] else '')
    return s


class FuzzyDirectoryMatcher:
    def __init__(self, reconciler):
        self._reconciler = reconciler
        # formats a recip (community, group, agent) as a string
        self._fmt = reconciler._fmt

    # Sets values used outside of the walk function.
    def _prepare(self):
        self.dirs = sorted([d for d in self._reconciler._unmatched_dirs], key=lambda d: d.upper())
        self.recips = sorted([r for r in self._reconciler._unmatched_recipients], key=lambda r: self._fmt(r).upper())
        self.dir_width = max([0] + [len(d) for d in self.dirs]) + 5
        self.recip_width = max([0] + [len(self._fmt(r)) for r in self.recips]) + 5

    def _walk(self, matched, advanced_dirs, advanced_recips):
        # convenience references
        dirs = self.dirs
        recips = self.recips
        # Does a fuzzy match on the d'th dir and r'th recip, -1 if no such element
        fz = lambda d, r: ufuzz(dirs[d].replace(' ', ''), self._fmt(recips[r]).replace(' ', '')) if d < len(
            dirs) and r < len(recips) else -1

        while len(dirs) and len(recips):
            # Compare current elements, plus 1 & 2 lookaheads. The comparisons of 2,1 and 1,2 don't seem to
            # affect the outcome, but they do affect how we get there.
            score = [fz(0, 0), fz(1, 0), fz(0, 1), fz(2, 0), fz(0, 2)]  # , fz(2,1), fz(1,2)]
            max_score = max(score)
            # Is any of them good enough?
            if max_score > self._reconciler._threshold:
                score_ix = score.index(max_score)  # returns first one found, which is what we want
                # prefer the tip, if it is equal or better. Otherwise take one from right or left, then do the
                # comparisons again.
                if score_ix == 0:
                    matched(recips.pop(0), dirs.pop(0), max_score)
                elif score_ix % 2 == 1:
                    # The next or second left matches the current right better. Skip one left.
                    advanced_dirs(dirs.pop(0))
                else:
                    # The next or second right matches the current left better. Skip one right.
                    advanced_recips(recips.pop(0))
            else:
                # Just advance based on lexical comparison
                if dirs[0].upper() < self._fmt(recips[0]).upper():
                    advanced_dirs(dirs.pop(0))
                else:
                    advanced_recips(recips.pop(0))
        while len(dirs):
            advanced_dirs(dirs.pop(0))
        while len(recips):
            advanced_recips(recips.pop(0))

    def make_matches(self):
        self._prepare()
        matched = lambda r, d, s: matches.append((r, d, s))
        advanced = lambda x: None  # doesn't do anything, so use same one for left and right

        num_matched = 0
        matches = []
        self._walk(matched, advanced, advanced)
        while len(matches) > 0:
            num_matched = num_matched + len(matches)
            for recip, directory, score in matches:
                self._reconciler.remove_matched(recip, directory, score)
            matches = []
            self._prepare()
            self._walk(matched, advanced, advanced)
        return num_matched

    def print_unmatched(self):
        self._prepare()
        # convenience shortcuts
        fmt = lambda r: '{}/{}'.format(r[0], r[1])
        matched = lambda d, r, s: print(
            '  {:{lw}} {:{rw}} ({}% match)'.format(format_cga(r), d, s, lw=lw, rw=rw))
        advanced_dirs = lambda d: print('  {:{lw}} {:{rw}}'.format('', d, lw=lw, rw=rw))
        advanced_recips = lambda r: print('  {:{lw}} {:{rw}}'.format(format_cga(r), '', lw=lw, rw=rw))

        cghead = '{} Community/Groups'.format(len(self.recips))
        dirhead = '{} Directories'.format(len(self.dirs))
        lw = max(self.recip_width, len(cghead))
        rw = max(self.dir_width, len(dirhead))

        if len(self.recips) > 0 or len(self.dirs) > 0:
            print('Unmatched directories and communities')
            print('  {:{lw}} {:{rw}}'.format(cghead, dirhead, lw=lw, rw=rw))
            print('  {:-^{lw}} {:-^{rw}}'.format('', '', rw=rw, lw=lw))

            self._walk(matched, advanced_dirs, advanced_recips)


class Reconciler:
    def __init__(self, acmdir, spec: programspec, strategy: int, update: set, outdir):
        def _strategy0(r):
            return _file_substitutions.sub('_', r[1])

        def _strategy3(r):
            # community {-group_or_community_worker}
            grp = '-' + r[1] if r[1] else ''
            return _file_substitutions.sub('_', r[0] + grp)

        def _strategy4(r):
            # community {-group_or_community_worker}
            name = r[0]
            name += ('-'+r[1]) if r[1] else ''
            name += ('-'+r[2]) if r[2] else ''
            return _file_substitutions.sub('_', name)

        self._acmdir = acmdir
        self._spec = spec
        self._update = update
        self._outdir = outdir

        self._recipient_utils = RecipientFileUtils(spec, acmdir)

        self._unmatched_dirs = set()
        # set of {(community, group, agent)}. Initialized from program spec, ideally there will be a directory matching each one.
        self._unmatched_recipients = set()

        # dictionary of {directory : recipientid}
        self._recipientid_by_directory = {}
        # dictionary of {recipientid : directory}
        self._directory_by_recipientid = {}

        # set of {DIRECTORY} found in communities dir, upper cased
        self._upper_case_directory_names = set()

        # list of [directory] without a recipientid file
        self._directories_without_recipientid = []

        # dictionary of {recipientid: {recipient.id file contents} }
        self._recipient_id_files_by_directory = {}

        # dictionary of {recipientid : Recipient}, from the Program Specification
        self._recipients_by_recipientid_from_spec = {}
        # dictionary of {directory : Recipient}, from the Program Specification
        self._recipients_by_directory_from_spec = {}
        # dictionary of {(community, group, agent) : Recipient }, from the Program Specification
        self._recipients_by_community_group_from_spec = {}
        # dictionary of {community : [Recipient,...]} from the Program Specification
        self._recipients_by_community_from_spec = {}

        # list of [(recipient, directory, score)], where recipient is (community, group, agent)
        self._matches = []
        # set of communities with at least one matched directory
        self._matched_communities = set()

        # how good does a fuzzy match need to be for use to consider it a match?
        self._threshold_value = None
        self._joiner = '-'
        # How the community and group names were *usually* joined into directory names.
        self._strategy = strategy
        if strategy == 4:
            self._fmt = _strategy4
        elif strategy == 3:
            self._fmt = _strategy3
        elif strategy == 2:
            self._fmt = lambda r: '{}{}{}'.format(r[1] if r[1] else '', '-' if r[1] else '', r[0])
        elif strategy == 1:
            self._fmt = lambda r: '{}{}{}'.format(r[0], ' ' if r[1] else '', r[1] if r[1] else '')
        else:
            self._fmt = _strategy0

    @property
    def _threshold(self):
        if self._threshold_value is None:
            self._threshold_value = 85
            print('Using {}% threshold for fuzzy matching.'.format(self._threshold_value))

        return self._threshold_value

    @property
    def communities_dir(self):
        return self._recipient_utils.communities_dir #self._acmdir + '/TB-Loaders/communities'

    @property
    def retired_communities_dir(self):
        return self._recipient_utils.retired_communities_dir #self._acmdir + '/TB-Loaders/archive/retired_communities'

    # Build a tuple for the recipient's name. Read as "X in Community", where X is group or support entity (CHW, etc)
    def _recip_tuple(self, recip: programspec.Recipient):
        return (recip.community, recip.group_name, recip.agent)
        # keys = [recip.community]
        # if recip.model.lower() == 'hhr':
        #     keys.append(None)
        # elif recip.model.lower() == 'group':
        #     keys.append(recip.group_name)
        # else:
        #     # community worker model?
        #     # If the _recipients_by_community_from_spec hasn't yet been initialized, this will give an empty list
        #     # of recipients. We'll "all(...)" will be true because none are false.
        #     community_recipients = self._recipients_by_community_from_spec.get(recip.community, [])
        #     agent = None
        #     # Is there a "group_name", and is it unique? If so, use that for agent.
        #     # Otherwise try with support_entity.
        #     # Otherwise concat group_name, support_entity, and model.
        #     if recip.group_name and all([r == recip or recip.group_name != r.group_name for r in community_recipients]):
        #         keys.append(recip.group_name)
        #     elif recip.support_entity and all(
        #             [r == recip or recip.support_entity != r.support_entity for r in community_recipients]):
        #         keys.append(recip.support_entity)
        #     else:
        #         keys.append(recip.model)
        #         if recip.group_name:
        #             keys.append(recip.group_name)
        #         if recip.support_entity:
        #             keys.append(recip.support_entity)
        # return tuple(keys)

    def _recip_key(self, recip: programspec.Recipient):
        return self._recip_tuple(recip)
        # tup = self._recip_tuple(recip)
        # a = tup[0]
        # b = tup[1]
        # for k in tup[2:]:
        #     b += '_' + k
        # return a, b

        # if recip.model.lower() == 'hhr':
        #     return (recip.community, None)
        # elif recip.model.lower() == 'group':
        #     return (recip.community, recip.group_name)
        # else:
        #     # community worker model?
        #     return (recip.community, recip.support_entity)

    def _recip_name(self, recip: programspec.Recipient):
        tup = self._recip_tuple(recip)
        name = tup[0]
        for k in tup[1:]:
            if k is not None:
                name += '/' + k
        return name
        #
        # if recip.model.lower() == 'hhr':
        #     return recip.community
        # elif recip.model.lower() == 'group':
        #     return '{}/{}'.format(recip.community, recip.group_name)
        # else:
        #     # community worker model?
        #     # If the _recipients_by_community_from_spec hasn't yet been initialized, this will give an empty list
        #     # of recipients. We'll "all(...)" will be true because none are false.
        #     community_recipients = self._recipients_by_community_from_spec.get(recip.community, [])
        #     agent = None
        #     # Is there a "group_name", and is it unique? If so, use that for agent.
        #     # Otherwise try with support_entity.
        #     # Otherwise concat group_name, support_entity, and model.
        #     if recip.group_name and all([r==recip or recip.group_name!=r.group_name for r in community_recipients]):
        #         agent = recip.group_name
        #     elif recip.support_entity and all([r==recip or recip.support_entity!=r.support_entity for r in community_recipients]):
        #         agent = recip.support_entity
        #     else:
        #         agent = recip.model
        #         if recip.group_name:
        #             agent += ' ' + recip.group_name
        #         if recip.support_entity:
        #             agent += ' ' + recip.support_entity
        #
        #     return '{}/{}'.format(recip.community, agent)

    # Move the unused directories out of the communities directory.
    def _remove_unused_directories(self):
        path = Path(self.retired_communities_dir)
        path.mkdir(parents=True, exist_ok=True)
        for unmatched_dir in self._unmatched_dirs:
            unmatched_path = Path(self.communities_dir).joinpath(unmatched_dir)
            target_dir = unmatched_dir
            increment = 0
            target_path = path.joinpath(target_dir)
            while target_path.exists():
                increment += 1
                target_dir = '{}-{}'.format(unmatched_dir, increment)
                target_path = path.joinpath(target_dir)
            print('Moving {} to {}'.format(unmatched_dir, target_dir))
            unmatched_path.rename(target_path)

    def _normalize_pathname(self, pathname: str):
        # Replace whitespace with underscores, eliminate some problematic characters.
        pathname = re.sub(r'\s', '_', pathname)
        pathname = re.sub(r'[\\\'"&*?]+', '', pathname)
        print(pathname)
        return pathname

    def _create_recipientid_for_recipient(self, recipient: programspec.Recipient):
        if recipient.recipient is not None:
            return
        directory = self._fmt(self._recip_key(recipient))  # (recipient.community, recipient.group_name))
        directory = self._normalize_pathname(directory)
        recipientid = self._recipient_utils.compute_recipientid(directory)
        if recipientid in self._recipients_by_recipientid_from_spec:
            errors.err(errors.recipientid_would_collide,
                       {'recipientid': recipientid,
                        'community': '{}'.format(self._recip_name(recipient)),
                        'community2': '{}'.format(self._recip_name(self._recipients_by_recipientid_from_spec[recipientid]))})

        recipient.recipientid = recipientid

    def _create_missing_recipientids(self):
        # For every component, for every recipient...
        for component in self._spec.components.values():
            for recipient in component.recipients:
                if recipient.recipientid is None:
                    self._create_recipientid_for_recipient(recipient)

    # Given a recipient, create the TB-Loaders/communities/* directory for that recipient. Include languages,
    # system (.grp), and recipient.id file.
    def _create_directory_for_recipient(self, recipient: programspec.Recipient):
        if recipient.directory_name is not None:
            return

        #RECIP_NAME
        # Compute the directory name and full path to the community/group directory
        directory = self._fmt(self._recip_key(recipient))  # (recipient.community, recipient.group_name))
        directory = self._normalize_pathname(directory)
        path = Path(self.communities_dir, directory)
        if path.exists() or directory.upper() in self._upper_case_directory_names:
            errors.err(errors.community_directory_exists,
                       {'directory': directory, 'community': '{}'.format(self._recip_name(recipient))})
            return
        # Make the directory structure & create recipient.id for the community/group
        existing = 'existing ' if recipient.recipientid else ''
        recipientid = self._recipient_utils.create_directory_for_recipient_in_path(recipient, path)

        #RECIP_NAME
        print('Created directory "{}" for {} with {}recipientid {}'.format(directory, self._recip_name(recipient),
                                                                         existing, recipientid))
        # Update the directories of what's in the TB-Loaders/communities directory
        self._recipientid_by_directory[directory] = recipientid
        self._directory_by_recipientid[recipientid] = directory
        self._upper_case_directory_names.add(directory.upper())
        if directory in self._directories_without_recipientid:
            self._directories_without_recipientid.remove(directory)
        # Update the recipient itself.
        recipient.recipientid = recipientid
        recipient.directory_name = directory

    def _create_directories_for_recipients(self):
        for community, group, agent in self._unmatched_recipients:
            recipient = self._recipients_by_community_group_from_spec[(community, group, agent)]
            self._create_directory_for_recipient(recipient)

    # Checks whether any of the directories that we would create would cause collisions.
    def _directories_to_create_are_ok(self):
        ok = True
        mark = errors.get_mark()
        directories_to_create = {}
        directory_collisions = {}
        for community, group, agent in self._unmatched_recipients:
            recipient = self._recipients_by_community_group_from_spec[(community, group, agent)]
            if recipient.directory_name is not None:  # already has a directory
                continue
            # Compute the directory name, and check for collisions with existing or to-be-created directories
            directory = self._fmt(
                self._recip_key(recipient)).upper()  # (recipient.community, recipient.group_name)).upper()
            if directory in self._upper_case_directory_names:
                errors.err(errors.community_directory_exists, {'directory': directory,
                                                               'community': '{}'.format(
                                                                   self._recip_name(recipient))})
                ok = False
            elif directory in directories_to_create:
                if directory not in directory_collisions:
                    directory_collisions[directory] = [directories_to_create[directory]]
                directory_collisions[directory].append(recipient)
                ok = False
            else:
                directories_to_create[directory] = recipient
        if len(directory_collisions) > 0:
            for directory, recipients in directory_collisions.items():
                communities = '", "'.join([self._recip_name(r) for r in recipients])
                errors.err(errors.community_directory_would_collide, {'directory': directory,
                                                                      'communities': communities})
        if not ok:
            print()
            errors.print_errors(mark=mark)
            print('No directories created.')
        return ok

    # For recipients without a recipientid file, create the file.
    def _create_missing_recipient_id_files(self):
        for community_group, directory, _ in self._matches:
            if directory in self._directories_without_recipientid:
                recipient = self._recipients_by_community_group_from_spec[community_group]
                recipientid = recipient.recipientid
                existing = 'existing ' if recipient.recipientid else ''
                recipientid = self._recipient_utils.create_recipient_id_file(directory, recipientid)
                print('Created recipient.id in "{}" for {}/{} with {}recipientid {}'.format(directory,
                                                                                          recipient.community,
                                                                                          recipient.group_name,
                                                                                          existing, recipientid))
                # Update the directories of what's in the TB-Loaders/communities directory
                self._recipientid_by_directory[directory] = recipientid
                self._directory_by_recipientid[recipientid] = directory
                self._directories_without_recipientid.remove(directory)
                # Update the recipient itself (writing a new xlsx, or not, is handled later)
                recipient.recipientid = recipientid
                recipient.directory_name = directory

    # For any newly matched recipents, record the newly found recipientid
    def _update_recipients_with_new_matches(self):
        for community_group, directory, _ in self._matches:
            recipient = self._recipients_by_community_group_from_spec[community_group]
            updates = {}
            if not recipient.recipientid and directory in self._recipientid_by_directory:
                recipientid = self._recipientid_by_directory[directory]
                recipient.recipientid = recipientid
                updates['recipientid'] = recipientid
            if not recipient.directory_name:
                recipient.directory_name = directory
                updates['directory'] = directory
            if len(updates) > 0:
                ups = ', '.join(['{}={}'.format(k, v) for k, v in updates.items()])
                print('Updated {}/{} with {}'.format(recipient.community, recipient.group_name, ups))

    # Enumerate "community" directories in the given TB-Loaders/communities directory.
    # Look for a recipient.id file in each one. Track what is and is not found.
    def read_existing_recipient_id_files(self):
        directories = os.listdir(self.communities_dir)
        recipient_id_files = {}
        for directory in directories:
            if not Path(self.communities_dir, directory).is_dir():
                continue
            recipient_id_file = self._recipient_utils.read_existing_recipient_id_file(directory)
            if recipient_id_file and 'recipientid' in recipient_id_file:
                recipientid = recipient_id_file['recipientid']
                if recipientid in self._directory_by_recipientid:
                    # Fatal error
                    other = self._directory_by_recipientid[recipientid]
                    print('Duplicate recipientid "{}" found in directory "{}" and "{}".'
                          .format(recipientid, directory, other))
                self._recipientid_by_directory[directory] = recipientid
                self._directory_by_recipientid[recipientid] = directory
            else:
                self._directories_without_recipientid.append(directory)
            self._upper_case_directory_names.add(directory.upper())
            recipient_id_files[directory] = recipient_id_file
        self._recipient_id_files_by_directory = recipient_id_files
        return recipient_id_files

    # Get the list of community directories. Each contains the language, greeting, and group for
    # some community, and may contain a recipient.id file.
    #
    # Builds a member dictionary of {directory name : recipient.id file}
    def get_community_dirs(self):
        recipient_id_files = self.read_existing_recipient_id_files()
        return recipient_id_files

    # Gets all of the recipients into a member dictionary of {(community, group_name, se) : Recipient}
    def get_recipients_from_spec(self):
        community_groups = {}
        communities = {}

        # For every community, a list of all the recipients
        for component in self._spec.components.values():
            for recipient in component.recipients:
                if recipient.community in communities:
                    communities[recipient.community].append(recipient)
                else:
                    communities[recipient.community] = [recipient]
        self._recipients_by_community_from_spec = communities

        for component in self._spec.components.values():
            for recipient in component.recipients:
                key = self._recip_key(recipient)
                if key in community_groups:
                    print('Unexpected duplicate community/group: "{}" in "{}" and "{}"'
                          .format(self._recip_name(recipient), recipient.component,
                                  community_groups[key].component))
                else:
                    community_groups[key] = recipient
                # If there is a recipient id, make sure it isn't a duplicate.
                if recipient.recipientid:
                    if recipient.recipientid in self._recipients_by_recipientid_from_spec:
                        # fatal error
                        other = self._recipients_by_recipientid_from_spec[recipient.recipientid]
                        recip_name = self._recip_name(recipient)
                        other_name = self._recip_name(other)
                        print('Duplicate recipientid "{}" assigned to "{}" and "{}".'
                              .format(recipient.recipientid, recip_name, other_name))
                    self._recipients_by_recipientid_from_spec[recipient.recipientid] = recipient
                # If there is a directory, make sure it isn't a duplicate.
                if recipient.directory_name:
                    if recipient.directory_name in self._recipients_by_directory_from_spec:
                        # fatal error
                        other = self._recipients_by_directory_from_spec[recipient.directory_name]
                        recip_name = self._recip_name(recipient)
                        other_name = self._recip_name(other)
                        print('Duplicate directory name "{}" assigned to "{}" and "{}".'
                              .format(recipient.directory_name, recip_name, other_name))
                    self._recipients_by_directory_from_spec[recipient.directory_name] = recipient
        self._recipients_by_community_group_from_spec = community_groups
        return community_groups

##
## group -> (group, agent)
##
    # For each community, a list of unmatched groups. Built from unmatched_recipients.
    # { community : [ (group, agent), ...]}
    def unmatched_ga_s_by_community(self):
        unmatched_ga_s = {}
        # List of unique community names, with a list of groups for each
        for community, group, agent in self._unmatched_recipients:
            if community is None:
                print("well, that's unexpected")
            if community in unmatched_ga_s:
                unmatched_ga_s[community].append((group, agent))
            else:
                unmatched_ga_s[community] = [(group, agent)]
        return unmatched_ga_s

    # Given a recipient and a directory, and optional score, record the match.
    # Remove the recipient and directory from unmatched lists.
    def remove_matched(self, community_group, directory, score=100):
        # TODO: if there is already a recipient id, be sure it is in the Recipient
        # TODO: be sure the directory name is in the Recipient
        self._unmatched_dirs.remove(directory)
        self._unmatched_recipients.remove(community_group)
        self._matches.append((community_group, directory, score))
        self._matched_communities.add(community_group[0])

    # Print the matches that were found.
    def print_matched(self):
        print('Matched {} community/groups  -->  directories:'.format(len(self._matches)))
        if len(self._matches) == 0:
            print('  No matches.')
            return
        if len([r for r, d, s in self._matches if s != 'recipientid']) == 0:
            print('  All match by recipientid.')
            return
        print_data = [(format_cga(r), d, s) for r, d, s in self._matches]
        rw = max(len(r) for r, d, s in print_data)
        dw = max(len(d) for r, d, s in print_data)
        for r, d, s in sorted(print_data, key=lambda x: x[0]):
            match_label = '{}{}'.format(s, '%' if s != 'recipientid' else '')
            print('  {:{rw}} --> {:{dw}} ({} match)'.format(r, d, match_label, rw=rw, dw=dw))

    # Prints the unmatched community/groups, with the groups that DO match, plus the model, if a group is None.
    def print_unmatched(self):
        unmatched_groups = self.unmatched_ga_s_by_community()
        if len(unmatched_groups) == 0:
            print('No unmatched communities.')
            return

        print('Unmatched community details:')
        # For each distinct community...
        for community, groups in sorted(unmatched_groups.items(), key=lambda c: c[0].upper()):
            all_groups = [r[1] for r in self._recipients_by_community_group_from_spec.keys() if
                          r[0] == community and r[1] is not None]
            unmatched = sorted(groups, key=lambda g: g if g is not None else ' ')
            if len(all_groups) == 0:
                group_list = 'no groups'
            else:
                group_list = '{} groups: {}'.format(len(all_groups), ', '.join(all_groups))
            print('  {}, {}'.format(community, group_list))
            for u in unmatched:
                if u is None:
                    recipient = self._recipients_by_community_group_from_spec[(community, u)]
                    model = '  {}?'.format(recipient.properties['model'])
                else:
                    model = ''
                print('    {} {}'.format(u, model))

    # Given a community, a list of directories containing that community name, the (group,agent)s
    # of the community, and the fuzzy match ratios, perform fuzzy matching on the
    # directories and community/group names.
    #
    # Return a dictionary of { (group,agent): {dir: fuzzy_match_ratio, ...}, ...}
    def get_fuzzy_community_match_ratios(self, community, ga_s, dirs):
        ratios = {}
        # For each group in the community...
        for ga in ga_s:
            test = self._fmt((community, ga[0], ga[1])).strip().upper()
            # test = '{} {}'.format(community, group_name).strip().upper()
            ratios[ga] = {}
            # ...make a fuzzy match test with every directory that matched the community
            for d in dirs:
                ratios[ga][d] = fuzz.ratio(test, d.upper())
        return ratios

    # Given a community, a list of directories containing that community name, the groups
    # of the community, and the fuzzy match ratios, print them out in a nice table.
    @staticmethod
    def print_fuzzy_community_match_ratios(ratios, community, ga_s, dirs, matches):
        '''
        :param ratios: { (group, agent) : {candidate_directory : match_score} }
        :param community: str
        :param ga_s: [ (group, agent), ...]
        :param potential_dirs: [ potential_dir, ...]
        :return: (num_matches, [((group, agent), directory), ...])
        '''
        def _name(ga):
            name = ga[0] if ga[0] else ''
            name += (('-' if ga[0] else '') + ga[1]) if ga[1] else ''
            name = '-' if not name else name
            return name

        dir_name_width = 20
        for d in dirs:
            dir_name_width = max(dir_name_width, len(d))
        # Print the results, header line first
        print('Community \'{}\'\ndir{:>{width}s}'.format(community, 'group ->', width=dir_name_width - 4), end=' | ')
        for ga in ga_s:
            group_label = _name(ga)
            width = max(len(group_label), 4)
            print('{:^{width}}'.format(group_label, width=width), end=' | ')
        print()
        for d in dirs:
            print('{:>{width}s}'.format(d, width=dir_name_width), end=' | ')
            for ga in ga_s:
                group_name = _name(ga)
                width = max(len(group_name), 4)
                score = '{}{}'.format(ratios[ga][d], '*' if (ga, d) in matches else ' ')
                print('{:^{width}}'.format(score, width=width), end=' | ')
            print()
        print()

    # Given a set of fuzzy matches for a community, its groups, and matching dirs,
    # remove any matches that are "good enough"
    def make_fuzzy_community_matches(self, ratios, community, ga_s, potential_dirs):
        '''
        :param ratios: { (group, agent) : {candidate_directory : match_score} }
        :param community: str
        :param ga_s: [ (group, agent), ...]
        :param potential_dirs: [ potential_dir, ...]
        :return: (num_matches, [((group, agent), directory), ...])
        '''
        matches = []
        n_removed = 0
        for ga in ga_s:
            scores = ratios[ga]
            # Find the best scoring directory for this group
            best_score, best_dir = -1, None
            for potential_dir in potential_dirs:
                if scores[potential_dir] > best_score:
                    best_score, best_dir = scores[potential_dir], potential_dir
            # Good enough to consider a match?
            if best_score > self._threshold:
                # See if this is the best scoring group for the directory
                best = True
                for dirs_ga in ga_s:
                    if dirs_ga != ga and ratios[dirs_ga][best_dir] >= best_score:
                        best = False
                        break
                if best:
                    matches.append((ga, best_dir))
                    recip = (community, ga[0], ga[1])
                    self.remove_matched(recip, best_dir, best_score)
                    n_removed = n_removed + 1

        if n_removed > 0:
            print()
        return n_removed, matches

    # Find the directory names containing community names as a substring. Do fuzzy matching
    # of community / group to the directory name, and match the ones that are "good enough".
    def make_community_matches(self):
        n_removed = 0
        # { community : [ (group, agent), ...]}
        unmatched_ga_s = self.unmatched_ga_s_by_community()

        # For each distinct community... (sorted so it is same run-to-run, for easier debugging)
        for community, ga_s in sorted(unmatched_ga_s.items(), key=lambda c: c):
            dirs = []
            # find the dirs containing that community name.
            for unmatched_dir in self._unmatched_dirs:
                if community.upper() in unmatched_dir.upper():
                    dirs.append(unmatched_dir)

            if len(dirs) > 0:
                # { (group, agent) : {candidate_directory : match_score} }
                ratios = self.get_fuzzy_community_match_ratios(community, ga_s, dirs)
                removed, matches = self.make_fuzzy_community_matches(ratios, community, ga_s, dirs)
                n_removed = n_removed + removed
                self.print_fuzzy_community_match_ratios(ratios, community, ga_s, dirs, matches)

        return n_removed

    # Given a list of unmatched directory names and recipients, finds (and removes) the
    # easy matches, which are where the names match exactly.
    def make_exact_matches(self):
        # Compare what may be a directory name against the list of unmatched directories.
        def examine(potential_dir):
            if potential_dir in self._unmatched_dirs:
                self.remove_matched(recip, potential_dir)
                return True
            potential_dir = potential_dir.upper()  # lots of combinations of casing, but only do the most trivial
            if potential_dir in self._unmatched_dirs:
                self.remove_matched(recip, potential_dir)
                return True

        n_removed = 0
        #        uppercase_dirs = [x.upper() for x in self._unmatched_dirs]
        for recip in copy.copy(self._unmatched_recipients):
            # Does this recipient already know its directory?
            recipient = self._recipients_by_community_group_from_spec[recip]
            if recipient.directory_name and recipient.directory_name in self._unmatched_dirs:
                self.remove_matched(recip, recipient.directory_name)
                n_removed = n_removed + 1
                continue

            (community, group, agent) = recip
            if group:
                test = '{}{}{}'.format(community, self._joiner, group)
                if examine(test):
                    n_removed = n_removed + 1
                    continue
                test = '{}{}{}'.format(group, self._joiner, community)
                if examine(test):
                    n_removed = n_removed + 1
                    continue
            else:
                # Is the bare community name a match?
                if examine(community):
                    n_removed = n_removed + 1
                    continue
        return n_removed

    # If a recipient has a recipientid, match that up with any directory with the same recipientid
    def _match_existing_recipientids(self):
        for recipientid, recipient in self._recipients_by_recipientid_from_spec.items():
            directory = self._directory_by_recipientid.get(recipientid)
            if directory:
                self.remove_matched(self._recip_key(recipient), directory, 'recipientid')

    # Make sure that if a recipient thinks it knows its directory, the directory doesn't contradict.
    def _check_recipientid_mismatches(self):
        for recipientid, recipient in self._recipients_by_recipientid_from_spec.items():
            if recipient.directory_name and recipientid in self._directory_by_recipientid:
                if recipient.directory_name != self._directory_by_recipientid[recipientid]:
                    # Fatal error
                    recip_name = self._recip_name(recipient)
                    print('Recipient "{}" claims directory "{}", but "{}" claims recipient.'
                          .format(recip_name, recipient.directory_name,
                                  self._directory_by_recipientid[recipientid]))

    def reconcile(self):
        # We can't reconcile the recipients if the directory doesn't even exist.
        if not self.communities_dir.exists() or not self.communities_dir.is_dir():
            errors.err(errors.missing_directory, {'directory': self.communities_dir})
            return

        # list of directory names, they all start off as unmatched.
        self._unmatched_dirs = set(self.get_community_dirs().keys())
        # list of (community, group_name) tuples
        self._unmatched_recipients = set(self.get_recipients_from_spec().keys())

        self._check_recipientid_mismatches()
        self._match_existing_recipientids()

        n_matched = self.make_exact_matches()
        n_matched = n_matched + self.make_community_matches()

        fdm = FuzzyDirectoryMatcher(self)
        n_matched = n_matched + fdm.make_matches()

        self.print_matched()
        print()
        fdm.print_unmatched()
        print()
        self.print_unmatched()

        print()
        print('{:4} matches\n{:4} unmatched community/groups\n'
              '{:4} unmatched directories\n{:4} directories without recipientid file'
              .format(len(self._matches), len(self._unmatched_recipients), len(self._unmatched_dirs),
                      len(self._directories_without_recipientid)))

        if DIRECTORIES in self._update:
            if self._directories_to_create_are_ok():
                self._create_directories_for_recipients()
                self._create_missing_recipient_id_files()
        if XDIRECTORIES in self._update:
            self._remove_unused_directories()
        if XLSX in self._update:
            self._update_recipients_with_new_matches()
            if RECIPIENTS in self._update:
                self._create_missing_recipientids()

        return


def reconcile(acmdir, spec: programspec, strategy: int, update: set, outdir=None):
    start = time.time()
    reconciler = Reconciler(acmdir, spec, strategy, update, outdir)
    result = reconciler.reconcile()
    end = time.time()
    print('Reconciled in {:.2g} seconds'.format(end - start))
    return result
