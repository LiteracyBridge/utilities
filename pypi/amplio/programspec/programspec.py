import re
from collections import OrderedDict

from .programspec_constants import columns_to_members_map, VARIANT
from .spreadsheet import Spreadsheet

RECIPIENT_FIELDS = set(columns_to_members_map('recipient').values())
RECIPIENT_FIELDS.add('row_num')


# The ProgramSpec.Program that this Reader read. If any.
def get_program_spec_from_spreadsheet(spreadsheet: Spreadsheet, acm=None):
    if not spreadsheet.ok:
        return None
    partner = spreadsheet.general_info['partner']
    program_name = spreadsheet.general_info['program']
    program = Program(spreadsheet, partner_name=partner, program_name=program_name, project_name=acm)
    for depl, depl_info in spreadsheet.deployments.items():
        program.add_deployment(depl, depl_info[0], depl_info[1], depl_info[2])
    for component_name in spreadsheet.components:
        component = program.add_component(component_name)
        for r in spreadsheet.recipients[component_name]:
            component.add_recipient(r)
    # The playlists and messages are (by definition) in the correct order
    for js_message in spreadsheet.content:
        deployment = program.get_deployment(js_message.deployment_num)
        playlist = deployment.get_playlist(js_message.playlist_title)
        playlist.add_message(js_message.message_title, js_message.key_points, js_message.default_category,
                             js_message.sdg_goals, js_message.sdg_targets, js_message.filters)

    return program


class Program:
    """
    The Program encapsulates a Program Specification.

    The Program Specification captures the information needed to create a Deployment and distribute it
    to Repients, and a bit more metadata to enable better reporting of distribution and usage statistics.

    The fundamental units are Recipients and Messages.

    Recipients are contained in Components.

    Recipients are the unit of management and reporting of Talking Book distribution. For management
    purposes they are defined as members of a 'Component'. The use of Components is semi-optional;
    there must be at least one Component. However, if there are distinct cohorts of Recipients, they
    may be conveniently organized into Components.

    Messages are in Playlists, which are in turn in Deployments.

    Deployments are the time-based unit of content management. A project has one or more Deployments,
    generally targeted for a periodic distribution to Recipients (that is, on a pre-defined schedule).
    Each Deployment consists of one or more Playlists, which are the unit of organization of messages
    on the Talking Book itself. And, of course, each Playlist contains one or more Messages.

    """

    def __init__(self, reader: Spreadsheet, partner_name: str, program_name: str, project_name: str = None):
        if project_name is None:
            project_name = program_name
        self.deployments = {}
        self.components = {}

        self._reader = reader
        self._recipientids = set()
        self.partner = partner_name
        self.program = program_name
        self.project = project_name
        self.affiliate = reader.general_info.get('affiliate')


    @property
    def __name__(self):
        return str(self.program)

    def __repr__(self):
        result = '{} ({})'.format(self.program, self.partner)
        if self.deployments:
            result += '\n {} deployments'.format(len(self.deployments))
        if self.components:
            result += '\n {} components'.format(len(self.components))
        return result

    @property
    def language_codes(self):
        """
        Examines all Recipients and creates a set of all the distinct language codes.
        :return: set<str>
        """
        language_codes = set()
        for cmp in self.components.values():
            for recip in cmp.recipients:
                language_codes.add(recip.language_code)
        return language_codes

    def has_recipientid(self, recipientid):
        return recipientid in self._recipientids

    def add_recipientid(self, recipientid):
        if self.has_recipientid(recipientid):
            raise Exception("Duplicate recipientid: " + recipientid)
        self._recipientids.add(recipientid)

    def add_component(self, name):
        component = Component(self, name)
        self.components[name] = component
        return component

    def add_deployment(self, deployment_num, start_date, end_date, filters):
        deployment = Deployment(self, deployment_num, start_date, end_date, filters=filters)
        self.deployments[deployment_num] = deployment
        return deployment

    @property
    def num_deployments(self):
        return len(self.deployments)

    # @property
    # def deployments(self):
    #     return copy.copy(self.deployments)
    @property
    def deployment_numbers(self):
        return sorted(self.deployments.keys())

    def get_deployment(self, number):
        return self.deployments[number]

    @property
    def num_components(self):
        return len(self.components)

    # @property
    # def components(self):
    #      return copy.deepcopy(self._components)
    @property
    def component_names(self):
        return self.components.keys()

    def get_component(self, name):
        return self.components[name]

    @property
    def serializable(self):
        result = {'partner': self.partner, 'program': self.program, 'project': self.project}
        depls, cal = self.content
        result['deployments'] = depls
        result['content'] = cal

        components = self.Xcomponents
        result['components'] = components
        return result

    @property
    def content(self):
        result = []
        # Deployments and content calendar
        for no in self.deployment_numbers:
            deployment = self.deployments[no]
            depl = {'number': no,
                    'start_date': str(deployment.start_date.date()),
                    'end_date': str(deployment.end_date.date()),
                    'playlists': []}
            filters = {k:str(v) for k,v in deployment.filters.items()}
            depl = {**depl, **filters}
            result.append(depl)
            for playlist in deployment.playlists:
                pl = {'deployment': deployment.number,
                      'playlist': playlist.title,
                      'messages': []}
                depl['playlists'].append(pl)
                for message in playlist.messages:
                    msg = {'deployment': deployment.number,
                           'playlist': playlist.title}
                    filters = {k: str(v) for k, v in message.filters.items()}
                    msg = {**msg, **filters}
                    for k in ['title', 'key_points', 'sdg_goals', 'sdg_targets']:
                        if getattr(message, k, None) is not None:
                            msg[k] = getattr(message, k)
                    pl['messages'].append(msg)
        return result

    @property
    def Xcomponents(self):
        # Components & Recipients
        components = {}
        for component in self.components.values():
            cmp = []
            for recip in component.recipients:
                cmp.append(recip.properties)
            components[component.name] = cmp
        return components

    @property
    def has_changes(self):
        return any(x.has_changes for x in self.components.values())

    # save the changes in this Program Specification back to new_file
    def save_changes(self, file=None, force_save: bool = False):
        if self.has_changes or force_save:
            for component in self.components.values():
                component.save_changes(self._reader)
        if file is not None:
            self._reader.save(file)


class Deployment:
    def __init__(self, program: Program, deployment_num: int, start_date, end_date, filters=None):
        if filters is None:
            filters = {}
        self._playlists = []
        self._filters = Filterset(filters)

        self.program = program
        self.number = deployment_num
        self.start_date = start_date
        self.end_date = end_date

    # Adds a playlist to the deployment
    def add_playlist(self, title):
        playlist = Playlist(self, title)
        self._playlists.append(playlist)
        return playlist

    def filter(self, filtername):
        return self._filters.get(filtername)

    @property
    def filters(self):
        return self._filters

    @property
    def playlists(self):
        return self._playlists

    # Gets the playlist with the given name; adds it if it doesn't already exist
    def get_playlist(self, title):
        pl = next((pl for pl in self._playlists if pl.title == title), None)
        return pl if pl is not None else self.add_playlist(title)

    # Is the given recipient included in the deployment, given any filters that may be in effect?
    def is_recipient_included(self, recip):
        return self._filters.matches(recip.properties)

    @property
    def deployment_info(self):
        return DeploymentInfo(self)

    @property
    def __name__(self):
        return str(self.number)

    def __repr__(self):
        return '# {}, {:%y-%b-%d} - {:%y-%b-%d}'.format(self.number, self.start_date, self.end_date)


class DeploymentInfo:
    def __init__(self, deployment: Deployment):
        self._name = '{}-{:%y}-{}'.format(deployment.program.program, deployment.start_date, deployment.number)
        self._deployment = deployment
        self._recipients = None
        self._language_codes = None
        self._packages = None

    @property
    def number(self):
        return self._deployment.number

    @property
    def name(self):
        return self._name

    # All the recipients that should receive this deployment
    @property
    def recipients(self):
        if not self._recipients:
            self._recipients = []
            for component in self._deployment.program.components.values():
                for recip in component.recipients:
                    if self._deployment.is_recipient_included(recip):
                        self._recipients.append(recip)
        return self._recipients

    # The language codes of the recipients in this deployment
    @property
    def language_codes(self):
        if not self._language_codes:
            self._language_codes = set()
            for recip in self._recipients:
                self._language_codes.add(recip.language_code)
        return self._language_codes

    @property
    def packages(self):
        if not self._packages:
            self._packages = {}
            messages = {}
            # Top level organization is by language_code. Future sub-organization by tags.
            for language_code in self.language_codes:
                base_name = '{}-{}'.format(self._name, language_code)
                # All of the messages for a language_code; may be multiple packages.
                messages[language_code] = self._get_messages_for_language(language_code)

                # Get the distinct tags. Each tag that's present may become a package, 'base_name-tag'
                tags = self._get_distinct_property_values(VARIANT)
                # Get the filters on the messages, and eliminate any tags that aren't filtered in this
                # batch of messages. Only the remaining tags, if any, become distince packages.
                filters = self._get_filters_for_messages(messages[language_code])
                tags = self._get_properties_tested_by_filters(VARIANT, tags, filters)
                # Dictionary of {package name : tag filter}
                pkg_list = {**{base_name: {'language_code': language_code}},
                            **{'{}-{}'.format(base_name, tag): {VARIANT: tag, 'language_code': language_code} for tag in tags}}
                for pkg_name, pkg_tag_filter in pkg_list.items():
                    pkg_playlists = OrderedDict()
                    cur_playlist = None
                    for message in messages[language_code]:
                        # If the message matches the filter for the package, include it in the package.
                        if message.matches(pkg_tag_filter):
                            if message.playlist != cur_playlist:
                                cur_playlist = message.playlist
                                pkg_playlists[cur_playlist] = []
                            pkg_playlists[cur_playlist].append(message)
                    # Do we have anything for this package?
                    if len(pkg_playlists) > 0:
                        self._packages[pkg_name] = PackageInfo(self, pkg_name, language_code, pkg_playlists)

        return self._packages

    # For the recipients in this deployment, gets all distinct values of some given property
    def _get_distinct_property_values(self, property_name):
        property_name = property_name.lower()
        property_values = {v for v in [getattr(r, property_name) for r in self.recipients] if v}
        return property_values

    # Gets all the messages of this deployment, filtered by a given language_code.
    def _get_messages_for_language(self, language_code):
        messages = []
        for playlist in self._deployment.playlists:
            for message in playlist.messages:
                if message.accepts({'language_code': language_code}):
                    messages.append(message)
        return messages

    @staticmethod
    def _get_filters_for_messages(messages):
        filters = set()
        for message in messages:
            filters.add(message.filters)
        return filters

    @staticmethod
    def _get_properties_tested_by_filters(prop, values, filters):
        filtered = set()
        for value in values:
            for filt in filters:
                if filt.tests(prop, value):
                    filtered.add(value)
                    break
        return filtered


class PackageInfo:
    def __init__(self, deployment_info, name, language_code, playlists):
        self._deployment_info = deployment_info
        self._name = name
        self._language_code = language_code
        self._playlists = playlists

    @property
    def name(self):
        return self._name

    @property
    def language_code(self):
        return self._language_code

    @property
    def playlists(self):
        return self._playlists


class Component:
    def __init__(self, program: Program, name: str):
        self._recipients = []

        self.program = program
        self.name = name

    def __repr__(self):
        result = 'Component {}'.format(self.name)
        if self._recipients:
            result += ', {} recipients'.format(len(self._recipients))
        return result

    def add_recipient(self, *args, **kwargs):
        recip = Recipient(self, *args, kwargs)
        self._recipients.append(recip)
        return recip

    @property
    def num_recipients(self):
        return len(self._recipients)

    @property
    def recipients(self):
        return self._recipients

    def get_deployment(self, number):
        return self.program.get_deployment(number)

    @property
    def has_changes(self):
        return any(x.has_changes for x in self._recipients)

    def save_changes(self, reader):
        for recip in self._recipients:
            recip.save_changes(reader)

    @property
    def __name__(self):
        return str(self.name)



# Represents a recipient in a TB program.
class Recipient:
    def __init__(self, containing_component: Component, *args, **kwargs):
        super().__setattr__('containing_component', containing_component)
        self._properties = {}
        for dictionary in args:
            for key in dictionary:
                setattr(self, key, dictionary[key])
        for key in kwargs:
            setattr(self, key, kwargs[key])
        self._tracker = set()

    def __str__(self):
        comm = self._properties.get('community')
        grp = self._properties.get('group_name')
        ag = self._properties.get('agent')
        if grp:
            comm = comm + '/' + grp
        if ag:
            comm = comm + '(' + ag + ')'
        return comm

    @property
    def has_changes(self):
        return len(self._tracker) > 0

    @property
    def properties(self):
        return self._properties

    @property
    def id_tuple(self):
        return (self.community, self.group_name, self.agent, self.language_code)

    def __getattr__(self, item):
        if item in RECIPIENT_FIELDS:
            return self._properties.get(item, None)
        else:
            getattr(super(type(self)), item, None)

    def __setattr__(self, name, value):
        if name in RECIPIENT_FIELDS:
            if name == 'recipientid':
                if value == self._properties.get(name, None):
                    return  # no change
                if value is not None:
                    # Throws if a duplicate.
                    self.containing_component.program.add_recipientid(value)

            if self._tracker is not None and value != self._properties.get(name, None):
                if value is None:
                    print('setting None value')
                self._tracker.add(name)
            self._properties[name] = value
        else:
            super().__setattr__(name, value)

    @property
    def __name__(self):
        return str(self)+':'+str(self.recipientid)

    def save_changes(self, reader):
        for name in self._tracker:
            reader.store_value(self.containing_component.name, self._properties['row_num'], name,
                               self._properties[name])


# Represents a Playlist in a Deployment. A Playlist consists of an ordered list of messages.
class Playlist:
    def __init__(self, deployment: Deployment, title: str):
        self._messages = []

        self.deployment = deployment
        self.title = title

    def add_message(self, title, key_points, default_category, sdg_goals, sdg_targets, *args):
        message = Message(self, title, key_points, default_category, sdg_goals, sdg_targets, *args)
        self._messages.append(message)
        return message

    @property
    def messages(self):
        return self._messages

    @property
    def __name__(self):
        return str(self.title)

    def __repr__(self):
        return str(self.title)




# This recognizes 'd' or 'd.d'. To recognize 'd.d.d', change the ? to {0,2}.
sdg_re = re.compile('^(\d+(?:\.\d)?)')
# Recognizes a list of 'd' or 'd.d' separated by comma (and spaces) or spaces.
sdg_bare_re = re.compile('^(\d+(\.\d)?)( *[ ,] *\d+(\.\d)?)*$')
# Represents a Message in a Playlist. The Message has a Title and Key_Points, and may have a
# set of 'Filters' limiting the recipients of the message.
class Message:
    def __init__(self, playlist: Playlist, title: str, key_points: str, default_category: str, sdg_goals: str,
                 sdg_targets: str, *args):
        def parse_sdg(sdg):
            if type(sdg) != str or len(sdg) == 0:
                return None
            if sdg_bare_re.match(sdg):
                list = re.split('[ ,]+', sdg)
            else:
                list = sdg.split(';')
            sdgs = []
            for s in list:
                match = sdg_re.match(s.strip())
                if match:
                    sdgs.append(match.group(0))
            return ','.join(sdgs) if len(sdgs)>0 else None

        self.playlist = playlist
        self.title = title
        self.key_points = key_points
        self.default_category = default_category
        self.sdg_goals = parse_sdg(sdg_goals)
        self.sdg_targets = parse_sdg(sdg_targets)
        filters = {}
        # We expect at most one dictionary of filters.
        for arg_dict in args:
            filters.update(arg_dict)
        self._filterset = Filterset(filters)

    @property
    def filters(self):
        return self._filterset

    def filter(self, filtername):
        return self._filterset.get(filtername)

    # Does this message's filter match the given criteria?
    def matches(self, criteria):
        return self._filterset.matches(criteria)

    # Does this message's filter accept the given criteria?
    def accepts(self, criteria):
        return self._filterset.accepts(criteria)

    @property
    def __name__(self):
        return str(self.title)

    def __repr__(self):
        result = self.title
        if self.sdg_goals:
            result += ', sdg {}/{}'.format(self.sdg_goals, self.sdg_targets)
        if len(self._filterset) > 0:
            result += ', filt {}'.format(self._filterset)
        return str(self.title)


# A class that matches against a set of one or more comma-separated values. A leading ~ means 'not one of these'.
class Filter:
    def __init__(self, values):
        if values.startswith('~'):
            # work as a blacklist; match targets not in the list of values
            self._whitelist = False
            values = values[1:]
        else:
            # work as a whitelist; match targets appearing in the list of values
            self._whitelist = True
        # separate the comma-separated items, canonicalize to lower case, and freeze.
        self._values = frozenset([v.lower() for v in [v.strip() for v in values.split(',')] if len(v) > 0])

    def __hash__(self):
        return hash((self._values, self._whitelist))

    def __eq__(self, other):
        if self is other:
            return True
        elif isinstance(other, self.__class__):
            return self._whitelist == other._whitelist and self._values == other._values
        else:
            return False

    def __repr__(self):
        return '{}{}'.format('' if self._whitelist else '~', ','.join(self._values))

    # is the test_value a match for this filter?
    def matches(self, target):
        # result | whitelist | target appears in list
        #   t    |     f     |      f
        #   f    |     f     |      t
        #   f    |     t     |      f
        #   t    |     t     |      t
        target_value = target.strip().lower() if target is not None else None
        return self._whitelist == (target_value in self._values)

    def tests_for_value(self, value):
        target_value = value.strip().lower() if value is not None else None
        return target_value in self._values


# Holds one or more Filters.
#
# dict: {filtername : filterstring}
# Can be used in a set, or as a key in another dictionary.
# Note that changing the contents in any way wreaks havoc on the hash value; don't change it after construction.
class Filterset(dict):
    def __init__(self, filter_dict):
        super().__init__({k.lower(): Filter(v) for (k, v) in filter_dict.items() if len(v) > 0})

    def __hash__(self):
        rv = hash((frozenset(self), frozenset(self.values())))
        return rv

    def __eq__(self, other):
        if self is other:
            return True
        elif type(self) != type(other):
            return False

        rv = super().__eq__(other)
        return rv

    # returns a potentially less restrictive version of this Filter, removing 'ignore' if it is present
    def new_filter_ignoring(self, ignore):
        ignore = ignore.lower()
        result = Filterset({})
        result.update({k: v for (k, v) in self.items() if k != ignore})
        return result

    # returns a potentially more restrictive version of this Filter, adding 'criteria'
    def new_filter_including(self, criteria):
        result = Filterset({})
        result.update(self)
        result.update(Filterset(criteria))  # will override self's criteria
        return result

    # Does the filter match on the given criteria? If a filter key is missing from the criteria, it is considered
    # to NOT match.
    def matches(self, target):
        target = {k.lower(): v for (k, v) in target.items()}
        matches = True
        for (k, v) in self.items():
            if not v.matches(target.get(k, None)):
                matches = False
                break
        return matches

    # Does the filter accept the given criteria? If a filter key is missing from the criteria, it is considered
    # to be accepted. This is to allow for partial matching, such as "everything matching language_code X, regardless
    # of other filter criteria.
    def accepts(self, target):
        target = {prop.lower(): value for (prop, value) in target.items()}
        matches = True
        for (prop, filt) in self.items():
            # If a property was supplied, it needs to match our filter. If no property supplied, still matching.
            if prop in target:
                if not filt.matches(target.get(prop, None)):
                    matches = False
                    break
        return matches

    # Does this filter test for the given value of the property? (Either whitelisting or blacklisting --
    # is this value different from no value at all?)
    def tests(self, prop, value):
        # Do we even care about that property?
        filt = self.get(prop.lower(), None)
        # If we do, what does the filter think? Otherwise, we don't care.
        return filt.tests_for_value(value) if filt else False


# ***************************************************************************************************
# *********                                                                                **********
# *********                                  Test code                                     **********
# *********                                                                                **********
# ***************************************************************************************************
if __name__ == "__main__":
    def _test_filter(filt, tests):
        for (target, expected) in tests:
            actual = filt.matches(target)
            if actual != expected:
                print('Expected {}, actual {}, testing {} with {}'.format(expected, actual, filt, target))


    filters_spec_1 = {'component': '~NOYED', 'language_code': 'dga'}
    filters_1 = Filterset(filters_spec_1)
    filters_tests_1 = [({'language_code': 'dga'}, True),
                       ({'language_code': 'en'}, False),
                       ({'component': 'Jirapa', 'language_code': 'dga'}, True),
                       ({'component': 'NOYED'}, False),
                       ({'community': 'Ving Ving', 'language_code': 'dga'}, True),
                       ({'Group Name': 'Pitu', 'language_code': 'dga'}, True),
                       ({'community': 'Jirapa', 'language_code': 'dga'}, True),
                       ({'Group Name': 'mother-to-mother', 'language_code': 'dga'}, True)]

    _test_filter(filters_1, filters_tests_1)

    filters_2 = filters_1.new_filter_ignoring('language_code')
    filters_tests_2 = [({'language_code': 'dga'}, True),
                       ({'language_code': 'en'}, True),
                       ({'component': 'Jirapa'}, True),
                       ({'component': 'NOYED'}, False),
                       ({'community': 'Ving Ving'}, True),
                       ({'Group Name': 'Pitu'}, True),
                       ({'community': 'Jirapa'}, True),
                       ({'Group Name': 'mother-to-mother'}, True)]

    _test_filter(filters_2, filters_tests_2)

    filters_3 = filters_1.new_filter_including({'community': 'ving ving', 'group name': '~pitu'})
    filters_tests_3 = [({'community': 'ving ving', 'language_code': 'dga'}, True),
                       ({'language_code': 'en'}, False),
                       ({'community': 'ving ving', 'component': 'Jirapa', 'language_code': 'dga'}, True),
                       ({'component': 'NOYED'}, False),
                       ({'community': 'Ving Ving', 'language_code': 'dga'}, True),
                       ({'Group Name': 'Pitu'}, False),
                       ({'community': 'Jirapa'}, False),
                       ({'community': 'ving ving', 'Group Name': 'mother-to-mother', 'language_code': 'dga'}, True),
                       ({'community': 'Ving Ving', 'group name': 'mother-to-mother', 'language_code': 'dga'}, True),
                       ({'community': 'Jirapa', 'group name': 'mother-to-mother'}, False),
                       ({'community': 'Ving Ving', 'group name': 'pitu'}, False),
                       ({'community': 'Jirapa', 'group name': 'Pitu'}, False)
                       ]

    _test_filter(filters_3, filters_tests_3)

    params = {'partner': 'unicef-2'}
    comp: Component = None
    recipient = Recipient(comp, params)
    partner = recipient.partner
    affiliate = recipient.affiliate
    recipient.affiliate = 'LBG'
    affiliate2 = recipient.affiliate
    recipient.partner = 'Foo'
    partner2 = recipient.partner
    nosuch = recipient.nosuch
    recipient.nosuch = 'WHISHFUL'
    nosuch2 = recipient.nosuch

    print(recipient)
