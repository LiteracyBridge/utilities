import json

from . import programspec


def QT(list):
    joined = "', '".join(list)
    return "'" + joined + "'"


def COMMA_SEP(list):
    joined = ', '.join(list)
    return "'" + joined + "'"


def NL_SEP(list, sp=4):
    return ('\n' + ' ' * sp).join(list)


class SpecDiff:
    def __init__(self, a: programspec.Program, b: programspec.Program):
        self.a: programspec.Program = a
        self.b: programspec.Program = b
        self.result = []

        a_depls = self.a.deployment_numbers
        b_depls = self.b.deployment_numbers
        self.common_depls = [x for x in a_depls if x in b_depls]


_DIFF_SPECS = {
    programspec.Program.__name__: {'title': '{type}: {str}',
                                   'attributes': ['affiliate', 'partner', 'program', 'project'],
                                   'children': ['deployments', 'components']},
    programspec.Deployment.__name__: {'title': '{type}: {str}',
                                      'attributes': ['number', 'start_date', 'end_date'],
                                      'children': ['playlists']},
    programspec.Playlist.__name__: {'title': '{type}: {str}',
                                    'attributes': ['title'],
                                    'children': ['messages']},
    programspec.Message.__name__: {'title': '{str}',
                                   'attributes': ['title', 'key_points', 'default_category', 'sdg_goals',
                                                  'sdg_targets', 'filters'],
                                   'children': []},
    programspec.Component.__name__: {'title': '{type}: {str}',
                                     'attributes': ['name'],
                                     'children': ['recipients']},
    programspec.Recipient.__name__: {'title': '{name}',
                                     'attributes': ['country', 'region', 'district', 'community', 'group_name', 'agent',
                                                    'support_entity', 'model', 'language_code', 'recipientid',
                                                    'directory_name',
                                                    'variant', 'num_hhs', 'num_tbs'],
                                     'children': []}
}


class SpecDiffPrint(SpecDiff):
    def __init__(self, a: programspec.Program, b: programspec.Program):
        super().__init__(a, b)

    def pr(self, st):
        self.result.append(st)

    def pr_diff(self, name, a, b):
        if a != b:
            self.pr('{} changed from {} to {}'.format(name, a, b))

    def _diff_obj(self, a, b, attrs, typename):
        diff_attrs = [attr for attr in attrs if str(getattr(a, attr)) != str(getattr(b, attr))]
        if len(diff_attrs) == 1:
            attr = diff_attrs[0]
            a_attr = str(getattr(a, attr))
            b_attr = str(getattr(b, attr))
            self.pr("{}, {} changed: '{}' => '{}'".format(typename, attr, a_attr,
                                                          b_attr))
        elif len(diff_attrs) > 1:
            self.pr("{} changed:".format(typename))
            w = max([len(x) for x in diff_attrs])
            for attr in diff_attrs:
                a_attr = str(getattr(a, attr))
                b_attr = str(getattr(b, attr))
                self.pr("    {:{w}} :  '{}' => '{}'".format(attr, a_attr, b_attr, w=w))

    # Differences in recipients.
    def diff_recipients(self):
        a_recips = {r.recipientid: r for cn in self.a.component_names for r in self.a.get_component(cn).recipients}
        b_recips = {r.recipientid: r for cn in self.b.component_names for r in self.b.get_component(cn).recipients}
        common_recipids = [x for x in a_recips.keys() if x in b_recips]
        if a_recips.keys() != b_recips.keys():
            added = [x for x in b_recips.keys() if x not in a_recips]
            removed = [x for x in a_recips.keys() if x not in b_recips]
            for recip_id in added:
                self.pr("Recipient added: {}, '{}'".format(recip_id, b_recips[recip_id]))
            for recip_id in removed:
                self.pr("Recipient removed: {}, '{}'".format(recip_id, a_recips[recip_id]))
        fields = [x for x in programspec.RECIPIENT_FIELDS if x != 'row_num']
        for recip_id in common_recipids:
            self._diff_obj(a_recips[recip_id], b_recips[recip_id], fields,
                           "Recipient '" + str(a_recips[recip_id]) + "'")

    # Differences in a message
    def diff_message(self, a_message, b_message):
        attrs = [x for x in dir(a_message) if x[:1] != '_' and not callable(getattr(a_message, x)) and type(
            getattr(a_message, x)) != programspec.Playlist]
        self._diff_obj(a_message, b_message, attrs, 'Message')

    # Differences in a single playlist
    def diff_playlist(self, a_playlist, b_playlist):
        a_messages = a_playlist.messages
        a_titles = [x.title for x in a_messages]
        b_messages = b_playlist.messages
        b_titles = [x.title for x in b_messages]
        common_titles = [x for x in a_titles if x in b_titles]
        if a_titles != b_titles:
            added = [x for x in b_titles if x not in a_titles]
            removed = [x for x in a_titles if x not in b_titles]
            if len(added) > 0:
                self.pr('Messages added: {}'.format(NL_SEP(added)))
            if len(removed) > 0:
                self.pr('Messages removed: {}'.format(NL_SEP(removed)))
            if len(added) == 0 and len(removed) == 0:
                self.pr('Messages re-ordered.')
        for msg_title in common_titles:
            a_message = next(filter(lambda x: x.title == msg_title, a_messages), None)
            b_message = next(filter(lambda x: x.title == msg_title, b_messages), None)
            self.diff_message(a_message, b_message)

    # Differences in a single deployment.
    def diff_deployment(self, depl_no, a_depl, b_depl):
        # Dates changed?
        if a_depl.start_date != b_depl.start_date:
            self.pr('Start date for deployment {} changed from {} to {}'.format(depl_no, a_depl.start_date,
                                                                                b_depl.start_date))
        if a_depl.end_date != b_depl.end_date:
            self.pr('End date for deployment {} changed from {} to {}'.format(depl_no, a_depl.end_date,
                                                                              b_depl.end_date))
        # Component filters changed?
        if a_depl.filters != b_depl.filters:
            self.pr('Filters for deployment {} changed from {} to {}'.format(depl_no, a_depl.filters, b_depl.filters))

        a_playlists = a_depl.playlists
        a_titles = [x.title for x in a_playlists]
        b_playlists = b_depl.playlists
        b_titles = [x.title for x in b_playlists]
        common_titles = [x for x in a_titles if x in b_titles]
        if a_titles != b_titles:
            added = [x for x in b_titles if x not in a_titles]
            removed = [x for x in a_titles if x not in b_titles]
            if len(added) > 0:
                self.pr('Playlist(s) added: {}'.format(QT(added)))
            if len(removed) > 0:
                self.pr('Playlist(s) removed: {}'.format(QT(removed)))
            if len(added) == 0 and len(removed) == 0:
                self.pr(
                    'Playlists re-ordered\n  From {}\n    To {}'.format(QT(a_titles), QT(b_titles)))
        for pl_title in common_titles:
            a_playlist = next(filter(lambda x: x.title == pl_title, a_playlists), None)
            b_playlist = next(filter(lambda x: x.title == pl_title, b_playlists), None)
            self.diff_playlist(a_playlist, b_playlist)

        # Playlists added or removed.
        # Is it worth trying to find playlists renamed?
        # Messages added / removed
        # Surely worth trying to find messages renamed.
        pass

    def diff_components(self):
        a_comps = self.a.component_names
        b_comps = self.b.component_names
        if a_comps != b_comps:
            added = [x for x in b_comps if x not in a_comps]
            removed = [x for x in a_comps if x not in b_comps]
            if len(added) > 0:
                self.pr('Components added: {}'.format(QT(added)))
            if len(removed) > 0:
                self.pr('Components removed: {}'.format(QT(removed)))

    # Differences in deployments. 
    #  - Deployments consist of include playlists, which include messages, generally in multiple languages.
    def diff_deployments(self):
        # Deployments added / removed
        if self.a.deployment_numbers != self.b.deployment_numbers:
            added = [str(x) for x in self.b.deployment_numbers if x not in self.a.deployment_numbers]
            removed = [str(x) for x in self.a.deployment_numbers if x not in self.b.deployment_numbers]
            if len(added) > 0:
                self.pr('Deployments added: {}'.format(COMMA_SEP(added)))
            if len(removed) > 0:
                self.pr('Deployments removed: {}'.format(COMMA_SEP(removed)))
        for depl_no in self.common_depls:
            a_depl = self.a.get_deployment(depl_no)
            b_depl = self.b.get_deployment(depl_no)
            self.diff_deployment(depl_no, a_depl, b_depl)

    # Global differences
    def diff_projects(self):
        """
        Find differences in global project information. 
        :return: 
        """
        self.pr_diff('partner', self.a.partner, self.b.partner)
        self.pr_diff('program', self.a.program, self.b.program)
        self.pr_diff('project', self.a.project, self.b.project)

    def diff(self):
        self.diff_projects()
        self.diff_deployments()
        self.diff_components()
        self.diff_recipients()

        return self.result


class SpecDiffDelta(SpecDiff):
    def __init__(self, a: programspec.Program, b: programspec.Program):
        super().__init__(a, b)

    def pr(self, st):
        self.result.append(st)

    def pr_diff(self, name, a, b):
        if a != b:
            self.pr('{} changed from {} to {}'.format(name, a, b))

    @staticmethod
    def _print_object(obj):
        type_name = type(obj).__name__
        if type_name not in _DIFF_SPECS:
            return str(obj)
        diff_spec = _DIFF_SPECS[type_name]
        return diff_spec.get('title', '{type}: {str}').format(type=type_name, str=str(obj), name=(obj.__name__ or None))

    def _compare_collections(self, a, b):
        def compare_dict():
            """
            Given two dictionaries, compare their contents. Items may have been removed from 'a',
            added to 'b', or changed 'a'->'b'. Since there is no intrinsic order in a dict, we
            can sort the keys and walk both lists looking for additions, deletions, and changes.
            """
            keys_a = sorted(a.keys())
            keys_b = sorted(b.keys())
            ix_a = 0
            ix_b = 0
            while ix_a < len(keys_a) and ix_b < len(keys_b):
                if keys_a[ix_a] == keys_b[ix_b]:
                    # keys are the same, compare the objects
                    obj_delta = self._compare_objects(a.get(keys_a[ix_a]), b.get(keys_b[ix_b]))
                    if obj_delta is not None:
                        delta.setdefault('changed', []).append((self._print_object(a.get(keys_a[ix_a])), obj_delta))
                    ix_a += 1
                    ix_b += 1
                elif keys_a[ix_a] < keys_b[ix_b]:
                    # In a, but not b, so "removed".
                    delta.setdefault('removed', []).append(self._print_object(a.get(keys_a[ix_a])))
                    ix_a += 1
                else:
                    # In b, but not a, so "added"
                    delta.setdefault('added', []).append(self._print_object(b.get(keys_b[ix_b])))
                    ix_b += 1
            # Removals from a, keys sort higher than all keys in b.
            while ix_a < len(keys_a):
                delta.setdefault('removed', []).append(self._print_object(a.get(keys_a[ix_a])))
                ix_a += 1
            # Additions to b, keys sort higher than all keys in a.
            while ix_b < len(keys_b):
                delta.setdefault('added', []).append(self._print_object(b.get(keys_b[ix_b])))
                ix_b += 1

        def compare_list():
            """
            Given two lists, compare their contents. Order matters in lists, but this doesn't really show
            changes in ordering.
            """
            a_map = {x.__name__: x for x in a}
            a_names = [x.__name__ for x in a]
            b_map = {x.__name__: x for x in b}
            b_names = [x.__name__ for x in b]

            a_only = [x for x in a_names if x not in b_names]
            b_only = [x for x in b_names if x not in a_names]
            common = [x for x in b_names if x in a_names]
            if len(a_only) > 0:
                delta['removed'] = [self._print_object(x) for x in a if x.__name__ in a_only]
            if len(b_only) > 0:
                delta['added'] = [self._print_object(x) for x in b if x.__name__ in b_only]
            for name in common:
                obj_a = a_map[name]
                obj_b = b_map[name]
                obj_delta = self._compare_objects(obj_a, obj_b)
                if obj_delta:
                    delta.setdefault('changed', []).append((self._print_object(obj_a), obj_delta))

        delta = {}
        # If they're lists, look for same items in same order.
        if type(a) == list:
            compare_list()

        elif type(a) == dict:
            compare_dict()

        elif type(a) == set:
            pass

        if len(delta.keys()) == 0:
            return None
        return delta

    def _compare_objects(self, a, b):
        delta = {}
        if type(a) != type(b):
            delta['diff'] = '{} and {} are different types'.format(str(a), str(b))
        else:
            diff_spec = _DIFF_SPECS[type(a).__name__]
            for attr_name in diff_spec['attributes']:
                attr_a = str(getattr(a, attr_name, None))
                attr_b = str(getattr(b, attr_name, None))
                if attr_a != attr_b:
                    delta.setdefault('attributes', {})[attr_name] = (attr_a, attr_b)
            for child_name in diff_spec['children']:
                children_a = getattr(a, child_name, None)
                children_b = getattr(b, child_name, None)
                child_delta = self._compare_collections(children_a, children_b)
                if child_delta:
                    if len(diff_spec['children']) > 1:
                        delta[child_name] = child_delta
                    else:
                        delta.update(child_delta)

        if len(delta.keys()) == 0:
            return None
        return delta

    def diff(self):
        prog_delta = self._compare_objects(self.a, self.b)

        print(json.dumps(prog_delta))
        return prog_delta
