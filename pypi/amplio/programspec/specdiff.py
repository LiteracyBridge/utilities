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

    def pr(self, str):
        self.result.append(str)

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
            for id in added:
                self.pr("Recipient added: {}, '{}'".format(id, b_recips[id]))
            for id in removed:
                self.pr("Recipient removed: {}, '{}'".format(id, a_recips[id]))
        fields = [x for x in programspec.RECIPIENT_FIELDS if x != 'row_num']
        for id in common_recipids:
            self._diff_obj(a_recips[id], b_recips[id], fields, "Recipient '" + str(a_recips[id]) + "'")

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
