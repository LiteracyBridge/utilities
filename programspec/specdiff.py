import programspec, programspec.programspec


def pr_diff(name, a, b):
    if a != b:
        print('{} changed from {} to {}'.format(name, a, b))


class SpecDiff:
    def __init__(self, a: programspec.Program, b: programspec.Program):
        self.a: programspec.Program = a
        self.b: programspec.Program = b

        a_depls = self.a.deployment_numbers
        b_depls = self.b.deployment_numbers
        self.common_depls = [x for x in a_depls if x in b_depls]

    def _diff_obj(self, a, b, attrs, typename):
        diff_attrs = [attr for attr in attrs if str(getattr(a, attr)) != str(getattr(b, attr))]
        if len(diff_attrs) == 1:
            attr = diff_attrs[0]
            a_attr = str(getattr(a, attr))
            b_attr = str(getattr(b, attr))
            print("{}, {} changed: '{}' => '{}'".format(typename, attr, a_attr,
                  b_attr))
        elif len(diff_attrs) > 1:
            print("{} changed:".format(typename))
            w = max([len(x) for x in diff_attrs])
            for attr in diff_attrs:
                a_attr = str(getattr(a, attr))
                b_attr = str(getattr(b, attr))
                print("    {:{w}} :  '{}' => '{}'".format(attr, a_attr, b_attr, w=w))

    def diff_recipients(self):
        a_recips = {r.recipientid:r for cn in self.a.component_names for r in self.a.get_component(cn).recipients}
        b_recips = {r.recipientid:r for cn in self.b.component_names for r in self.b.get_component(cn).recipients}
        common_recipids = [x for x in a_recips.keys() if x in b_recips]
        if a_recips.keys() != b_recips.keys():
            added = [x for x in b_recips.keys() if x not in a_recips]
            removed = [x for x in a_recips.keys() if x not in b_recips]
            for id in added:
                print("Recipient added: {}, '{}'".format(id, b_recips[id]))
            for id in removed:
                print("Recipient removed: {}, '{}'".format(id, a_recips[id]))
        fields = [x for x in programspec.programspec.RECIPIENT_FIELDS if x != 'row_num']
        for id in common_recipids:
            self._diff_obj(a_recips[id], b_recips[id], fields, "Recipient '"+str(a_recips[id])+"'")


        # Recipient ids added
        # Recipient ids removed
        # Recipient ids with changed properties
        pass

    def diff_message(self, depl_no, playlist_title, a_message, b_message):
        attrs = [x for x in dir(a_message) if x[:1] != '_' and not callable(getattr(a_message, x)) and type(
            getattr(a_message, x)) != programspec.programspec.Playlist]
        self._diff_obj(a_message, b_message, attrs, "Deployment # {}, playlist {}, message".format(depl_no, playlist_title))

    def diff_playlist(self, depl_no, a_playlist, b_playlist):
        a_messages = a_playlist.messages
        a_titles = [x.title for x in a_messages]
        b_messages = b_playlist.messages
        b_titles = [x.title for x in b_messages]
        common_titles = [x for x in a_titles if x in b_titles]
        if a_titles != b_titles:
            added = [x for x in b_titles if x not in a_titles]
            removed = [x for x in a_titles if x not in b_titles]
            if len(added) > 0:
                print("Deployment # {}, playlist {}, messages added: {}".format(depl_no, a_playlist.title, added))
            if len(removed) > 0:
                print("Deployment # {}, playlist {}, messages removed: {}".format(depl_no, a_playlist.title, removed))
            if len(added) == 0 and len(removed) == 0:
                print('Messages re-ordered.')
        for msg_title in common_titles:
            a_message = next(filter(lambda x: x.title == msg_title, a_messages), None)
            b_message = next(filter(lambda x: x.title == msg_title, b_messages), None)
            self.diff_message(depl_no, a_playlist.title, a_message, b_message)

    def diff_deployment(self, depl_no, a_depl, b_depl):
        # Dates changed
        if a_depl.start_date != b_depl.start_date:
            print('Start date for deployment {} changed from {} to {}'.format(depl_no, a_depl.start_date,
                                                                              b_depl.start_date))
        if a_depl.end_date != b_depl.end_date:
            print('End date for deployment {} changed from {} to {}'.format(depl_no, a_depl.end_date,
                                                                            b_depl.end_date))
        # Component filters changed
        if a_depl.filters != b_depl.filters:
            print('Filters for deployment {} changed from {} to {}'.format(depl_no, a_depl.filters, b_depl.filters))

        a_playlists = a_depl.playlists
        a_titles = [x.title for x in a_playlists]
        b_playlists = b_depl.playlists
        b_titles = [x.title for x in b_playlists]
        common_titles = [x for x in a_titles if x in b_titles]
        if a_titles != b_titles:
            added = [x for x in b_titles if x not in a_titles]
            removed = [x for x in a_titles if x not in b_titles]
            if len(added) > 0:
                print('Deployment # {} playlists added: {}'.format(depl_no, added))
            if len(removed) > 0:
                print('Deployment # {} playlists removed: {}'.format(depl_no, removed))
            if len(added) == 0 and len(removed) == 0:
                print('Deployment # {} playlists re-ordered.'.format(depl_no))
        for pl_title in common_titles:
            a_playlist = next(filter(lambda x: x.title == pl_title, a_playlists), None)
            b_playlist = next(filter(lambda x: x.title == pl_title, b_playlists), None)
            self.diff_playlist(depl_no, a_playlist, b_playlist)

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
                print('Components added: {}'.format(added))
            if len(removed) > 0:
                print('Components removed: {}'.format(removed))

    def diff_deployments(self):
        a_depls = self.a.deployment_numbers
        b_depls = self.b.deployment_numbers
        # Deployments added / removed
        if a_depls != b_depls:
            added = [x for x in b_depls if x not in a_depls]
            removed = [x for x in a_depls if x not in b_depls]
            if len(added) > 0:
                print('Deployments added: {}'.format(added))
            if len(removed) > 0:
                print('Deployments removed: {}'.format(removed))
        for depl_no in self.common_depls:
            a_depl = self.a.get_deployment(depl_no)
            b_depl = self.b.get_deployment(depl_no)
            self.diff_deployment(depl_no, a_depl, b_depl)

    def diff_projects(self):
        # partner
        pr_diff('partner', self.a.partner, self.b.partner)
        pr_diff('program', self.a.program, self.b.program)
        pr_diff('project', self.a.project, self.b.project)
        # program
        # project

    def diff(self):
        self.diff_projects()
        self.diff_deployments()
        self.diff_components()
        self.diff_recipients()
