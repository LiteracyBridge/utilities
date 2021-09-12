from .recipient_utils import RecipientUtils

from . import programspec, errors

class Validator():
    def __init__(self, progspec: programspec, **kwargs):
        self.progspec = progspec
        self._fix_recips = kwargs.get('fix_recips', None) == True
        self._save_changes = kwargs.get('save_changes', None) == True
        ru = kwargs.get('recipient_utils')
        if not ru:
            ru = RecipientUtils(progspec)
        self._recipient_utils = ru

    # Generate warnings for missing recipientid or directoryname, or generate recipientids
    # and directory names, if the "fix_recips" option was given.
    def _check_missing_recipientids(self):
        def get_label(recipient):
            label = 'community'
            name = recipient.community
            grp = recipient.group_name
            if grp:
                label += '/group'
                name += '/' + grp
            agt = recipient.agent
            if agt:
                label += '/agent'
                name += '/' + agt
            return label,name

        project_uses_custom_greetings = True
        rows_to_report = 5
        rows_found = 0

        for component in self.progspec.components.values():
            for recipient in component.recipients:
                recipientid = recipient.recipientid
                directory_name = recipient.directory_name
                lbl = None

                if project_uses_custom_greetings and not directory_name and self._fix_recips:
                    directory_name = self._recipient_utils.compute_directory(recipient)
                    recipient.directory_name = directory_name
                    lbl = get_label(recipient)
                    args = {'component': component.name, 'label': lbl[0], 'name': lbl[1],
                            'row': recipient.row_num, 'directory': directory_name}
                    errors.error(errors.added_directory, args)

                if not recipientid and self._fix_recips:
                    recipientid_str = directory_name or self._recipient_utils.compute_directory(recipient)
                    recipientid = self._recipient_utils.compute_recipientid(recipientid_str)
                    recipient.recipientid = recipientid
                    lbl = lbl or get_label(recipient)
                    args = {'component': component.name, 'label': lbl[0], 'name': lbl[1],
                            'row': recipient.row_num, 'recipientid': recipientid}
                    errors.error(errors.added_recipientid, args)

                if not recipientid or (project_uses_custom_greetings and not directory_name):
                    if rows_found < rows_to_report:
                        lbl = get_label(recipient)
                        args = {'component': component.name, 'label': lbl[0], 'name': lbl[1],
                                'row': recipient.row_num}
                        errors.error(errors.missing_recipientid_or_dir, args)
                    rows_found += 1
        if rows_found > rows_to_report:
            args = {'num': rows_found - rows_to_report}
            errors.error(errors.missing_recipientids_or_dirs, args)

    def validate(self):
        self._check_missing_recipientids()
        if self._save_changes:
            self.progspec.save_changes()
        pass
