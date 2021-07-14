import csv
import json
from io import StringIO

from . import programspec
from .recipient_utils import RecipientUtils


class Exporter:
    def __init__(self, spec: programspec):
        self._spec = spec

    #
    #   TalkingBook Map
    #
    def get_talkingbook_map_data(self, csvfile):
        """
        Writes a talkingbook_map from the given program spec. A program spec has a talkingbook_map only
        if the recipients have a talkingbook column.
        :param csvfile: The file-like object to which the csv data is written.
        :return: Nothing.
        """
        heading = False
        project = self._spec.project
        for component in self._spec.components.values():
            for recipient in component.recipients:
                if recipient.recipientid and recipient.directory_name and recipient.talkingbookid:
                    if not heading:
                        heading = True
                        print('talkingbookid,recipientid,community,project', file=csvfile)
                    # In the talkingbook_map, the directory name is always upper-cased. This is because the
                    # directory name is taken from, and used for "community" and "village" in the database, and
                    # comes from the "community" and "village" on the Talking Book, where it is always upper cased.
                    community = recipient.directory_name.upper()
                    line = [recipient.talkingbookid, recipient.recipientid, community.upper(), project]
                    print(','.join(line), file=csvfile)
        return heading  # proxy for 'wrote something'

    def have_talkingbook_map_data(self):
        for component in self._spec.components.values():
            for recipient in component.recipients:
                if recipient.recipientid and recipient.directory_name and recipient.talkingbookid:
                    return True
        return False

    def get_talkingbook_map_csv(self):
        csv_data = StringIO()
        self.get_talkingbook_map_data(csv_data)
        return csv_data

    #
    #  Recipients Map
    #
    def get_recipients_map_data(self, csvfile, recip_utils: RecipientUtils = None):
        print('project,directory,recipientid', file=csvfile)
        for component in self._spec.components.values():
            for recipient in component.recipients:
                if recipient.recipientid and recipient.directory_name:
                    aliases = recip_utils.get_aliases_for_recipient(recipient)

                    for alias in aliases:
                        line = [self._spec.project, alias.upper(), recipient.recipientid]
                        print(','.join(line), file=csvfile)

    def get_recipients_map_csv(self):
        csv_data = StringIO()
        self.get_recipients_map_data(csv_data, RecipientUtils(self._spec))
        return csv_data

    #
    #  Recipients
    #
    def get_recipients_data(self, csvfile):
        """
        Writes a recipients.csv for the given program specification.
        :param csvfile: A file-like object to which the csv data is written.
        :return: Nothing.
        """
        columns = ['recipientid', 'project', 'partner', 'communityname', 'groupname', 'affiliate',
                   'component', 'country', 'region', 'district', 'numhouseholds', 'numtbs',
                   'supportentity', 'listening_model', 'languagecode', 'coordinates', 'agent',
                   'latitude', 'longitude', 'variant', 'group_size']
        computed_props = {'project': lambda: self._spec.project,
                          'affiliate': lambda: self._spec.affiliate,
                          'partner': lambda: self._spec.partner,
                          'component': lambda: component_name,
                          'coordinates': lambda: None,
                          'latitude': lambda: None,
                          'longitude': lambda: None}
        numeric_props = {'numhouseholds', 'numtbs', 'group_size'}
        coordinate_props = {'coordinates', 'latitude', 'longitude'}
        property_map = {'communityname': 'community', 'groupname': 'group_name',
                        'numhouseholds': 'num_hh', 'numtbs': 'num_tbs',
                        'supportentity': 'support_entity', 'languagecode': 'language_code'}

        # Properly retrieve and quote a value for the recipients.csv file
        def val(col, recip):
            # v = ''
            if col in property_map:
                v = recip.properties.get(property_map[col])
            elif col in computed_props:
                v = computed_props[col]()
            else:
                v = recip.properties.get(col)

            if v == '' or v is None:
                if col in numeric_props:
                    v = '0'
                elif col in coordinate_props:
                    v = ''
                else:
                    v = '""'
            elif type(v) != str:
                v = str(v)
            elif ',' in v:
                v = '"' + v + '"'

            return v

        print(','.join(columns), file=csvfile)
        for component in self._spec.components.values():
            component_name = component.name
            for recipient in component.recipients:
                if recipient.recipientid:
                    props = [val(c, recipient) for c in columns]
                    # csvwriter.writerow(props)
                    line = ','.join(props)
                    print(line, file=csvfile)

    def get_recipients_csv(self):
        csv_data = StringIO()
        self.get_recipients_data(csv_data)
        return csv_data

    #
    #  Deployments
    #
    def get_deployments_data(self, csvfile):
        """
        Writes the deployments from the given ProgramSpec to a .csv file in the given directory.
        The .csv file is named 'deployment_spec.csv', and the columns are:
            project,deployment_num,startdate,enddate,component
        :param csvfile: A file-like object -- the csv data is written here.
        :return: None
        """
        csvwriter = csv.writer(csvfile, delimiter=',')
        csvwriter.writerow(['project', 'deployment_num', 'startdate', 'enddate', 'component', 'name'])
        # Sort the deployments by number, for the human readers.
        for deployment_no in sorted(self._spec.deployment_numbers):
            deployment = self._spec.get_deployment(deployment_no)
            name = '{}-{}-{}'.format(self._spec.project,
                                     deployment.start_date.year % 100, deployment_no)

            # Get the component filter, if there is one, and make it printable.
            component_filter = deployment.filter('component')
            component_filter = str(component_filter) if component_filter else ''
            line = [self._spec.project, deployment_no, str(deployment.start_date.date()),
                    str(deployment.end_date.date()), component_filter, name]
            csvwriter.writerow(line)

    def get_deployments_csv(self):
        csv_data = StringIO()
        self.get_deployments_data(csv_data)
        return csv_data

    #
    #  Content Calendar
    #
    def get_content_data(self, csvfile) -> None:
        """
        Writes the Content Calendar from the given ProgramSpec to a .csv file in the given directory.
        The .csv file, named 'content.csv', has columns:
            deployment_num,playlist_title,message_title,key_points,language_code,default_category
        :param csvfile: A file-like object -- the csv data is written here.
        :return: None.
        """
        csvwriter = csv.writer(csvfile, delimiter=',')
        csvwriter.writerow(
            ['deployment_num', 'playlist_title', 'message_title', 'key_points', 'languagecode', 'variant',
             'default_category',
             'sdg_goals', 'sdg_targets'])

        for no in sorted(self._spec.deployment_numbers):
            deployment = self._spec.get_deployment(no)
            for playlist in deployment.playlists:
                for message in playlist.messages:
                    language_filter = message.filter('language_code')
                    language_filter = str(language_filter) if language_filter else ''
                    tag_filter = message.filter('variant')
                    tag_filter = str(tag_filter) if tag_filter else ''
                    line = [deployment.number, playlist.title, message.title, message.key_points, language_filter,
                            tag_filter, message.default_category, message.sdg_goals, message.sdg_targets]
                    csvwriter.writerow(line)

    def get_content_csv(self):
        csv_data = StringIO()
        self.get_content_data(csv_data)
        return csv_data

    def get_content_json(self):
        content = self._spec.content
        return json.dumps(content)
