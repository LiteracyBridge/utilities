import csv
import json
from io import StringIO
from pathlib import Path

import programspec.programspec
from programspec.recipient_utils import RecipientUtils


def write_deployments_file(self, outdir):
    deployments_file = Path(outdir, 'deployments.csv')
    with deployments_file.open(mode='w', newline='\n') as depls:
        print(
            'project,deployment,deploymentname,deploymentnumber,startdate,enddate,distribution,comment',
            file=depls)
        deployments = {n: self._spec.get_deployment(n) for n in
                       sorted(self._spec.deployment_numbers)}
        for n, depl in deployments.items():
            name = '{}-{}-{}'.format(self._spec.project,
                                     depl.start_date.year % 100, n)
            line = [self._spec.project, name, '', str(n),
                    str(depl.start_date.date()), str(depl.end_date.date()),
                    '', '']
            print(','.join(line), file=depls)


#
#   TalkingBook Map
#
def write_talkingbook_map_csv(progspec: programspec, csvfile):
    """
    Writes a talkingbook_map from the given program spec. A program spec has a talkingbook_map only
    if the recipients have a talkingbook column.
    :param progspec: A programspec that may have talkingbook ids.
    :param csvfile: The file-like object to which the csv data is written.
    :return: Nothing.
    """
    heading = False
    project = progspec.project
    for component in progspec.components.values():
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


def write_talkingbook_map_csv_file(progspec: programspec, outdir):
    talkingbook_map = Path(outdir, 'talkingbook_map.csv')
    with talkingbook_map.open(mode='w', newline='\n') as rm:
        write_talkingbook_map_csv(progspec, rm)


def get_talkingbook_map_csv(progspec: programspec):
    csv_data = StringIO()
    write_talkingbook_map_csv(progspec, csv_data)
    return csv_data


#
#  Recipients Map
#
def write_recipients_map_csv(progspec: programspec, csvfile, recip_utils: RecipientUtils = None):
    print('project,directory,recipientid', file=csvfile)
    for component in progspec.components.values():
        for recipient in component.recipients:
            if recipient.recipientid and recipient.directory_name:
                # In the recipients_map, the directory name is always upper-cased. This is because the
                # directory name is taken from, and used for "community" and "village" in the database, and
                # comes from the "community" and "village" on the Talking Book, where it is always upper cased.
                aliases = set()
                aliases.add(recipient.directory_name.upper())
                if recip_utils:
                    recipient_id_file = recip_utils.read_existing_recipient_id_file(recipient.directory_name)
                    if recipient_id_file and 'alias' in recipient_id_file:
                        aliases = aliases | set([a.upper() for a in recipient_id_file['alias']])

                for alias in aliases:
                    line = [progspec.project, alias.upper(), recipient.recipientid]
                    print(','.join(line), file=csvfile)


def write_recipients_map_csv_file(progspec: programspec, outdir, recip_utils: RecipientUtils = None):
    recipient_map = Path(outdir, 'recipients_map.csv')
    with recipient_map.open(mode='w', newline='\n') as rm:
        write_recipients_map_csv(progspec, rm, recip_utils)


def get_recipients_map_csv(progspec: programspec):
    csv_data = StringIO()
    write_recipients_map_csv(progspec, csv_data)
    return csv_data


#
#  Recipients
#
def write_recipients_csv(progspec: programspec, csvfile):
    """
    Writes a recipients.csv for the given program specification.
    :param progspec: The program spec from which to write recipients.
    :param csvfile: A file-like object to which the csv data is written.
    :return: Nothing.
    """
    columns = ['recipientid', 'project', 'partner', 'communityname', 'groupname', 'affiliate',
               'component', 'country', 'region', 'district', 'numhouseholds', 'numtbs',
               'supportentity', 'model', 'language', 'coordinates', 'agent', 'latitude', 'longitude']
    computed_props = {'project': lambda: progspec.project,
                      'coordinates': lambda: None,
                      'latitude': lambda: None,
                      'longitude': lambda: None}
    numeric_props = {'numhouseholds', 'numtbs'}
    coordinate_props = {'coordinates', 'latitude', 'longitude'}
    property_map = {'communityname': 'community', 'groupname': 'group_name',
                    'numhouseholds': 'num_hh', 'numtbs': 'num_tbs',
                    'supportentity': 'support_entity'}

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
    for component in progspec.components.values():
        for recipient in component.recipients:
            if recipient.recipientid:
                props = [val(c, recipient) for c in columns]
                # csvwriter.writerow(props)
                line = ','.join(props)
                print(line, file=csvfile)


def write_recipients_csv_file(progspec: programspec, outdir: Path):
    recipients_file = Path(outdir, 'recipients.csv')
    with recipients_file.open(mode='w', newline='\n', encoding='utf-8-sig') as csvfile:
        write_recipients_csv(progspec, csvfile)


def get_recipients_csv(progspec: programspec):
    csv_data = StringIO()
    write_recipients_csv(progspec, csv_data)
    return csv_data


#
#  Deployments
#
def write_deployments_csv(progspec: programspec, csvfile):
    """
    Writes the deployments from the given ProgramSpec to a .csv file in the given directory.
    The .csv file is named 'deployment_spec.csv', and the columns are:
        project,deployment_num,startdate,enddate,component
    :param progspec: ProgramSpec -- the ProgramSpec with deployments.
    :param csvfile: A file-like object -- the csv data is written here.
    :return: None
    """
    csvwriter = csv.writer(csvfile, delimiter=',')
    csvwriter.writerow(['project', 'deployment_num', 'startdate', 'enddate', 'component', 'name'])
    # Sort the deployments by number, for the human readers.
    for deployment_no in sorted(progspec.deployment_numbers):
        deployment = progspec.get_deployment(deployment_no)
        name = '{}-{}-{}'.format(progspec.project,
                                 deployment.start_date.year % 100, deployment_no)

        # Get the component filter, if there is one, and make it printable.
        component_filter = deployment.filter('component')
        component_filter = str(component_filter) if component_filter else ''
        line = [progspec.project, deployment_no, str(deployment.start_date.date()),
                str(deployment.end_date.date()), component_filter, name]
        csvwriter.writerow(line)


def write_deployments_csv_file(progspec: programspec, outdir: Path):
    deployment_spec_file = Path(outdir, 'deployment_spec.csv')
    with deployment_spec_file.open(mode='w', newline='', encoding='utf-8-sig') as csvfile:
        write_deployments_csv(progspec, csvfile)


def get_deployments_csv(progspec: programspec):
    csv_data = StringIO()
    write_deployments_csv(progspec, csv_data)
    return csv_data


#
#  Content Calendar
#
def write_content_csv(progspec: programspec, csvfile) -> None:
    """
    Writes the Content Calendar from the given ProgramSpec to a .csv file in the given directory.
    The .csv file, named 'content.csv', has columns:
        deployment_num,playlist_title,message_title,key_points,language,default_category
    :param progspec: ProgramSpec -- the ProgramSpec with the Content Calendar.
    :param csvfile: A file-like object -- the csv data is written here.
    :return: None.
    """
    csvwriter = csv.writer(csvfile, delimiter=',')
    csvwriter.writerow(
        ['deployment_num', 'playlist_title', 'message_title', 'key_points', 'language', 'default_category',
         'sdg_goals', 'sdg_targets'])

    for no in sorted(progspec.deployment_numbers):
        deployment = progspec.get_deployment(no)
        for playlist in deployment.playlists:
            for message in playlist.messages:
                language_filter = message.filter('language')
                language_filter = str(language_filter) if language_filter else ''
                line = [deployment.number, playlist.title, message.title, message.key_points, language_filter,
                        message.default_category, message.sdg_goals, message.sdg_targets]
                csvwriter.writerow(line)


def write_content_csv_file(progspec: programspec, outdir: Path) -> None:
    content_file = Path(outdir, 'content.csv')
    # 'utf-8-sig' is not recommended, because it puts a practically useless BOM in the file. However, if a .csv
    # contains anything beyond ASCII, Excel won't easily open it (one has to go through a difficult import
    # process). Once again, Microsoft screws the pooch for the entire world.
    with content_file.open(mode='w', newline='', encoding='utf-8-sig') as csvfile:
        write_content_csv(progspec, csvfile)


def get_content_csv(progspec: programspec):
    csv_data = StringIO()
    write_content_csv(progspec, csv_data)
    return csv_data


def get_content_json(progspec: programspec):
    content = progspec.content
    return json.dumps(content)


def write_content_json_file(progspec: programspec, outdir: Path) -> None:
    content_file = Path(outdir, 'content.json')
    with content_file.open(mode='w', newline='', encoding='utf-8') as jsonfile:
        content_json = get_content_json(progspec)
        print(content_json, file=jsonfile)


def export(acmdir: Path, progspec: programspec, outdir: Path):
    recipient_utils = RecipientUtils(progspec, acmdir)
    # recipient_utils.write_recipients_file(outdir)
    # recipient_utils.write_recipients_map_file(outdir)

    write_recipients_csv_file(progspec, outdir)
    write_recipients_map_csv_file(progspec, outdir, recipient_utils)
    write_talkingbook_map_csv_file(progspec, outdir)

    write_deployments_csv_file(progspec, outdir)

    write_content_csv_file(progspec, outdir)
    write_content_json_file(progspec, outdir)
