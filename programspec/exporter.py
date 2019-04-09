import csv
from pathlib import Path

import programspec.programspec
from programspec.recipient_utils import RecipientUtils


def write_deployments_csv_file(progspec: programspec, outdir: Path):
    '''
    Writes the deployments from the given ProgramSpec to a .csv file in the given directory.
    The .csv file is named 'deployment_spec.csv', and the columns are:
        project,deployment_num,startdate,enddate,component
    :param progspec: ProgramSpec -- the ProgramSpec with deployments.
    :param outdir: Path -- the path into which to write the .csv file.
    :return: None
    '''
    deployments_file = Path(outdir, 'deployment_spec.csv')
    with deployments_file.open(mode='w', newline='', encoding='utf-8-sig') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',')
        csvwriter.writerow(['project', 'deployment_num', 'startdate', 'enddate', 'component'])
        # Sort the deployments by number, for the human readers.
        for deployment_no in sorted(progspec.deployment_numbers):
            deployment = progspec.get_deployment(deployment_no)
            # Get the component filter, if there is one, and make it printable.
            component_filter = deployment.filter('component')
            component_filter = str(component_filter) if component_filter else ''
            line = [progspec.project, deployment_no, str(deployment.start_date.date()),
                    str(deployment.end_date.date()), component_filter]
            csvwriter.writerow(line)


def write_content_csv_file(progspec: programspec, outdir: Path) -> None:
    '''
    Writes the Content Calendar from the given ProgramSpec to a .csv file in the given directory.
    The .csv file, named 'content.csv', has columns:
        deployment_num,playlist_title,message_title,key_points,language,default_category
    :param progspec: ProgramSpec -- the ProgramSpec with the Content Calendar.
    :param outdir: Path -- the path into which to write the .csv file.
    :return: None.
    '''
    content_file = Path(outdir, 'content.csv')
    with content_file.open(mode='w', newline='', encoding='utf-8-sig') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',')
        csvwriter.writerow(
            ['deployment_num', 'playlist_title', 'message_title', 'key_points', 'language', 'default_category'])

        for no in sorted(progspec.deployment_numbers):
            deployment = progspec.deployments[no]
            for playlist in deployment._playlists:
                for message in playlist._messages:
                    language_filter = message.filter('language')
                    language_filter = str(language_filter) if language_filter else ''
                    line = [deployment.number, playlist.title, message.title, message.key_points, language_filter,
                            message.default_category]
                    csvwriter.writerow(line)


def export(acmdir: Path, progspec: programspec, outdir: Path):
    recipient_utils = RecipientUtils(progspec, acmdir)
    recipient_utils.write_recipients_file(outdir)
    recipient_utils.write_recipients_map_file(outdir)

    write_deployments_csv_file(progspec, outdir)

    write_content_csv_file(progspec, outdir)
