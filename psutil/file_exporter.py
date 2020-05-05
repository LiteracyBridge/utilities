from pathlib import Path

from amplio.programspec import programspec
from amplio.programspec.exporter import Exporter

from recipient_file_utils import RecipientFileUtils


class FileExporter(Exporter):
    def __init__(self, acmdir: Path, progspec: programspec, outdir: Path):
        super().__init__(progspec)
        self._outdir = outdir
        self._acmdir = acmdir

    def write_deployments_csv_file(self):
        deployments_file = Path(self._outdir, 'deployments.csv')
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

    def write_recipients_csv_file(self):
        recipients_file = Path(self._outdir, 'recipients.csv')
        with recipients_file.open(mode='w', newline='\n', encoding='utf-8-sig') as csvfile:
            self.get_recipients_data(csvfile)

    def write_recipients_map_csv_file(self):
        recipient_utils = RecipientFileUtils(self._spec, self._acmdir)
        recipient_map = Path(self._outdir, 'recipients_map.csv')
        with recipient_map.open(mode='w', newline='\n') as rm:
            self.get_recipients_map_data(rm, recipient_utils)

    def write_talkingbook_map_csv_file(self):
        talkingbook_map = Path(self._outdir, 'talkingbook_map.csv')
        if self.have_talkingbook_map_data():
            with talkingbook_map.open(mode='w', newline='\n') as tbmap:
                self.get_talkingbook_map_data(tbmap)

    def write_deployment_spec_csv_file(self):
        deployment_spec_file = Path(self._outdir, 'deployment_spec.csv')
        with deployment_spec_file.open(mode='w', newline='', encoding='utf-8-sig') as csvfile:
            self.get_deployments_data(csvfile)

    def write_content_csv_file(self) -> None:
        content_file = Path(self._outdir, 'content.csv')
        # 'utf-8-sig' is not recommended, because it puts a practically useless BOM in the file. However, if a .csv
        # contains anything beyond ASCII, Excel won't easily open it (one has to go through a difficult import
        # process). Once again, Microsoft screws the pooch for the entire world.
        with content_file.open(mode='w', newline='', encoding='utf-8-sig') as csvfile:
            self.get_content_data(csvfile)

    def write_content_json_file(self) -> None:
        content_file = Path(self._outdir, 'content.json')
        with content_file.open(mode='w', newline='', encoding='utf-8') as jsonfile:
            content_json = self.get_content_json()
            print(content_json, file=jsonfile)

    def export(self):
        self.write_recipients_csv_file()
        self.write_recipients_map_csv_file()
        self.write_talkingbook_map_csv_file()

        self.write_deployment_spec_csv_file()

        self.write_content_csv_file()
        self.write_content_json_file()
