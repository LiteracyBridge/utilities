#!/usr/bin/env python3
import argparse
import os
import sys
from os.path import expanduser
from pathlib import Path

from programspec import errors, spreadsheet, reconcillation, exporter, programspec
from programspec.programspec_constants import XLSX, UPDATABLES
from programspec.utils import KeyWords

desc = """
Program Specification Utility

The Program Specification describes the deliverables of a Talking Book Program,
including the content to be delivered, the schedule of those deliveries, and
the recipients for whom the content is intended. Additional metadata describes
the locations of the recipients, their language, demographics, and possibly
when they will join into the program.

The Program Specification is created as an .xlsx workbook file, with several
sheets describing the particulars of the Program.

Commands:

  validate   Validates the basic structure of the spreadsheet, and internal
             consistency.
             * The sreadsheet is specified in the --spec argument.

  reconcile  Validates the spreadsheet, then reconciles with the project
             directory in Dropbox.
             * The spreadsheet is specified in the --spec argument.
             * The ACM or Project name is specified in the --acm argument.
             * The --dropbox argument specifies the location of the Dropbox
               directory. Default "~/Dropbox".
             * The --strategy argument describes how Recipient directory
               names are created. See --strategy discussion, below.
             * The --update argument describes what should be updated by the
               reconcillation process.
             * The --outdir argument specifies a directory into which output
               files should be written. Default: current directory.
             * The --out argument specifies the naming scheme to be used for
               an updated spreadsheet .xlsx file. See below for more info.

             By default, the reconcile command will analyze the spreadsheet
             and project directory, but in the absence of an --update
             argument, will not update anything. Possible updates are:
             * directories, dirs     Create or update recipient directories.
             * xdirectories, xdirs   Move obsolete recipient directories to
                                     /TB-Loaders/archive/retired_communities
                                     in the project directory.
             * xlsx                  Update the program spec .xlsx file.
             * recipientids          Update the recipientids in the .xlsx,
                                     by computing what they *would* be if
                                     the corresponding directory existed.
                                     If directories are created later, this
                                     recipient id will be used.
             The update options may be abbreviated to any unambiguous value,
             so 'd', 'xd', and 'xl' are all valid.

             Considering the community name, group name, and support entity,
             the strategies for naming directories are:
             0: {group}   (with non-alphanum -> underscore)
             1: {community} or {community}-{group} (if there is a group)
             2: {community} or {group}-{community}
             3: {community} or {community}-{group} (with non-alpha -> '_')

             Because of upper/lower case and multiple spaces, fuzzy matching
             is heavily applied to try to find the best matches.

             To manually assign a match, create a recipient id in the
             directory, and add the recipient id to the recipient row in the
             program specification .xlsx file.

             When recipient directories are created, they are named by the
             strategy, but whitespace is translated to '_' and certain file-
             system-unfriendly characters are eliminated: \*?:&'"

             If a new spreadsheet is created, by default it is named like
             the input file with '-new' appended to the filename. You can
             give your own pattern, with the following substitutions:
             {dir}       The input spreadsheet's directory.
             {name}      The input spreadsheet's file name.
             {ext}       The input spreadsheet's extension (.xlsx).
             {N}         An incrementing number.
             {outdir}    From the --outdir argument.
             The default name is {dir}{name}-new{ext}.

  export     Exports .csv files suitable to insert into PostgreSQL. The
             files are:
             * deployment_spec.csv   Deployments in the project.
                 project               Name of project (same on all lines)
                 deployment_num        Deployment number of given line
                 startdate             First day of Deployment
                 enddate               Last day of Deployment
                 component             Component filter: "east" or "~east"
             * recipients.csv        Recipients in the project.
                 recipientid           The globally unique recipientid
                 project               Name of project (same on all lines)
                 partner               Partner who owns the project
                 communityname         Community name of the recipient
                 groupname             Group name of the recipient
                 affiliate             Affiliate, if any, supporting Partner
                 component             Component in which Recipient is member
                 country               Country of Recipient (geo-poly-0)
                 region                Between country and district
                 district              Smallest geographical region
                 numhouseholds         # hhs in household rotation model
                 numtbs                # tbs distributed to recipient
                 supportentity         Who to call with problems, or agent
                 model                 Distribution model: hhr, group, agent
                 language              ISO 639-3 code of language
                 coordinates           (latitude, longitude)
             * recipients_map.csv    Maps Recipients to Directories.
                 project               Name of project (same on all lines)
                 directory             Directory with greeting; used as
                                       community name in the past
                 recipientid           The globally unique recipientid
             * content.csv           The Content Calendar
                 deployment_num        Deployment number of given line
                 playlist_title        Subject title (eg, Health)
                 message_title         Message title
                 key_points            What does the message try to convey
                 language              A filter, like "swh" or "~swh"
                 default_category      Category for import into the ACM
"""


class store_directory(argparse.Action):
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        if nargs is not None:
            raise ValueError("nargs not allowed")
        super(store_directory, self).__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        print('%r %r %r' % (namespace, values, option_string))
        try:
            values = expanduser(values)
        except:
            pass
        setattr(namespace, self.dest, values)


dropbox = ''
args = {}


def cannonical_acm_path_name(acm):
    global dropbox
    acm = acm.upper()
    if not acm.startswith('ACM-'):
        acm = 'ACM-' + acm
    acm_path = dropbox + '/' + acm
    return acm_path


def cannonical_acm_project_name(acmdir):
    if acmdir is None:
        return None
    _, acm = os.path.split(acmdir)
    acm = acm.upper()
    if acm.startswith('ACM-'):
        acm = acm[4:]
    return acm


# Create a name for a new xlsx file. {outdir}, {dir}, {name}, {ext}, and {N} are substitut
def new_xlsx_name():
    global args
    fullpath = args.spec
    xls_dir, fn = os.path.split(fullpath)
    if len(xls_dir) > 0 and xls_dir[-1] != '/':
        xls_dir += '/'
    name, ext = os.path.splitext(fn)
    N = ''
    sequence = 0
    if '{N}' in args.out:
        # If the name already ends in a number, remove it from name, and use as a starting point
        if name[-1].isdigit():
            trailing = ''
            while len(name) > 0 and name[-1].isdigit():
                trailing = name[-1:] + trailing
                name = name[:-1]
            sequence = int(trailing)
        # Find a unique name by incrementing N
        while Path(
                os.path.expanduser(args.out.format(outdir=args.outdir, dir=xls_dir, name=name, ext=ext, N=N))).exists():
            sequence += 1
            N = str(sequence)
    outpath = os.path.expanduser(args.out.format(outdir=args.outdir, dir=xls_dir, name=name, ext=ext, N=N))
    return outpath


def _validate():
    global args
    file = expanduser(args.spec)
    ps = spreadsheet.load(file)
    previous_severity = -1
    for error in errors.get_errors():
        if error[0] != previous_severity:
            previous_severity = error[0]
            print('{}:'.format(errors.severity[error[0]]))
        print('  {}: {}'.format(error[1], error[2]))
    if not errors.has_error():
        return programspec.get_program_spec_from_spreadsheet(ps, cannonical_acm_project_name(args.acm))


def do_validation():
    prog_spec = _validate()
    if prog_spec:
        print(prog_spec)


def do_reconcilation(updates: set):
    global args
    if args.acm is None:
        print('Error: reconcile operation requires --acm argument')
        return
    prog_spec = _validate()
    outpath = None
    if prog_spec:
        if args.out is not None:
            outpath = new_xlsx_name()
        acmdir = cannonical_acm_path_name(args.acm)
        reconcillation.reconcile(acmdir, prog_spec, args.strategy, update=updates, outdir=args.outdir)
        if XLSX in updates:
            prog_spec.save_changes(outpath, True)


def do_exports():
    global args
    prog_spec = _validate()
    if prog_spec:
        acmdir = Path(cannonical_acm_path_name(args.acm))
        exporter.export(acmdir, prog_spec, outdir=args.outdir)


def do_make_deployment():
    pass


def main():
    global args, dropbox
    updatables = KeyWords(synonyms=UPDATABLES.synonyms, *UPDATABLES.words)

    arg_parser = argparse.ArgumentParser(epilog=desc, formatter_class=argparse.RawDescriptionHelpFormatter)
    arg_parser.add_argument('command', choices=['validate', 'reconcile', 'export'])
    arg_parser.add_argument('--spec', '--program-spec', metavar='XLSX',
                            help='Name of the Program Specification spreadsheet.')
    arg_parser.add_argument('--acm', help='Name of the ACM project.')
    arg_parser.add_argument('--dropbox', default='~/Dropbox', help='Dropbox directory (default is ~/Dropbox).')
    arg_parser.add_argument('--out', metavar='XLSX', nargs='?', default='{dir}{name}-new{ext}',
                            const='{dir}{name}-new{ext}', help='Create a new, updated Program Specification. An '
                                                               'optional filename may be specified. Deafult is the '
                                                               'original name with "-new" appended. A format string '
                                                               'with {outdir}, {dir}, {name}, and {ext} substitutions may be '
                                                               'supplied.')
    arg_parser.add_argument('--outdir', action=store_directory, default='.', metavar='DIR',
                            help='Directory for output files (default ".")')
    arg_parser.add_argument('--strategy', type=int, default=1,
                            help='What strategy was used in creating directory names?',
                            choices=[0, 1, 2, 3])
    arg_parser.add_argument('--update', '-u', metavar='itm', default='', nargs='*',
                            help='What items should be updated? Choices are "{}"'.format(updatables))
    arg_parser.add_argument('--verbose', '-v', action='count', help='Provide more verbose output.')
    args = arg_parser.parse_args()

    updates = updatables.parse(*args.update)
    if len(updates[1]) > 0:
        errors.err(errors.unknown_update, {'items': '", "'.join(updates[1])})
    if len(updates[2]) > 0:
        errors.err(errors.ambiguous_update, {'items': '", "'.join(updates[2])})

    if len(args.outdir) > 0 and args.outdir[-1] != '/':
        args.outdir = args.outdir + '/'

    dropbox = expanduser(args.dropbox)
    print('Using {} for Dropbox.'.format(dropbox))

    if errors.has_fatal():
        errors.print_errors()
        return 1

    if args.command == 'validate':
        do_validation()
    elif args.command == 'reconcile':
        do_reconcilation(updates[0])
    elif args.command == 'export':
        do_exports()


if __name__ == "__main__":
    sys.exit(main())
