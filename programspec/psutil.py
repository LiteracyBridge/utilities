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

The Program Specification describes the deliverables of a Talking Book Program, including
the content to be delivered, the schedule of those deliveries, and the recipients for 
whom the content is intended. Additional metadata describes the locations of the 
recipients, their language, demographics, and possibly when they will join into the 
program.

The Program Specification is created as an .xlsx workbook file, with several sheets
describing the particulars of the Program. 

This application performs several utility functions relating to a Talking Book Program
Specification:
- Structural valiation of the workbook to ensure that the required sheets are present,
  with their required columns and data.
- Logical validation of the contents, including inter-relationships between data in the
  several sheets.
- Physical reconcillation of the Program Specification with an ACM database, specifically
  the communities directory. The application can:
  - Match Recipients with an existing communities directory structure, using several
    heuristics to achieve a high level of confidence in the matches.
  - Create the structure for the individual communities.
  - Create recipient.id files in the individual directories.
  - Update the spreadsheet with recipientid and directory.
  - Create recipients.csv and recipients_map.csv for uploading to a database.
  
Future work may include:
- Generation of a script to create a deployment.  
    
Note on directory strategy: Various schemes were used to create the directory names 
  for recipients. Generally, the same strategy was used for most of the recipients
  in a project, but there are outliers in every case. Also there was no consistency
  in the casing of directory names. Strategies used, numbered arbitrarily:
  0: '{group}'                (Group name)
  1: '{community} {group}     (Community name, one or more spaces, and group name.)
  2: '{group} - {community}'  (Group name, a hyphen, and community name. Zero or more 
                                spaces before and/or after the hyphen.)
  3: '{community}-{group}'    (Community name, hyphen, group. Spaces become '_'.)
"""
# file = 'commSpec.xlsx'
#
# ProgramSpecReader.options['debug'] = True
# ps = ProgramSpecReader.load(file)
# if ps.valid:
#     program = ps.Program
#
#     print(program)
#     for depl_no, d in program.deployments.items():
#         #print(d)
#         m = d.deployment_info
#         print('Deployment {}, {}, {} recipients'.format(m.name, d, len(m.recipients)))
#         for (pkg, pkginfo) in m.packages.items():
#             print('  {}'.format(pkg))
#             for (playlist, messages) in pkginfo.playlists.items():
#                 print('      {}'.format(playlist.title))
#                 for msg in messages:
#                     print('          {}'.format(msg.title))
#     for _, c in program.components.items():
#         print(c)
#
# else:
#     for msg in ps.errors:
#         print(msg, file=sys.stderr)

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
    if len(xls_dir)>0 and xls_dir[-1]!='/':
        xls_dir += '/'
    name, ext = os.path.splitext(fn)
    N = ''
    sequence = 0
    if '{N}' in args.out:
        # If the name already ends in a number, remove it from name, and use as a starting point
        if name[-1].isdigit():
            trailing = ''
            while len(name)>0 and name[-1].isdigit():
                trailing = name[-1:] + trailing
                name = name[:-1]
            sequence = int(trailing)
        # Find a unique name by incrementing N
        while Path(os.path.expanduser(args.out.format(outdir=args.outdir, dir=xls_dir, name=name, ext=ext, N=N))).exists():
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
    prog_spec = _validate()
    outpath = None
    if prog_spec:
        if args.out is not None:
            outpath = new_xlsx_name()
        acmdir = cannonical_acm_path_name(args.acm)
        reconcillation.reconcile(acmdir, prog_spec, args.strategy, update=updates, outdir=args.outdir)
        if XLSX in updates:
            prog_spec.save_changes(outpath)

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
    arg_parser.add_argument('operation', help='The operation to perform.',
                            choices=['validate', 'reconcile', 'recipients', 'export'])
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
    arg_parser.add_argument('--outdir', action=store_directory, default='.', metavar='DIR', help='Directory for output files (default ".")')
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

    if len(args.outdir)>0 and args.outdir[-1]!='/':
        args.outdir = args.outdir+'/'

    dropbox = expanduser(args.dropbox)
    print('Using {} for Dropbox.'.format(dropbox))

    if errors.has_fatal():
        errors.print_errors()
        return 1

    if args.operation == 'validate':
        do_validation()
    elif args.operation == 'reconcile':
        do_reconcilation(updates[0])
    elif args.operation == 'export':
        do_exports()


if __name__ == "__main__":
    sys.exit(main())
