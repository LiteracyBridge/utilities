
# For keeping track of issues found while reading, validating, reconciling, etc.
# Usage is like: issue(issue#, details, ...)
import sys

options = {}

FATAL = 0
ERROR = 1
ISSUE = 2
INFO = 3
NO_ISSUE = 999

severity = {FATAL: 'Fatal errors',
            ERROR: 'Errors',
            ISSUE: 'Issues',
            INFO:  'Information'}

# fatal errors - structural issues mean we can't really even read the spreadsheet
duplicate_columns = (FATAL, 100, 'Column "{column}" appears multiple times in sheet "{sheet}".')
missing_sheet = (FATAL, 101, 'Workbook is missing sheet "{sheet}".')

missing_directory = (FATAL, 200, 'Directory is missing: "{directory}"')

unknown_update = (FATAL, 400, 'Unknown item(s) to update: "{items}".')
ambiguous_update = (FATAL, 401, 'Ambiguous item(s) to update: "{items}" -- use longer word')
generic_fatal = (FATAL, 499, '{message}')

# errors - inconsistencies or missing information means this isn't a valid program specification
missing_columns_in_sheet = (ERROR, 500, 'Column(s) "{missing}" not found in sheet "{sheet}".')

missing_deployment_values = (ERROR, 510, 'Missing {columns} in Deployments, row {row}.')
missing_deployment_numbers = (ERROR, 511,  '#(s) {missing} missing from list in Deployments sheet.')
duplicate_deployment_numbers = (ERROR, 512, 'Deployment(s) # {duplicates} appears multiple times in Deployments')
deployment_start_after_end = (ERROR, 513, 'Start date "{start}" is >= end date "{end}" in deployment # {deployment}.')
deployment_lt_zero = (ERROR, 514, 'Deployment # may not be less than 0: {deployment}.')
no_deployments = (ERROR, 515, 'There are no Deployments in the Program Specification.')

message_unknown_deployment = (ERROR, 550, 'Message #{row} refers to unknown deployment "{deployment}": {title}.')

duplicate_recipientids = (ERROR, 600, 'Duplicate recipientid "{recipientid}".')
missing_recipientid_or_dir = (ERROR, 610, 'Missing recipientid or directory name in sheet "{component}", {label}: {name} (row {row}).')
missing_recipientids_or_dirs = (ERROR, 611, '... {num} more missing recipientids and/or directory names.')


community_directory_exists = (ERROR, 800, 'Directory "{directory}" already exists; can\'t use it for "{community}".')
community_directory_would_collide = (ERROR, 801, 'Directory "{directory}" would collide; can\'t use it for "{communities}".')
recipientid_would_collide = (ERROR, 802, 'Recipientid "{recipientid}" already for "{community2}"; can\'t use it for "{community}".')

validation_exception = (ERROR, 900, 'Exception attempting to validate Program Specification: {message}')
generic_issue = (ERROR, 999, '{message}')

# warnings - something may be wrong, or at least unusual
repeated_community_group = (ISSUE, 1000, 'Repeated community/group "{community}/{group}/{agent} in {component} (row {row}), already seen in Component "{component2}" (row {row2}).')
incorrect_component = (ISSUE, 1001, 'Incorrect Component in community/group "{community}/{group}" (row {row}): "{incorrect}" should be "{component}"')

missing_sheet_values = (ISSUE, 1010, 'Missing value for "{columns}" in sheet "{sheet}", row {row}.')

generic_warning = (ISSUE, 1999, '{message}')

added_recipientid = (INFO, 2010, 'Added recipientid "{recipientid}" in sheet "{component}", for {label}: {name} (row {row}).')
added_directory = (INFO, 2011, 'Added directory name "{directory}" in sheet "{component}", for {label}: {name} (row {row}).')


_severity = NO_ISSUE
_errors = []
def error(definition: tuple, args: dict = None):
    global _severity
    severity, code, fmt = definition
    if _severity == None or severity < _severity:
        _severity = severity
    message = fmt.format(**args) if args is not None else fmt
    _errors.append((severity, code, message))

def has_fatal(mark=None):
    return get_severity(mark) == FATAL

def has_error(mark=None):
    return get_severity(mark) <= ERROR

def has_issue(mark=None):
    return get_severity(mark) <= ISSUE

def has_info(mark=None):
    return get_severity(mark) <= INFO

def get_severity(mark=None):
    if mark is None:
        return _severity
    start = 0 if mark is None else mark[0]
    return min(x[0] for x in _errors[start:])

def reset():
    global _errors, _severity
    _errors = []
    _severity = NO_ISSUE

def get_errors(severity=None, mark=None):
    start = 0 if mark is None else mark[0]
    errors = [x for x in _errors[start:] if x[0]==severity or severity is None]
    errors.sort(key=lambda e:e[0])
    return errors

def print_errors(mark=None):
    previous_severity = -1
    for error in get_errors(mark=mark):
        if error[0] != previous_severity:
            previous_severity = error[0]
            print('{}:'.format(severity[error[0]]))
        print('  {}: {}'.format(error[1], error[2]))

def get_mark():
    return tuple([len(_errors)])

def err(err, args=None):
    if type(err) == tuple:
        err = error(err, args)
    else:
        err = error(generic_issue, {'message': err})
    if 'debug' in options:
        print('{} {}: {}'.format(*err), file=sys.stderr)

def warn(warning, args=None):
    global options
    if type(warning) == tuple:
        err = error(warning, args)
    else:
        err = error(generic_warning, {'message': warning})
    if 'debug' in options:
        print('{} {}: {}'.format(*err), file=sys.stderr)

