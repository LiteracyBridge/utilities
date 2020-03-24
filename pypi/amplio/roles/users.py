import re
import time
from typing import Dict, List

import boto3

"""
Management of Organizations, Programs, and UserRoles tables in DynamoDB.

Roles
    A role and it's meaning is defined elsewhere, this code just manages lists
    of strings. That said, roles are:
    PM - A Program Manager in a program. Approves Program Specs. Can use
         the ACM to manage content or create deployments.
    CO - Content Officer. Can use the ACM to manage content or create
         deployments.
    FO - Field Officer. Uses the TB-Loader to load content onto Talking Books.
    AD - Admin. Can add, remove, or edit users. An admin for a program can
         manage users for the program. An admin for an organization can manage
         users for the organization, any dependent organizations, and any
         programs of those organizations. Note that an admin can give themself
         any of the other roles.
    *  - Shorthand for "all roles", present and future.
    Remember, they're just strings, and may be changed at any point. 

Organizations table
    Key is the organization name. They need to be unique, so make them 
      descriptive "enough" (that is, not "UNICEF", but maybe "UNICEF Ghana".)
    An optional "roles" value is a map of { email:"roles...", ...}. The
      "roles..." is a comma separated string of role names.
    An optional "admin_org" value gives the name of an "umbrella" organization.
      User roles are inherited from the admin_org. All organizations are 
      ultimately dependent organizations of Amplio.

Programs table
    Key is the Program name.
    "ou" value (organization) "must" be a member of the Organizations table.
    The name is the name of the ACM. Leave off the "ACM-" part.
    An optional "roles" value is user roles in the Program. Note that roles
      are additive, so if a user is "PM" in an organization, and "CO"
      in a Program, they're both a "PM" and a "CO" in the program.
      
UserRoles table
    Key is the email address. 
    This is a convenience table, built from the Programs and Organizations, to
      allow easy lookup of a person's roles in Programs. As such, it isn't 
      required, and might be removed as some future time, if it doesn't seem
      to be worth the effort of keeping it up to date.
    The "roles" value is a map, by role, of lists of Programs for which the 
      user has that role. This is the data needed to make an access decision.
 
Users are specified by their email addresses, as those are (almost entirely 
  globally unique. "All users in organization X" can be indicated by an
  email address without a user part, eg @amplio.org. 
  
API:
    # for program, get roles as {email: roles}
    get_roles_for_program(program: str) -> Dict[str,str]
    # for user & program, get roles as str
    get_roles_for_user_in_program(user: str, program: str) -> str 
    
    # for org, get roles as {'org': {org: {email: roles}}, 'program': {program: {email: roles}} } 
    get_roles_for_org(org: str) -> Dict[str,Dict[str,Dict[str,str]]]
    # for user, get roles as {'org:' {org: roles}, 'program':  {program: roles}}
    get_defined_roles_for_user(user: str) -> Dict[str,Dict[str,str]]

"""
production_db = True
if production_db:
    ORGANIZATIONS_TABLE = 'organizations'
    ORGANIZATION_ROLES_TABLE = 'organization_roles'

    PROGRAMS_TABLE = 'programs'
    PROGRAM_ROLES_TABLE = 'program_roles'

    # USER_ROLES_TABLE = 'UserRoles'
    ACM_USERS_TABLE = 'acm_users'  # existing user table
    ACM_CHECKOUT_TABLE = 'acm_check_out'

    dynamodb_client = boto3.client('dynamodb')  # specify amazon service to be used
    dynamodb_resource = boto3.resource('dynamodb')

else:
    ACM_USERS_TABLE = 'local_acm_users'
    ORGANIZATIONS_TABLE = 'local_organizations'
    ORGANIZATION_ROLES_TABLE = 'local_organization_roles'
    PROGRAMS_TABLE = 'local_programs'
    PROGRAM_ROLES_TABLE = 'local_program_roles'
    dynamodb_client = boto3.client('dynamodb', endpoint_url='http://localhost:8008', region_name='us-west-2')
    dynamodb_resource = boto3.resource('dynamodb', endpoint_url='http://localhost:8008', region_name='us-west-2')

organizations_table = None
programs_table = None

# user_roles_table = None
acm_users_table = None
acm_checkout_table = None

# limit read/write capacity units of table (affects cost of table)
default_provisioning = {'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}

# Property & key names
ORGS_ORGANIZATION_FIELD: str = 'organization'
ORGS_PARENT_FIELD: str = 'parent'
ORGS_ROLES_FIELD: str = 'roles'

PROGRAMS_PROGRAM_FIELD = 'program'
PROGRAMS_ORG_FIELD = 'organization'
PROGRAMS_ROLES_FIELD = 'roles'

SUPER_USER_ROLE = '*'
ADMIN_ROLE = 'AD'
PM_ROLE = 'PM'
CONTENT_OFFICER_ROLE = 'CO'
FIELD_OFFICER_ROLE = 'FO'
ROLES = [SUPER_USER_ROLE, ADMIN_ROLE, PM_ROLE, CONTENT_OFFICER_ROLE, FIELD_OFFICER_ROLE]

SUPER_ADMIN_ROLES = ','.join(ROLES)
ADMIN_ROLES = ','.join([x for x in ROLES if x not in [SUPER_USER_ROLE]])
OPERATIONAL_ROLES = ','.join([x for x in ROLES if x not in [SUPER_USER_ROLE, ADMIN_ROLE, PM_ROLE]])
MANAGER_USER_ROLES = ','.join([x for x in ROLES if x not in [SUPER_USER_ROLE, ADMIN_ROLE]])

WRITER_ROLES = MANAGER_USER_ROLES
READER_ROLES = [FIELD_OFFICER_ROLE]


# { role : [ roles-with-at-least-as-much-access ] }
# CONTAINING_ROLES = {role: [r for r in ROLES if ROLES.index(r) <= ROLES.index(role)] for role in ROLES}


def create_table(table_name, key):
    """
    Creates table with the given name and attributes.
    :param table_name: String, name of the table.
    :param key: List of tuples of (attribute_name, attribute_type, key_type)
    """
    attribute_defs = []
    key_schema = []
    for name, attr_type, keytype in key:
        attribute_defs.append({'AttributeName': name, 'AttributeType': attr_type})
        key_schema.append({'AttributeName': name, 'KeyType': keytype})

    try:
        table = dynamodb_client.create_table(
            AttributeDefinitions=attribute_defs,
            TableName=table_name,
            KeySchema=key_schema,
            ProvisionedThroughput=default_provisioning
        )
        print('Successfully created table: ', table['TableDescription'])

    except Exception as err:
        print('ERROR: ' + str(err))


def delete_tables():
    start_time = time.time()
    num_updated = 0
    wait_list = []

    def delete(table_name):
        nonlocal num_updated, wait_list
        if table_name in existing_tables:
            table = dynamodb_resource.Table(table_name)
            table.delete()
            wait_list.append(table_name)

    def await_tables():
        nonlocal num_updated, wait_list
        for table_name in wait_list:
            # Wait for the table to be deleted before exiting
            print('Waiting for', table_name, '...')
            waiter = dynamodb_client.get_waiter('table_not_exists')
            waiter.wait(TableName=table_name)
            num_updated += 1

    existing_tables = dynamodb_client.list_tables()['TableNames']
    delete(ORGANIZATIONS_TABLE)
    delete(ORGANIZATION_ROLES_TABLE)
    delete(PROGRAMS_TABLE)
    delete(PROGRAM_ROLES_TABLE)
    # delete(USER_ROLES_TABLE)

    await_tables()

    end_time = time.time()
    print('Deleted {} tables in {:.2g} seconds'.format(num_updated, end_time - start_time))


def create_tables():
    start_time = time.time()
    num_updated = 0
    wait_list = []

    def create(table_name, args):
        nonlocal num_updated, wait_list
        if table_name not in existing_tables:
            create_table(table_name, args)
            wait_list.append(table_name)

    def await_tables():
        nonlocal num_updated, wait_list
        for table_name in wait_list:
            # Wait for the table to be deleted before exiting
            print('Waiting for', table_name, '...')
            waiter = dynamodb_client.get_waiter('table_exists')
            waiter.wait(TableName=table_name)
            num_updated += 1

    existing_tables = dynamodb_client.list_tables()['TableNames']
    create(ORGANIZATIONS_TABLE, [(ORGS_ORGANIZATION_FIELD, 'S', 'HASH')])
    create(PROGRAMS_TABLE, [(PROGRAMS_PROGRAM_FIELD, 'S', 'HASH')])

    await_tables()

    end_time = time.time()
    print('Created {} tables in {:.2g} seconds'.format(num_updated, end_time - start_time))


def open_tables():
    global organizations_table
    global programs_table
    global acm_users_table, acm_checkout_table  # , user_roles_table
    organizations_table = dynamodb_resource.Table(ORGANIZATIONS_TABLE)
    programs_table = dynamodb_resource.Table(PROGRAMS_TABLE)

    # user_roles_table = dynamodb_resource.Table(USER_ROLES_TABLE)
    acm_users_table = dynamodb_resource.Table(ACM_USERS_TABLE)
    acm_checkout_table = dynamodb_resource.Table(ACM_CHECKOUT_TABLE)


# Loads test / initial organizations data into the Organizations table.
def load_organizations_table(data=None):
    start_time = time.time()
    num_updated = 0
    if data is None:
        data = [
            # organization, admin_org, roles
            ('Amplio', None, {'bill@amplio.org': SUPER_ADMIN_ROLES, 'ryan@amplio.org': SUPER_ADMIN_ROLES,
                              'cliff@literacybridge.org': SUPER_ADMIN_ROLES, '@amplio.org': MANAGER_USER_ROLES}),
            ('LBG', 'Amplio',
             {'mdakura@literacybridge.org': ADMIN_ROLES, 'gumah@literacybridge.org': MANAGER_USER_ROLES,
              'toffic@literacybridge.org': MANAGER_USER_ROLES, '@literacybridge.org': OPERATIONAL_ROLES,
              'lindsay@amploio.org': ADMIN_ROLES}),
            ('Unicef Ghana', 'LBG', None),
            ('CBCC', 'Amplio', {'tpamba@centreforbcc.com': ADMIN_ROLES, 'silantoi@centreforbcc.com': ADMIN_ROLES,
                                '@centreforbcc.com': OPERATIONAL_ROLES}),
            ('RTI', 'CBCC', {'lindsay@amplio.org': ADMIN_ROLES}),
            ('ITU Niger', 'Amplio', None)
        ]
    for org, parent_org, roles in data:
        item = {ORGS_ORGANIZATION_FIELD: org}
        if parent_org:
            item[ORGS_PARENT_FIELD] = parent_org
        if roles:
            item[ORGS_ROLES_FIELD] = roles
        organizations_table.put_item(Item=item)
        num_updated += 1

    end_time = time.time()
    print('Added {} organizations in {:.2g} seconds'.format(num_updated, end_time - start_time))


# Loads test / initial organizations data into the Programs table.
def load_programs_table(data=None):
    start_time = time.time()
    num_updated = 0
    if data is None:
        data = [
            # program, organization, roles
            ('AMPLIO-ED', 'Amplio', None),
            ('BUSARA', 'Amplio', None),
            ('CARE', 'LBG', None),
            ('CBCC', 'CBCC', None),
            ('CBCC-ANZ', 'CBCC', None),
            ('CBCC-AT', 'CBCC', None),
            ('CBCC-ATAY', 'CBCC', None),
            ('CBCC-DEMO', 'CBCC', None),
            ('CBCC-RTI', 'RTI', None),
            ('CBCC-TEST', 'CBCC', None),
            ('DEMO', 'Amplio', {'@amplio.org': ADMIN_ROLES}),
            ('ITU-NIGER', 'ITU Niger', None),
            ('LBG-DEMO', 'LBG', None),
            ('LBG-FL', 'LBG', None),
            ('MARKETING', 'Amplio', {'lisa@amplio.org': ADMIN_ROLES, 'erin@amplio.org': OPERATIONAL_ROLES}),
            ('MEDA', 'LBG', None),
            ('TEST', 'Amplio', None),
            ('TPORTAL', 'Amplio', None),
            ('TUDRIDEP', 'LBG', None),
            ('UNFM', 'Amplio', None),
            ('UNICEF-2', 'Unicef Ghana', None),
            ('UNICEF-CHPS', 'Unicef Ghana', None),
            ('UNICEFGHDF-MAHAMA', 'CBCC', None),
            ('UWR', 'LBG', None),
            ('WKW-TLE', 'Amplio', None)

        ]
    for name, ou, roles in data:
        item = {PROGRAMS_PROGRAM_FIELD: name.upper(), PROGRAMS_ORG_FIELD: ou}
        if roles:
            item[PROGRAMS_ROLES_FIELD] = roles
        programs_table.put_item(Item=item)
        num_updated += 1

    end_time = time.time()
    print('Added {} programs in {:.2g} seconds'.format(num_updated, end_time - start_time))


def import_existing_user_roles():
    programs_to_ignore: List[str] = ['TEST-NEW', 'TEST-AUD', 'XTEST', 'ANSI-SV1', 'UNFM-CAMEROON', 'WINROCK',
                                     'TEMPLATE', 'NIGER']
    start_time = time.time()
    num_updated = 0

    sub_time = time.time()
    if production_db:
        programs = filter(lambda x: '-FB-' not in x, [x['acm_name'] for x in acm_checkout_table.scan()['Items']])
        programs = [x[4:] for x in programs]
    else:
        programs = [
            'AMPLIO-ED', 'ANSI-SV1', 'BUSARA', 'CARE', 'CBCC', 'CBCC-ANZ', 'CBCC-AT', 'CBCC-ATAY',
            'CBCC-DEMO', 'CBCC-RTI', 'CBCC-TEST', 'DEMO', 'ITU-NIGER', 'LBG-DEMO', 'LBG-FL', 'MARKETING',
            'MEDA', 'TEST', 'TPORTAL', 'UNFM', 'UNICEF-2', 'UNICEF-CHPS', 'UNICEFGHDF-MAHAMA', 'WKW-TLE', 'XTEST']
    programs = [x for x in programs if x not in programs_to_ignore]
    roles_to_add = {}
    print('Found {} programs in {:.2g} seconds'.format(len(programs), time.time() - sub_time))

    # noinspection PyShadowingNames
    def add_role(role_str, email=None, program=None):
        nonlocal num_updated
        num_updated += 1
        roles_for_program = roles_to_add.setdefault(program, {})
        roles_for_program[email] = role_str

    def roles_were_added(old, new):
        old_set = set([x.strip() for x in old.split(',')])
        new_set = set([x.strip() for x in new.split(',')])
        return not (old_set >= new_set)

    programs_roles = {}

    def get_cached_roles_for_program(prog):
        if prog not in programs_roles:
            programs_roles[prog] = get_roles_for_program(prog)
        return programs_roles[prog]

    sub_time = time.time()
    for program in programs:
        get_cached_roles_for_program(program)
    print('Filled prog cache in {:.2g} seconds'.format(time.time() - sub_time))

    # find the roles/access that are missing
    sub_time = time.time()
    existing_users = acm_users_table.scan()['Items']
    print('Found {} users in {:.2g} seconds'.format(len(existing_users), time.time() - sub_time))
    for existing_user in existing_users:
        # old table omitted leading '@' for domains, but we want it.
        email = existing_user.get('email')
        if '@' not in email:
            email = '@' + email

        is_admin = existing_user.get('admin', False)
        edit_str = existing_user.get('edit', '--')  # default won't match any valid project name.
        read_str = existing_user.get('read', '--')
        edit_regex = re.compile(edit_str)
        read_regex = re.compile(read_str)

        # Match all projects against the email's existing access
        for program in programs:
            role_str = None
            # Should user(s) with this email be able to write in this project?
            if edit_regex.match(program):
                role_str = WRITER_ROLES
                if is_admin:
                    role_str += ',' + ADMIN_ROLE
            elif read_regex.match(program):
                role_str = READER_ROLES

            if role_str:
                roles_for_program = get_cached_roles_for_program(program)
                roles_for_email = roles_for_program.get(email, '')
                if roles_were_added(roles_for_email, role_str):
                    add_role(role_str, program=program, email=email)

    for program, roles in roles_to_add.items():
        _add_roles_to_program(program=program, roles=roles)
    end_time = time.time()
    print('Imported {} existing roles in {:.2g} seconds'.format(num_updated, end_time - start_time))
    return roles_to_add


# Given one or more strings of comma-separated roles, returns a single string of unique comma-separated roles.
def _normalize_role_list(*args: str) -> str:
    # result_set = set()
    result_list = []
    for arg in args:
        if not arg:
            continue
        role_list = [x.upper() for x in [x.strip() for x in arg.split(',')] if x and x in ROLES]
        result_list.extend(role_list)
        # result_set.update(set(role_list))
    seen = set()
    result_set = [x for x in result_list if not (x in seen or seen.add(x))]
    return ','.join(result_set)


# Given one or more dict(s) of {email : role_str}, merge the dicts and normalize the roles.
def _merge_roles_dicts(*args: Dict[str, str]) -> Dict[str, str]:
    result = {}
    for arg in args:
        for b_email, b_roles in arg.items():
            a_roles = result.get(b_email, '')
            result[b_email] = _normalize_role_list(a_roles, b_roles)
    return result


# Adds a dict of {email: role_str} to a program. Roles are additive.
def _add_roles_to_program(program: str, roles: Dict[str, str]):
    program_record = programs_table.get_item(Key={PROGRAMS_PROGRAM_FIELD: program}).get('Item', {})
    program_roles: Dict[str, str] = program_record.get(PROGRAMS_ROLES_FIELD, {})
    merged_roles = _merge_roles_dicts(program_roles, roles)
    if merged_roles != program_roles:
        program_record[PROGRAMS_ROLES_FIELD] = merged_roles
        programs_table.put_item(Item=program_record)


orgs_items_cache: Dict[str, object] = {}
programs_items_cache: Dict[str, object] = {}


def invalidate_caches():
    global orgs_items_cache, programs_items_cache
    orgs_items_cache = {}
    programs_items_cache = {}


def get_org_items():
    global orgs_items_cache
    if len(orgs_items_cache) == 0:
        orgs_items_cache = {x[ORGS_ORGANIZATION_FIELD]: x for x in organizations_table.scan()['Items']}
    return orgs_items_cache


def get_program_items():
    global programs_items_cache
    if len(programs_items_cache) == 0:
        programs_items_cache = {x[PROGRAMS_PROGRAM_FIELD]: x for x in programs_table.scan()['Items']}
    return programs_items_cache


def get_organizations_for_organization(org):
    result = [org]
    item = get_org_items().get(org)
    if ORGS_PARENT_FIELD in item:
        result.extend(get_organizations_for_organization(item[ORGS_PARENT_FIELD]))
    return result


def get_organizations_for_program(program):
    item = get_program_items().get(program)
    return get_organizations_for_organization(item.get(PROGRAMS_ORG_FIELD))


# Given a program, gets the roles defined directly for the program, and any inherited from
# the program's organization and any supporting organization(s).
def get_roles_for_program(program: str) -> Dict[str, str]:
    program_record = get_program_items().get(program)
    program_roles: Dict[str, str] = program_record.get(PROGRAMS_ROLES_FIELD, {})
    org_roles: Dict[str, str] = get_roles_for_organization(program_record.get(PROGRAMS_ORG_FIELD))
    return _merge_roles_dicts(program_roles, org_roles)


def get_roles_for_user_in_program(email: str, program: str) -> str:
    program_roles: Dict[str, str] = get_roles_for_program(program)
    # Roles directly assigned to user
    user_roles_str: str = program_roles.get(email, '')
    # Roles assigned through organization email (ie, domain name)
    email_split = email.split('@')
    # Only look for the org part this if the address is like me@example.com
    email_domain = '@' + email_split[1] if len(email_split) == 2 and len(email_split[0]) > 0 else None
    domain_roles_str = program_roles.get(email_domain, '')

    return _normalize_role_list(user_roles_str, domain_roles_str)


# Given an organization, gets the roles defined directly by the organization, and any inherited from
# any supporting organization(s).
org_role_cache = {}


def get_roles_for_organization(org: str) -> Dict[str, str]:
    if not org:
        return {}
    if org in org_role_cache:
        return org_role_cache[org]

    org_record = get_org_items().get(org)
    org_roles: Dict[str, str] = org_record.get(ORGS_ROLES_FIELD, {})
    admin_org_roles: Dict[str, str] = get_roles_for_organization(org_record.get(ORGS_PARENT_FIELD))
    roles = _merge_roles_dicts(org_roles, admin_org_roles)
    org_role_cache[org] = roles
    return roles


def get_defined_roles_for_user(email: str) -> Dict[str, Dict[str, str]]:
    # Roles assigned through organization email (ie, domain name)
    email_split = email.split('@')
    # Only look for the org part this if the address is like me@example.com
    email_domain = '@' + email_split[1] if len(email_split) == 2 and len(email_split[0]) > 0 else None

    program_roles = {}
    program_domain_roles = {}
    for prog in get_program_items().values():
        if PROGRAMS_ROLES_FIELD in prog:
            roles_str = prog[PROGRAMS_ROLES_FIELD].get(email)
            if roles_str:
                program_roles[prog[PROGRAMS_PROGRAM_FIELD]] = roles_str
            roles_str = prog[PROGRAMS_ROLES_FIELD].get(email_domain)
            if roles_str:
                program_domain_roles[prog[PROGRAMS_PROGRAM_FIELD]] = roles_str

    org_roles = {}
    org_domain_roles = {}
    for org in get_org_items().values():
        if ORGS_ROLES_FIELD in org:
            roles_str = org[ORGS_ROLES_FIELD].get(email)
            if roles_str:
                org_roles[org[ORGS_ORGANIZATION_FIELD]] = roles_str
            roles_str = org[ORGS_ROLES_FIELD].get(email_domain)
            if roles_str:
                program_domain_roles[org[ORGS_ORGANIZATION_FIELD]] = roles_str

    return {'program': program_roles, 'program_domain': program_domain_roles,
            'org': org_roles, 'org_domain': org_domain_roles}


def get_admin_objects_for_user(email: str):
    email_split = email.split('@')
    email_domain = '@' + email_split[1] if len(email_split) == 2 and len(email_split[0]) > 0 else None

    # Build a tree of the organizations.
    root = None
    org_map = {}
    for org_name, org_item in get_org_items().items():
        org = org_map.setdefault(org_name, {'name': org_name})
        if org_item.get(ORGS_ROLES_FIELD):
            org['roles'] = org_item[ORGS_ROLES_FIELD]
        parent_name = org_item.get(ORGS_PARENT_FIELD)
        if parent_name:
            org['parent'] = parent_name
            parent_org = org_map.setdefault(parent_name, {'name': parent_name})
            parents_children = parent_org.setdefault('orgs', {})
            parents_children[org_name] = org
        else:  # no parent; this is Amplio
            root = org

    # Add programs to their owning organizations.
    orphan_programs = {}
    for prog_name, prog_item in get_program_items().items():
        prog = {'name': prog_name}
        if prog_item.get(PROGRAMS_ROLES_FIELD):
            prog['roles'] = prog_item.get(PROGRAMS_ROLES_FIELD)
        org_name = prog_item.get(PROGRAMS_ORG_FIELD)
        prog['org'] = org_name
        if org_name in org_map:
            org_map[org_name].setdefault('programs', {})[prog_name] = prog
        else:
            orphan_programs[prog_name] = prog

    # We have a tree of all organizations and programs. Do a breadth-first search for
    # the user or domain, and add matches to the results. Queue the children of non-matches
    # for to look deeper for matches.
    def has_admin_role(obj):
        if 'roles' in obj:
            roles = obj['roles'].get(email, '').split(',')
            if ADMIN_ROLE in roles or SUPER_USER_ROLE in roles:
                return True
            roles = obj['roles'].get(email_domain, '').split(',')
            if ADMIN_ROLE in roles:
                return True
        return False

    # noinspection PyShadowingNames
    def flatten_org(org):
        result = {'name': org['name']}
        if 'roles' in org:
            result['roles'] = org['roles']
        if 'parent' in org:
            result['parent'] = org['parent']
        if 'orgs' in org:
            result['orgs'] = [flatten_org(x) for x in org['orgs'].values()]
        if 'programs' in org:
            result['programs'] = list(org['programs'].values())
        return result

    result_orgs = []
    result_progs = []
    to_search = [root]
    while len(to_search) > 0:
        org = to_search.pop(0)
        if has_admin_role(org):
            result_orgs.append(flatten_org(org))
        else:
            if 'programs' in org:
                result_progs.extend([x for x in org['programs'].values() if has_admin_role(x)])
            to_search.extend(org.get('orgs', {}).values())

    return result_orgs, result_progs, orphan_programs


def is_email_known(email):
    email_split = email.split('@')
    email_domain = '@' + email_split[1] if len(email_split) == 2 and len(email_split[0]) > 0 else None

    for org_item in get_org_items().values():
        roles = org_item.get(ORGS_ROLES_FIELD, [])
        if email in roles or email_domain in roles:
            return True, email not in roles
    for prog_item in get_program_items().values():
        roles = prog_item.get(ORGS_ROLES_FIELD, [])
        if email in roles or email_domain in roles:
            return True, email not in roles
    return False, False


def get_programs_for_user(email: str):
    programs = {}
    email_split = email.split('@')
    email_domain = '@' + email_split[1] if len(email_split) == 2 and len(email_split[0]) > 0 else None

    for prog in get_program_items().values():
        roles_strs = []
        if PROGRAMS_ROLES_FIELD in prog:
            roles_strs.append(prog[PROGRAMS_ROLES_FIELD].get(email))
            roles_strs.append(prog[PROGRAMS_ROLES_FIELD].get(email_domain))
        if PROGRAMS_ORG_FIELD in prog:
            org_roles = get_roles_for_organization(prog[PROGRAMS_ORG_FIELD])
            roles_strs.append(org_roles.get(email))
            roles_strs.append(org_roles.get(email_domain))
        roles_str = _normalize_role_list(*roles_strs)
        if roles_str:
            programs[prog[PROGRAMS_PROGRAM_FIELD]] = roles_str
    return programs


def get_organizations_and_dependants():
    # Make a copy because we're going to change it.
    organizations = {x[ORGS_ORGANIZATION_FIELD]: x for x in get_org_items()}
    for org_name, organization in get_org_items().items():
        # Ensure that every org record has a 'dependent_orgs', even if it's empty.
        organization.setdefault('dependent_orgs', [])
        admin_org_name = organization.get(ORGS_PARENT_FIELD)
        if admin_org_name and admin_org_name in organizations:
            admin_org = organizations[admin_org_name]
            dependent_org_list = admin_org.setdefault('dependent_orgs', [])
            if org_name not in dependent_org_list:
                dependent_org_list.append(org_name)
    return organizations
