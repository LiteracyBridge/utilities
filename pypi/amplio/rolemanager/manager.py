from typing import Any, Dict, Tuple, Union, List

from amplio.rolemanager import Roles
from amplio.rolemanager.rolesdb import *

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

rolesdb: RolesDb

# Email domains not allowed for "wild card" roles. Don't give a role to @gmail.com.
EXCLUDED_EMAIL_DOMAINS = ['@gmail.com', '@icloud.com', '@yahoo.com']

def open_tables(**kwargs):
    global rolesdb
    rolesdb = RolesDb(**kwargs)


# Given one or more strings of comma-separated roles, returns a single string of unique comma-separated roles.
def _normalize_role_list(*args: str) -> str:
    return Roles.normalize(*args)

def _normalize_email(email: str) -> str:
    return email.lower().strip()

# Given one or more dict(s) of {email : role_str}, merge the dicts and normalize the roles.
def _merge_roles_dicts(*args: Dict[str, str]) -> Dict[str, str]:
    result = {}
    for arg in args:
        for b_email, b_roles in arg.items():
            a_roles = result.get(b_email, '')
            result[b_email] = _normalize_role_list(a_roles, b_roles)
    return result


# Adds a dict of {email: role_str} to a program. Roles are additive.
def add_roles_to_program(program: str, roles_dict: Dict[str, str]):
    program_record = rolesdb.get_program_items().get(program, {})
    # programs_table.get_item(Key={PROGRAMS_PROGRAM_FIELD: program}).get('Item', {})
    program_roles: Dict[str, str] = program_record.get(PROGRAMS_ROLES_FIELD, {})
    merged_roles = _merge_roles_dicts(program_roles, roles_dict)
    if merged_roles != program_roles:
        program_record[PROGRAMS_ROLES_FIELD] = merged_roles
        rolesdb.put_program_item(item=program_record)


def invalidate_caches():
    rolesdb.invalidate_caches()


def get_organizations_for_organization(org: str) -> List[str]:
    """
    Returns the organization and any chain of parent organizations.
    """
    result = [org]
    item = rolesdb.get_organization_items().get(org, {})
    if ORGS_PARENT_FIELD in item:
        result.extend(get_organizations_for_organization(item[ORGS_PARENT_FIELD]))
    return result


def get_organizations_for_program(program: str) -> List[str]:
    """
    Returns a list of the owning organization, and all parent organizations, for the given programid.

    Parameters:
        program: the programid for which organizations are desired.
    """
    item = rolesdb.get_program_items().get(program, {})
    return get_organizations_for_organization(item.get(PROGRAMS_ORG_FIELD))


def get_roles_for_program(program: str) -> Dict[str, str]:
    """
    Returns a dict of {email: rolestr} for the roles defined for a program, its organization,
    and any chain of parent organizations.

    Parameters:
        program: the programid for which roles are desired.
    """
    program_record = rolesdb.get_program_items().get(program, {})
    program_roles: Dict[str, str] = program_record.get(PROGRAMS_ROLES_FIELD, {})
    org_roles: Dict[str, str] = get_roles_for_organization(program_record.get(PROGRAMS_ORG_FIELD))
    return _merge_roles_dicts(program_roles, org_roles)


def get_roles_for_user_in_program(email: str, program: str) -> str:
    """
    Returns a string of comma-separated roles assigned to the given user in the given program.

    Parameters:
        email: the email address of the user for whom the roles are desired.
        program: the programid of the program for which the user's roles are desired.
    """
    email = _normalize_email(email)
    program_roles: Dict[str, str] = get_roles_for_program(program)
    # Roles directly assigned to user
    user_roles_str: str = program_roles.get(email, '')
    # Roles assigned through organization email (ie, domain name)
    email_split = email.split('@')
    # Only look for the org part this if the address is like me@example.com
    email_domain = '@' + email_split[1] if len(email_split) == 2 and len(email_split[0]) > 0 else None
    domain_roles_str = program_roles.get(email_domain, '')

    return _normalize_role_list(user_roles_str, domain_roles_str)


def get_roles_for_user_in_organization(email: str, org: str) -> str:
    email = _normalize_email(email)
    org_roles: Dict[str, str] = get_roles_for_organization(org)
    # Roles directly assigned to user
    user_roles_str: str = org_roles.get(email, '')
    # Roles assigned through organization email (ie, domain name)
    email_split = email.split('@')
    # Only look for the org part this if the address is like me@example.com
    email_domain = '@' + email_split[1] if len(email_split) == 2 and len(email_split[0]) > 0 else None
    domain_roles_str = org_roles.get(email_domain, '')

    return _normalize_role_list(user_roles_str, domain_roles_str)


def get_roles_for_organization(org: str) -> Dict[str, str]:
    """
    Returns a dict of {email: rolestr} for the roles defined for an organization and any chain of
    parent organizations.

    Parameters:
        program: the organization for which roles are desired.
    """
    if not org:
        return {}
    org_record = rolesdb.get_organization_items().get(org, {})
    org_roles: Dict[str, str] = org_record.get(ORGS_ROLES_FIELD, {})
    admin_org_roles: Dict[str, str] = get_roles_for_organization(org_record.get(ORGS_PARENT_FIELD))
    roles_dict = _merge_roles_dicts(org_roles, admin_org_roles)
    return roles_dict


def get_defined_roles_for_user(email: str) -> Dict[str, Dict[str, str]]:
    """
    Only used in tests.
    """
    email = _normalize_email(email)
    # Roles assigned through organization email (ie, domain name)
    email_split = email.split('@')
    # Only look for the org part this if the address is like me@example.com
    email_domain = '@' + email_split[1] if len(email_split) == 2 and len(email_split[0]) > 0 else None

    program_roles = {}
    program_domain_roles = {}
    for prog in rolesdb.get_program_items().values():
        if PROGRAMS_ROLES_FIELD in prog:
            roles_str = prog[PROGRAMS_ROLES_FIELD].get(email)
            if roles_str:
                program_roles[prog[PROGRAMS_PROGRAM_FIELD]] = roles_str
            roles_str = prog[PROGRAMS_ROLES_FIELD].get(email_domain)
            if roles_str:
                program_domain_roles[prog[PROGRAMS_PROGRAM_FIELD]] = roles_str

    org_roles = {}
    org_domain_roles = {}
    for org in rolesdb.get_organization_items().values():
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
    """
    Only used in tests.
    """
    email = _normalize_email(email)
    email_split = email.split('@')
    email_domain = '@' + email_split[1] if len(email_split) == 2 and len(email_split[0]) > 0 else None

    # Build a tree of the organizations.
    # {org_name: {'name': org_name, 'roles': '...', 'parent': parent_name, 'orgs': {'child_org_name': {...}}
    root = None
    org_map = {}
    for org_name, org_item in rolesdb.get_organization_items().items():
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

    # Add programs to their owning organizations. Adds 'programs' dict to org records (from just above),
    # as {program_name: {'name': program_name, 'roles': '...', 'org': org_name}, ...}
    orphan_programs = {}
    for prog_name, prog_item in rolesdb.get_program_items().items():
        prog = {'name': prog_name}
        if prog_item.get(PROGRAMS_ROLES_FIELD):
            prog['roles'] = prog_item.get(PROGRAMS_ROLES_FIELD)
        org_name = prog_item.get(PROGRAMS_ORG_FIELD)
        prog['org'] = org_name
        if org_name in org_map:
            # Add this program to its organization's list of programs.
            org_map[org_name].setdefault('programs', {})[prog_name] = prog
        else:
            orphan_programs[prog_name] = prog

    # We have a tree of all organizations and programs. Do a breadth-first search for
    # the user or domain, and add matches to the results. Queue the children of non-matches
    # for to look deeper for matches.
    def has_admin_role(obj):
        if 'roles' in obj:
            roles_list = obj['roles'].get(email, '').split(',')
            if Roles.ADMIN_ROLE in roles_list or Roles.SUPER_USER_ROLE in roles_list:
                return True
            roles_list = obj['roles'].get(email_domain, '').split(',')
            if Roles.ADMIN_ROLE in roles_list or Roles.SUPER_USER_ROLE in roles_list:
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
    if root is not None:
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


def is_email_known(email)-> Tuple[bool, bool]:
    """
    Determine if a given email is "known", that is, has any role been assigned to that
    email address, for any program or organization. Returns two booleans; the first
    is True if the email address is known, and the second is true if it is known only
    by the domain (ie, nobody@amplio.org is known by @amplio.org)
    """
    email = _normalize_email(email)
    email_split = email.split('@')
    email_domain = '@' + email_split[1] if len(email_split) == 2 and len(email_split[0]) > 0 else None

    # Does the email address have any role assigned in any organization?
    for org_item in rolesdb.get_organization_items().values():
        roles_str = org_item.get(ORGS_ROLES_FIELD, [])
        if email in roles_str or email_domain in roles_str:
            return True, email not in roles_str
    # Does the email address have any role assigned in any program?
    for prog_item in rolesdb.get_program_items().values():
        roles_str = prog_item.get(ORGS_ROLES_FIELD, [])
        if email in roles_str or email_domain in roles_str:
            return True, email not in roles_str
    # Don't know the email address.
    return False, False


def get_programs_for_user(email: str) -> Dict[str, str]:
    """
    Gets the programs in which the given user has any role, and the user's role in that program.

    Parameters:
        email: The email address of the user for which the available programs is desired.

    Return:
        A dict of {programid: rolestr} for the user's roles.
    """
    programs = {}
    email = _normalize_email(email)
    email_split = email.split('@')
    email_domain = '@' + email_split[1] if len(email_split) == 2 and len(email_split[0]) > 0 else None

    for prog in rolesdb.get_program_items().values():
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


# noinspection PyUnusedLocal
def update_roles_for_program(program: str, old: Dict[str, str], new: Dict[str, str]):
    """
    Update the roles assigned to a program.
    Parameters:
        program: the programid for which roles are to be changed.
        old: a dict of {email: roles} (that should be) currently assigned to the program.
        new: a dict of {email: roles} to be assigned to the program.
        
    For each email address key in new, 
        if the (normalized) desired role string is empty, 
            remove that email from the list of roles
        otherwise
            set the roles for the email to the given role string.
    
    These roles are in addition to any roles assigned at an organizational level.
    """
    program_item = rolesdb.get_program_items().get(program)
    if program_item is None:
        return

    # We could look at the delta from old to new and apply those same changes. Do that
    # if people start stepping on each other.
    program_item[PROGRAMS_ROLES_FIELD] = {}
    for email, role_str in new.items():
        email = _normalize_email(email)
        role_str = _normalize_role_list(role_str)
        if role_str:
            if email not in EXCLUDED_EMAIL_DOMAINS:
                program_item[PROGRAMS_ROLES_FIELD][email] = role_str
        else:
            del program_item[PROGRAMS_ROLES_FIELD][email]

    rolesdb.put_program_item(program_item)


# noinspection PyUnusedLocal
def update_roles_for_organization(org: str, old: Dict[str, str], new: Dict[str, str]):
    """
    Update the roles assigned to an organization. These roles are inherited by all programs belonging
    to this organization, and to all programs belonging to any dependent (child) organization.
    
    Parameters:
        org: the name of the organization for which roles are to be changed.
        old: a dict of {email: roles} (that should be) currently assigned to the organization.
        new: a dict of {email: roles} to be assigned to the organization.

    For each email address key in new, 
        if the (normalized) desired role string is empty, 
            remove that email from the list of roles
        otherwise
            set the roles for the email to the given role string.

    Note that assigned roles are additive. Any roles assigned to an organization apply to all
    programs belonging to that organization or child organization, regardless of what may or may
    not be assigned to the program. In other words, roles are always "granted", and can not then
    be "denied" at a finer level.
    """
    org_item = rolesdb.get_organization_items().get(org)
    if org_item is None:
        return

    # We could look at the delta from old to new and apply those same changes. Do that
    # if people start stepping on each other.
    org_item[ORGS_ROLES_FIELD] = {}
    for email, role_str in new.items():
        email = _normalize_email(email)
        role_str = _normalize_role_list(role_str)
        if role_str:
            if email not in EXCLUDED_EMAIL_DOMAINS:
                org_item[ORGS_ROLES_FIELD][email] = role_str
        else:
            del org_item[ORGS_ROLES_FIELD][email]

    rolesdb.put_organization_item(org_item)


def get_organizations_and_dependants() -> Dict[str, Dict]:
    """
    Return a dict of {organization_name : organization_item}, where organization_item are the rows (objects?) in
    the organizations DynamoDB table. Add a 'dependent_orgs' member to each organization_item with a list of
    the names of dependent (child) organizations. (In the table, the linkage goes up, from dependent to parent).
    """
    # Make a copy because we're going to change it.
    organizations = {x[ORGS_ORGANIZATION_FIELD]: x for x in rolesdb.get_organization_items()}
    for org_name, organization in rolesdb.get_organization_items().items():
        # Ensure that every org record has a 'dependent_orgs', even if it's empty.
        organization.setdefault('dependent_orgs', [])
        parent_org_name = organization.get(ORGS_PARENT_FIELD)
        # If there is a parent org, and we know that parent org, add this org to that parent's list of dependents.
        if parent_org_name and parent_org_name in organizations:
            parent_org_item = organizations[parent_org_name]
            parents_dependent_orgs = parent_org_item.setdefault('dependent_orgs', [])
            if org_name not in parents_dependent_orgs:
                parents_dependent_orgs.append(org_name)
    return organizations
