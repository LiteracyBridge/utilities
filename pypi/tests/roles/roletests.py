import json
import re
import time
from typing import List

import boto3

import amplio.rolemanager.manager as roles_manager
from amplio.rolemanager import Roles
from amplio.rolemanager.Roles import *
from amplio.rolemanager.rolesdb import ORGS_ORGANIZATION_FIELD, ORGS_PARENT_FIELD, ORGS_ROLES_FIELD, \
    PROGRAMS_PROGRAM_FIELD, PROGRAMS_ORG_FIELD, PROGRAMS_ROLES_FIELD

# noinspection PyUnusedLocal


production_db = True
if production_db:
    ACM_USERS_TABLE = 'acm_users'  # existing user table
    ACM_CHECKOUT_TABLE = 'acm_check_out'

    dynamodb_client = boto3.client('dynamodb')  # specify amazon service to be used
    dynamodb_resource = boto3.resource('dynamodb')

else:
    ACM_USERS_TABLE = 'local_acm_users'
    dynamodb_client = boto3.client('dynamodb', endpoint_url='http://localhost:8008', region_name='us-west-2')
    dynamodb_resource = boto3.resource('dynamodb', endpoint_url='http://localhost:8008', region_name='us-west-2')

# user_roles_table = None
acm_users_table = None
acm_checkout_table = None


def open_tables(**kwargs):
    global acm_users_table, acm_checkout_table
    roles_manager.open_tables(**kwargs)
    acm_users_table = dynamodb_resource.Table(ACM_USERS_TABLE)
    acm_checkout_table = dynamodb_resource.Table(ACM_CHECKOUT_TABLE)


# Loads test / initial organizations data into the Organizations table.
def load_organizations_table(data=None):
    start_time = time.time()
    num_updated = 0
    if data is None:
        data = [  # organization, admin_org, roles
            # Amplio
            ('Amplio', None, {'bill@amplio.org': SUPER_ADMIN_ROLES, 'ryan@amplio.org': SUPER_ADMIN_ROLES,
                              'cliff@literacybridge.org': SUPER_ADMIN_ROLES, '@amplio.org': MANAGER_USER_ROLES}
             ),
            ('Busara Center for Behavioral Economics', 'Amplio', None),
            ('International Telecommunication Union (ITU)', 'Amplio', None),
            ('Université Numérique Francophone Mondiale', 'Amplio', None),

            # CBCC
            ('Centre for Behaviour Change Communication (CBCC)', 'Amplio',
             {'tpamba@centreforbcc.com': ADMIN_ROLES, 'silantoi@centreforbcc.com': ADMIN_ROLES,
              '@centreforbcc.com': OPERATIONAL_ROLES}
             ),
            ('RTI International', 'Centre for Behaviour Change Communication (CBCC)', None),
            ('Nutrition International', 'Centre for Behaviour Change Communication (CBCC)', None),
            ('Amref', 'Centre for Behaviour Change Communication (CBCC)', None),
            ('UNICEF Rwanda', 'Centre for Behaviour Change Communication (CBCC)', None),

            # LBG
            ('Literacy Bridge Ghana (LBG)', 'Amplio',
             {'mdakura@literacybridge.org': ADMIN_ROLES, 'gumah@literacybridge.org': MANAGER_USER_ROLES,
              'toffic@literacybridge.org': MANAGER_USER_ROLES, '@literacybridge.org': OPERATIONAL_ROLES}
             ),
            ('UNICEF Ghana', 'Literacy Bridge Ghana (LBG)', None),
            ('CARE Ghana', 'Literacy Bridge Ghana (LBG)', None),
            ('MEDA Ghana', 'Literacy Bridge Ghana (LBG)', None),
            ('TUDRIDEP', 'Literacy Bridge Ghana (LBG)', None),

            # WKW
            ('Whiz Kids Workshop', 'Amplio', None),

        ]
    for org, parent_org, roles in data:
        item = {ORGS_ORGANIZATION_FIELD: org}
        if parent_org:
            item[ORGS_PARENT_FIELD] = parent_org
        if roles:
            item[ORGS_ROLES_FIELD] = roles
        roles_manager.rolesdb.put_organization_item(item=item)
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
            ('DEMO', 'Amplio', {'@amplio.org': ADMIN_ROLES}),
            ('MARKETING', 'Amplio', {'lisa@amplio.org': ADMIN_ROLES, 'erin@amplio.org': OPERATIONAL_ROLES}),
            ('TEST', 'Amplio', None),
            ('TPORTAL', 'Amplio', None),

            ('BUSARA', 'Busara Center for Behavioral Economics', None),
            ('ITU-NIGER', 'International Telecommunication Union (ITU)', None),
            ('UNFM', 'Université Numérique Francophone Mondiale', None),

            ('CBCC', 'Centre for Behaviour Change Communication (CBCC)', None),
            ('CBCC-DEMO', 'Centre for Behaviour Change Communication (CBCC)', None),
            ('CBCC-TEST', 'Centre for Behaviour Change Communication (CBCC)', None),

            ('CBCC-ANZ', 'Nutrition International', None),
            ('CBCC-AT', 'Amref', None),
            ('CBCC-ATAY', 'Amref', None),
            ('CBCC-RTI', 'RTI International', None),
            ('UNICEFGHDF-MAHAMA', 'UNICEF Rwanda', None),

            ('LBG-DEMO', 'Literacy Bridge Ghana (LBG)', None),

            ('LBG-FL', 'Literacy Bridge Ghana (LBG)', None),
            ('LBG-COVID19', 'Literacy Bridge Ghana (LBG)', None),
            ('CARE', 'CARE Ghana', None),
            ('MEDA', 'MEDA Ghana', None),
            ('TUDRIDEP', 'TUDRIDEP', None),
            ('UNICEF-2', 'UNICEF Ghana', None),
            ('UNICEF-CHPS', 'UNICEF Ghana', None),
            ('UWR', 'UNICEF Ghana', None),

            ('WKW-TLE', 'Whiz Kids Workshop', None),
        ]
    for name, ou, roles in data:
        item = {PROGRAMS_PROGRAM_FIELD: name.upper(), PROGRAMS_ORG_FIELD: ou}
        if roles:
            item[PROGRAMS_ROLES_FIELD] = roles
        roles_manager.rolesdb.put_program_item(item=item)
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
            programs_roles[prog] = roles_manager.get_roles_for_program(prog)
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
        roles_manager.add_roles_to_program(program=program, roles_dict=roles)
    end_time = time.time()
    print('Imported {} existing roles in {:.2g} seconds'.format(num_updated, end_time - start_time))
    return roles_to_add


_make_fake_data: bool = False


def clear_cache():
    roles_manager.invalidate_caches()
    if _make_fake_data:
        make_fake_data()


def make_fake_data():
    from random import seed
    seed(2)

    org_items = roles_manager.rolesdb.get_organization_items()
    program_items = roles_manager.rolesdb.get_program_items()

    def create_org(org=None, parent='Amplio', num_admins=2):
        if org is not None:
            org_email = '@{}.ra'.format(org)
            org_admins = ['{}-ad-{}{}'.format(org, x, org_email) for x in range(1, num_admins)]
            org_roles = {x: roles_manager.ADMIN_ROLES for x in org_admins}
            item = {roles_manager.ORGS_ORGANIZATION_FIELD: org, roles_manager.ORGS_PARENT_FIELD: parent,
                    roles_manager.ORGS_ROLES_FIELD: org_roles}
            org_items[org] = item

    def create_program(program=None, org=None):
        if program is not None and org is not None:
            org_email = '@{}.ra'.format(org)
            program_roles = {'p-admin{}'.format(org_email): roles_manager.ADMIN_ROLES,
                             'p-manager{}'.format(org_email): roles_manager.MANAGER_USER_ROLES,
                             'p-content{}'.format(org_email): roles_manager.CONTENT_OFFICER_ROLE}
            item = {roles_manager.PROGRAMS_PROGRAM_FIELD: program.upper(), roles_manager.PROGRAMS_ORG_FIELD: org,
                    roles_manager.PROGRAMS_ROLES_FIELD: program_roles}
            program_items[program.upper()] = item

    print('Creating fake data...')

    ras_created = 0
    affiliates_created = 0
    partners_created = 0
    start_time = time.time()

    # Create 100 regional affiliate organizations (with 3 admin users)
    num_ras = 2
    num_affiliates = 50
    num_partners = 10
    for ra_no in range(1, num_ras + 1):
        ra_name = 'ra-{}'.format(ra_no)
        create_org(ra_name, num_admins=3)
        ras_created += 1

        # Create an average of 10 affiliate organizations, each with 2 admin users
        for affiliate_no in range(1, num_affiliates + 1):
            affiliate_name = 'a-{}-{}'.format(ra_no, affiliate_no)
            create_org(affiliate_name, parent=ra_name)
            affiliates_created += 1

            # Create an average of 5 partner orgs (each with 2 admin users), each with 1 program
            for partner_no in range(1, num_partners + 1):
                partner_name = 'p-{}-{}-{}'.format(ra_no, affiliate_no, partner_no)
                create_org(partner_name, parent=affiliate_name)
                partners_created += 1

                program_name = '{}-{}-{}'.format(ra_no, affiliate_no, partner_no)
                create_program(program_name, partner_name)

    end_time = time.time()
    print('Created {} RAs, {} affiliates, {} partners & programs in {:.2g} seconds'.format(ras_created, num_affiliates,
                                                                                           num_partners,
                                                                                           end_time - start_time))


# noinspection PyUnusedLocal
def test_roles():
    users = ['bill@amplio.org', 'ryan@amplio.org', 'lisa@amplio.org', 'erin@amplio.org', 'mdakura@literacybridge.org',
             'toffic@literacybridge.org', '@amplio.org', '@literacybridge.org', 'silantoi@centreforbcc.com',
             'joel@centreforbcc.com', 'rndune@kcdmsd.rti.org']
    programs = roles_manager.rolesdb.get_program_items().keys()
    organizations = roles_manager.rolesdb.get_organization_items().keys()

    programs_to_show = ['UNICEF-CHPS', 'RTI', 'DEMO', 'CBCC-ATAY', 'ITU-NIGER']

    # print('Organizations in organizations')
    # orgs_in_orgs = get_organizations_and_dependants()
    # for org in orgs_in_orgs.values():
    #     print('{}: {}'.format(org['name'], org['dependent_orgs']))

    print('\nOrganizations for program:')
    start_time = time.time()
    num_queried = 0
    for program in programs:
        orgs = roles_manager.get_organizations_for_program(program)
        num_queried += 1
        if program in programs_to_show:
            print('  {}: {}'.format(program, orgs))
    print('Queried {} get_organizations_for_program in {:.2g} seconds'.format(num_queried, time.time() - start_time))

    print('\nRoles for program:')
    start_time = time.time()
    num_queried = 0
    for program in programs:
        roles_dict = roles_manager.get_roles_for_program(program)
        num_queried += 1
        if program in programs_to_show:
            print('  Program {}'.format(program))
            for email, roles_str in roles_dict.items():
                print('    {}: {}'.format(email, roles_str))
    print('Queried {} get_roles_for_program in {:.2g} seconds'.format(num_queried, time.time() - start_time))

    print('\nRoles for user in project')
    start_time = time.time()
    num_queried = 0
    for program in ['UNICEF-CHPS', 'CBCC-AT', 'ITU-NIGER']:
        for email in ['bill@amplio.org', 'mdakura@literacybridge.org', 'lisa@amplio.org', 'tpamba@centreforbcc.com',
                      'random@centreforbcc.com']:
            roles_str = roles_manager.get_roles_for_user_in_program(email, program)
            num_queried += 1
            if program in programs_to_show:
                if roles_str:
                    print('  Program {}'.format(program))
                    print('    {}: {}'.format(email, roles_str))

    print('Queried {} get_roles_for_user_in_program in {:.2g} seconds'.format(num_queried, time.time() - start_time))

    # for u in users:
    #     roles = get_defined_roles_for_user(u)
    #     print('{}: {}'.format(u, roles))

    # print('\nRoles for users in programs')
    # start_time = time.time()
    # num_queried = 0
    # for u in users:
    #     for p in programs:
    #         get_user_role_in_program(u, p)
    #         # print('{} in {}: {}'.format(u, p, get_user_role_in_program(u, p)))
    #         num_queried += 1
    # end_time = time.time()
    # print('Queried {} get_user_role_in_program in {:.2g} seconds'.format(num_queried, end_time - start_time))

    # print('\nRoles for users in organizations')
    # start_time = time.time()
    # num_queried = 0
    # for u in users:
    #     for o in organizations:
    #         get_user_role_in_organization(u, o)
    #         # print('{} in {}: {}'.format(u, o, get_user_role_in_organization(u, o)))
    #         num_queried += 1
    # end_time = time.time()
    # print('Queried {} get_user_role_in_organization in {:.2g} seconds'.format(num_queried, end_time - start_time))

    print('\nDefined roles for user')
    start_time = time.time()
    num_queried = 0
    for email in ['bill@amplio.org', 'mdakura@literacybridge.org']:
        roles_dict = roles_manager.get_defined_roles_for_user(email)
        num_queried += 1
        if num_queried < 5:
            print('{}: {}'.format(email, roles_dict))
    print('Queried {} get_defined_roles_for_user in {:.2g} seconds'.format(num_queried, time.time() - start_time))

    print('\nPrograms for user')
    roles_manager.invalidate_caches()
    start_time = time.time()
    num_queried = 0
    for email in ['bill@amplio.org', 'mdakura@literacybridge.org', 'silantoi@centreforbcc.com', 'hacker@evil.com']:
        program_roles = roles_manager.get_programs_for_user(email)
        num_queried += 1
        if num_queried < 5:
            programs = list(program_roles.keys())
            print('  {}: {} programs: {}'.format(email, len(programs), programs))
    print('Queried {} get_programs_for_user in {:.2g} seconds'.format(num_queried, time.time() - start_time))

    program_roles = roles_manager.get_programs_for_user('bill@amplio.org')
    max_len = max([0] + [len(x) for x in program_roles.keys()])  # don't generate an empty list
    for program, roles_dict in program_roles.items():
        print('  {:>{width}}: {}'.format(program, roles_dict, width=max_len))

    print('\nAdmin objects for user')
    roles_manager.invalidate_caches()
    start_time = time.time()
    num_queried = 0
    for email in ['bill@amplio.org', 'lindsay@amplio.org', 'silantoi@centreforbcc.com', 'tbaratier@forba.net',
                  'mdakura@literacybridge.org', 'hacker@evil.com']:
        admin_orgs, admin_progs, _ = roles_manager.get_admin_objects_for_user(email)
        jd = json.dumps({'orgs': admin_orgs, 'programs': admin_progs, 'name': email})
        num_queried += 1
        if num_queried <= 5:
            print('  {} administers {}, {}'.format(email, admin_orgs, admin_progs))
    print('Queried {} get_admin_objects_for_user in {:.2g} seconds'.format(num_queried, time.time() - start_time))

    print('\nTesting for known emails')
    roles_manager.invalidate_caches()
    start_time = time.time()
    num_queried = 0
    for email in ['bill@amplio.org', 'joe@amplio.org', 'michael@literacybridge.org', 'joe@centreforbcc.com',
                  'tbaratier@forba.net', 'hacker@evil.com']:
        is_known, via_domain = roles_manager.is_email_known(email)
        num_queried += 1
        # if num_queried < 5:
        print('  {} is{}known{}'.format(email, ' ' if is_known else ' not ', ' by domain' if via_domain else ''))
    print('Queried {} is_email_known in {:.2g} seconds'.format(num_queried, time.time() - start_time))

    random_str = 'CO,PM,*,AD,FO'
    roles_str = Roles.normalize(random_str)
    print('Normalize roles {} -> {}'.format(random_str, roles_str))
    random_str = 'CO,foo,PM,*,AD,PM'
    roles_str = Roles.normalize(random_str)
    print('Normalize roles {} -> {}'.format(random_str, roles_str))

    # print('\nRoles in programs')
    # for p in programs:
    #     print('{}: {}'.format(p, get_roles_for_program(p)))

    # print('\nRoles in organizations')
    # for o in organizations:
    #     print('{}: {}'.format(o, get_roles_for_organization(o)))


def run_main():
    global _make_fake_data

    kwargs = {
        # 'delete_tables':True,
        # 'create_tables':True,
    }
    open_tables(**kwargs)
    # load_organizations_table()
    # load_programs_table()
    # import_existing_user_roles()

    _make_fake_data = False
    roles_manager.invalidate_caches()
    test_roles()


if __name__ == '__main__':
    run_main()
