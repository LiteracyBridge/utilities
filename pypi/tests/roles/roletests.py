import json
import time

import amplio.roles.users as amplioroles

# noinspection PyUnusedLocal


_make_fake_data: bool = False


def clear_cache():
    amplioroles.invalidate_caches();
    if _make_fake_data:
        make_fake_data()


def make_fake_data():
    from random import seed
    seed(2)
    spinner = '\\|/-'

    org_items = amplioroles.get_org_items()
    program_items = amplioroles.get_program_items()

    def create_org(org=None, parent='Amplio', num_admins=2):
        if org is not None:
            org_email = '@{}.ra'.format(org)
            org_admins = ['{}-ad-{}{}'.format(org, x, org_email) for x in range(1, num_admins)]
            org_roles = {x: amplioroles.ADMIN_ROLES for x in org_admins}
            item = {amplioroles.ORGS_ORGANIZATION_FIELD: org, amplioroles.ORGS_PARENT_FIELD: parent, amplioroles.ORGS_ROLES_FIELD: org_roles}
            org_items[org] = item

    def create_program(program=None, org=None):
        if program is not None and org is not None:
            org_email = '@{}.ra'.format(org)
            program_roles = {'p-admin{}'.format(org_email): amplioroles.ADMIN_ROLES,
                             'p-manager{}'.format(org_email): amplioroles.MANAGER_USER_ROLES,
                             'p-content{}'.format(org_email): amplioroles.CONTENT_OFFICER_ROLE}
            item = {amplioroles.PROGRAMS_PROGRAM_FIELD: program.upper(), amplioroles.PROGRAMS_ORG_FIELD: org,
                    amplioroles.PROGRAMS_ROLES_FIELD: program_roles}
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


def test_roles():
    users = ['bill@amplio.org', 'ryan@amplio.org', 'lisa@amplio.org', 'erin@amplio.org', 'mdakura@literacybridge.org',
             'toffic@literacybridge.org', '@amplio.org', '@literacybridge.org', 'silantoi@centreforbcc.com',
             'joel@centreforbcc.com', 'rndune@kcdmsd.rti.org']
    programs = [x[amplioroles.PROGRAMS_PROGRAM_FIELD] for x in amplioroles.programs_table.scan()['Items']]
    organizations = [x[amplioroles.ORGS_ORGANIZATION_FIELD] for x in amplioroles.organizations_table.scan()['Items']]

    programs_to_show = ['UNICEF-CHPS', 'RTI', 'DEMO', 'CBCC-ATAY', 'ITU-NIGER']

    # print('Organizations in organizations')
    # orgs_in_orgs = get_organizations_and_dependants()
    # for org in orgs_in_orgs.values():
    #     print('{}: {}'.format(org['name'], org['dependent_orgs']))

    print('\nOrganizations for program:')
    start_time = time.time()
    num_queried = 0
    for program in programs:
        orgs = amplioroles.get_organizations_for_program(program)
        num_queried += 1
        if program in programs_to_show:
            print('  {}: {}'.format(program, orgs))
    print('Queried {} get_organizations_for_program in {:.2g} seconds'.format(num_queried, time.time() - start_time))

    print('\nRoles for program:')
    start_time = time.time()
    num_queried = 0
    for program in programs:
        roles = amplioroles.get_roles_for_program(program)
        num_queried += 1
        if program in programs_to_show:
            print('  Program {}'.format(program))
            for email, roles_str in roles.items():
                print('    {}: {}'.format(email, roles_str))
    print('Queried {} get_roles_for_program in {:.2g} seconds'.format(num_queried, time.time() - start_time))

    print('\nRoles for user in project')
    start_time = time.time()
    num_queried = 0
    for program in ['UNICEF-CHPS', 'CBCC-AT', 'ITU-NIGER']:
        for email in ['bill@amplio.org', 'mdakura@literacybridge.org', 'lisa@amplio.org', 'tpamba@centreforbcc.com',
                      'random@centreforbcc.com']:
            roles_str = amplioroles.get_roles_for_user_in_program(email, program)
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
        roles = amplioroles.get_defined_roles_for_user(email)
        num_queried += 1
        if num_queried < 5:
            print('{}: {}'.format(email, roles))
    print('Queried {} get_defined_roles_for_user in {:.2g} seconds'.format(num_queried, time.time() - start_time))

    print('\nPrograms for user')
    amplioroles.invalidate_caches()
    start_time = time.time()
    num_queried = 0
    for email in ['bill@amplio.org', 'mdakura@literacybridge.org', 'silantoi@centreforbcc.com', 'hacker@evil.com']:
        program_roles = amplioroles.get_programs_for_user(email)
        num_queried += 1
        if num_queried < 5:
            programs = list(program_roles.keys())
            print('  {}: {} programs: {}'.format(email, len(programs), programs))
    print('Queried {} get_programs_for_user in {:.2g} seconds'.format(num_queried, time.time() - start_time))

    print('\nAdmin objects for user')
    amplioroles.invalidate_caches()
    start_time = time.time()
    num_queried = 0
    for email in ['bill@amplio.org', 'lindsay@amplio.org', 'silantoi@centreforbcc.com', 'tbaratier@forba.net',
                  'mdakura@literacybridge.org', 'hacker@evil.com']:
        admin_orgs, admin_progs, _ = amplioroles.get_admin_objects_for_user(email)
        jd = json.dumps({'orgs': admin_orgs, 'programs': admin_progs, 'name': email})
        num_queried += 1
        if num_queried <= 5:
            print('  {} administers {}, {}'.format(email, admin_orgs, admin_progs))
    print('Queried {} get_admin_objects_for_user in {:.2g} seconds'.format(num_queried, time.time() - start_time))

    print('\nTesting for known emails')
    amplioroles.invalidate_caches()
    start_time = time.time()
    num_queried = 0
    for email in ['bill@amplio.org', 'joe@amplio.org', 'michael@literacybridge.org', 'joe@centreforbcc.com',
                  'tbaratier@forba.net', 'hacker@evil.com']:
        is_known, via_domain = amplioroles.is_email_known(email)
        num_queried += 1
        # if num_queried < 5:
        print('  {} is{}known{}'.format(email, ' ' if is_known else ' not ', ' by domain' if via_domain else ''))
    print('Queried {} is_email_known in {:.2g} seconds'.format(num_queried, time.time() - start_time))

    # print('\nRoles in programs')
    # for p in programs:
    #     print('{}: {}'.format(p, get_roles_for_program(p)))

    # print('\nRoles in organizations')
    # for o in organizations:
    #     print('{}: {}'.format(o, get_roles_for_organization(o)))


def run_main():
    global _make_fake_data

    # delete_tables()
    # create_tables()
    amplioroles.open_tables()
    # load_organizations_table()
    # load_programs_table()
    # import_existing_user_roles()

    _make_fake_data = False
    amplioroles.invalidate_caches()
    test_roles()


if __name__ == '__main__':
    run_main()
