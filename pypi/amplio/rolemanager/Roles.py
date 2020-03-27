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


# Given one or more strings of comma-separated roles, returns a single string of unique comma-separated roles.
def normalize(*args: str) -> str:
    # result_set = set()
    roles = set()
    for arg in args:
        if not arg:
            continue
        roles.update([x.upper() for x in [x.strip() for x in arg.split(',')] if x and x in ROLES])

    roles_list = sorted(roles, key=lambda x: ROLES.index(x))
    return ','.join(roles_list)
