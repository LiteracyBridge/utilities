import json
import sys
import time
import traceback
from typing import Tuple, List, Union

import amplio.rolemanager.manager as roles_manager
from amplio.rolemanager.Roles import *

"""
Management of users and their roles within TB programs.

"""

roles_manager.open_tables()

STATUS_OK = 'ok'
STATUS_FAILURE = 'failure'
STATUS_EXTRA_PARAMETER = 'Extraneous parameter'
STATUS_ACCESS_DENIED = 'Access denied'
STATUS_MISSING_PARAMETER = 'Missing parameter'

DEFAULT_REPOSITORY = 'dbx'


def get_program_info_for_user(email: str) -> Tuple[Dict[str, Dict[str, str]], str]:
    # Start with the user's roles in programs, because that gets the list of relevant programs
    programs_and_roles: Dict[str, str] = roles_manager.get_programs_for_user(email)
    result: Dict[str, Dict[str, str]] = {p: {'roles': r} for p, r in programs_and_roles.items()}
    programids: List[str] = [x for x in result.keys()]

    # Add the friendly name, and collect repository info.
    repository_programs: Dict[str, List[str]] = {}  # list of programs in each repository.
    programs_table_items = roles_manager.rolesdb.get_program_items()  # programs_table.scan()['Items']
    for programid in programids:
        item = programs_table_items.get(programid)
        program_repository = item.get('repository', DEFAULT_REPOSITORY).lower()
        list = repository_programs.setdefault(program_repository, [])
        list.append(programid)
        result[programid]['name'] = item.get('program_name') or programid

    # Find the repository with the most programs, and make that the implicit repository.
    implicit_repo: Union[str,None] = None
    max_prog = -1
    for repo, program_list in repository_programs.items():
        if len(program_list) > max_prog:
            implicit_repo = repo
            max_prog = len(program_list)
    if implicit_repo:
        del repository_programs[implicit_repo]
    # Invert the repositories list into the result
    for repo, program_list in repository_programs.items():
        for programid in program_list:
            result[programid]['repository'] = repo
    return (result, implicit_repo)


def do_get_programs(claims):
    email = claims.get('email')
    program_info, implicit_repo = get_program_info_for_user(email)

    return {  # 'output': program_roles,
        'status': STATUS_OK,
        'programs': program_info,
        # 'repository': default_repository
        'implicit_repository': implicit_repo
    }


def do_get_admin_objects(claims):
    email = claims.get('email')
    admin_orgs, admin_progs, _ = roles_manager.get_admin_objects_for_user(email)
    return {'output': {'orgs': admin_orgs, 'programs': admin_progs, 'name': email}, 'status': STATUS_OK}


def do_update_roles(claims, updates):
    print('updates: {}'.format(updates))
    email = claims.get('email')
    old = updates.get('old', {})
    new = updates.get('new', {})
    name = updates.get('name')
    obj_type = updates.get('type')
    if obj_type == 'program':
        roles = roles_manager.get_roles_for_user_in_program(email, name)
    elif obj_type == 'org':
        roles = roles_manager.get_roles_for_user_in_organization(email, name)
    else:
        return {'status': STATUS_MISSING_PARAMETER}
    if not (SUPER_USER_ROLE in roles or ADMIN_ROLE in roles):
        return {'status': STATUS_ACCESS_DENIED}

    if obj_type == 'program':
        roles_manager.update_roles_for_program(name, old, new)
    else:
        roles_manager.update_roles_for_organization(name, old, new)

    return {'status': STATUS_OK}


# noinspection PyUnusedLocal,PyBroadException
def lambda_handler(event, context):
    start = time.time_ns()

    result = {'output': [],
              'status': ''}

    # print('Event path parameters: {}'.format(event.get('pathParameters')))
    parts = [x for x in event.get('pathParameters', {}).get('proxy', 'reserve').split('/') if x != 'data']
    action = parts[0].lower()
    body = event.get('body')
    print(str(body))
    try:
        data = json.loads(body)
        # print('body length {}, data: {}'.format(len(body), body))
    except Exception as ex:
        data = ''
        # print('exception getting body length')

    print('Action: {}'.format(action))

    path = event.get('path', {})
    path_parameters = event.get('pathParameters', {})
    query_string_params = event.get('queryStringParameters', {})
    # print('Query string parameters: {}'.format(query_string_params))

    claims = event.get('requestContext', {}).get('authorizer', {}).get('claims', {})

    try:
        if action == 'getprograms':
            result = do_get_programs(claims=claims)

        elif action == 'getadminobjects':
            result = do_get_admin_objects(claims=claims)

        elif action == 'updateroles':
            result = do_update_roles(claims=claims, updates=data)

    except Exception as ex:
        traceback.print_exception(type(ex), ex, ex.__traceback__)
        result['status'] = STATUS_FAILURE
        result['exception'] = 'Exception: {}'.format(ex)

    end = time.time_ns()
    print(f'Action: {action}, claims: {claims}, data: {data}, result: {result}, time {(end - start) / 1000000} msec.')

    return {
        'statusCode': 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        'body': json.dumps({'msg': 'Role Helper Utility',
                            'result': result,
                            'claims': claims,
                            'action': action,
                            'path': path,
                            'path_parameters': path_parameters,
                            'query_string_params': query_string_params,
                            'msec': (end - start) / 1000000})
    }


# region Testing Code
if __name__ == '__main__':
    def test():
        def test_get_programs(email=None):
            if email is None:
                email = claims['email']

            submit_event = {'requestContext': {
                'authorizer': {'claims': {'email': email, 'edit': claims['edit'], 'view': claims['view']}}},
                'pathParameters': {'proxy': 'getprograms'},
                'queryStringParameters': {}
            }
            result = lambda_handler(submit_event, {})
            x = json.loads(result['body']).get('result', {})
            return x

        def test_get_admin_objects(email=None):
            if email is None:
                email = claims['email']

            submit_event = {'requestContext': {
                'authorizer': {'claims': {'email': email, 'edit': claims['edit'], 'view': claims['view']}}},
                'pathParameters': {'proxy': 'getAdminObjects'},
                'queryStringParameters': {}
            }
            result = lambda_handler(submit_event, {})
            x = json.loads(result['body']).get('result', {})
            return x

        claims = {'edit': '.*', 'view': '.*', 'email': 'bill@amplio.org'}
        print('Just testing')

        roles_result = test_get_programs()
        print(roles_result)
        roles_result = test_get_programs(email='mdakura@literacybridge.org')
        print(roles_result)
        print(test_get_programs(email='hacker@evil.com'))
        print(test_get_admin_objects(email='bill@amplio.org'))

        start = time.time_ns()
        pi, implicit_repo = get_program_info_for_user('bill@amplio.org')
        end = time.time_ns()
        print(f'Query took {(end - start) / 1000000} ms: {pi}')
        print(implicit_repo)


    def _main():
        test()


    sys.exit(_main())
# endregion
