import asyncio
import concurrent.futures
import json
import re
import sys
import time
import traceback
from typing import Tuple, List, Union, Dict, Pattern

import amplio.rolemanager.manager as roles_manager

dir(roles_manager)
# from amplio.rolemanager.manager import open_tables
# import amplio.rolemanager.manager as roles_manager
from amplio.rolemanager.Roles import *
from amplio.utils import *

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


def _bool_arg(arg, default=False):
    """
    Given a value that may be True/False or may be a [sub-]string 'true'/'false', return the truth value.
    :param arg: May contain a truth value.
    :param default: If the argument can't be interpreted as a boolean, use this default.
    :return: the True or False
    """
    if type(arg) == bool:
        return arg
    elif arg is None:
        return default
    try:
        val = str(arg).lower()
        if val in ('y', 'yes', 't', 'true', 'on', '1'):
            return 1
        elif val in ('n', 'no', 'f', 'false', 'off', '0'):
            return 0
        else:
            return default
    except ValueError:
        return default


def get_program_info_for_user(email: str) -> Tuple[Dict[str, Dict[str, str]], str]:
    # Start with the user's roles in programs, because that gets the list of relevant programs
    # {program: roles}
    programs_and_roles: Dict[str, str] = roles_manager.get_programs_for_user(email)
    # {program: {'roles': roles}}
    result: Dict[str, Dict[str, str]] = {p: {'roles': r} for p, r in programs_and_roles.items()}
    programids: List[str] = [x for x in result.keys()]

    # Add the friendly name, and collect repository info.
    # {repo: [prog1, prog2, ...]}
    repository_programs: Dict[str, List[str]] = {}  # list of programs in each repository.
    programs_table_items = roles_manager.rolesdb.get_program_items()  # programs_table.scan()['Items']
    for programid in programids:
        item = programs_table_items.get(programid)
        program_repository = item.get('repository', DEFAULT_REPOSITORY).lower()
        list = repository_programs.setdefault(program_repository, [])
        list.append(programid)
        # add {'name': program_name} as {program: {'roles':roles, 'name':program_name}}
        result[programid]['name'] = item.get('program_name') or programid

    # Find the repository with the most programs, and make that the implicit repository.
    implicit_repo: Union[str, None] = None
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
            # add {'repository':repository} to {program: {'roles':roles, ...}}
            result[programid]['repository'] = repo
    return (result, implicit_repo)


s3 = None

def _add_deployment_revs(program_info: Dict[str, Dict], implict_repo: str, use_async: bool = False) -> None:
    """
    Given program_info ({program: {'name':name, 'roles':roles}}) and the default repository, add
    the most recent deployment rev to the program_info.
    :param program_info: The program_info to which deployment revs are to be added.
    :type program_info: Dict[str,Dict[str,str]]
    :param implict_repo: The repo for any program without an explicitly defined repo. ('dbx' or 's3')
    :type implict_repo: str
    :return: None
    """
    def get_revs(pattern:Pattern, bucket: str, prefix:str) -> Dict[str,str]:
        """
        Given a bucket and a prefix, and a pattern for matching files, find the contained .rev files.
        :param pattern: Pattern for matching deployment.rev files from a full object name. The pattern
                should define group(1) as the program name and group(2) as the revision.
        :param bucket: The S3 bucket to be examined.
        :param prefix: Prefix within the bucket, to restrict the search to more relevant objects.
        :return: A dict{str:str} of {program:deployment.rev}
        """
        def _list_objects(Bucket: str, Prefix=''):
            paginator = s3_client.get_paginator("list_objects_v2")
            kwargs = {'Bucket': Bucket, 'Prefix': Prefix}
            for objects in paginator.paginate(**kwargs):
                for obj in objects.get('Contents', []):
                    yield obj
        # print(f'Getting objects in {bucket} / {prefix}')
        s3_client = s3 or boto3.client('s3')
        result: Dict[str,str] = {}
        for object in _list_objects(Bucket=bucket, Prefix=prefix):
            if matcher := pattern.match(object.get('Key')):
                program = matcher.group(1)
                result[program] = matcher.group(2)
        # print(f'Got {len(result)} objects from {bucket} / {prefix}')
        return result

    global s3
    import boto3
    if s3 is None:
        s3 = boto3.client('s3')

    dbx_rev_pattern = re.compile(r'(?i)^projects/([a-z0-9_-]+)/([a-z0-9_-]+)\.(?:current|rev)$')
    s3_rev_pattern = re.compile(r'(?i)^([a-z0-9_-]+)/TB-Loaders/published/([a-z0-9_-]+)\.rev$')
    dbx_programs = [prog for prog, info in program_info.items() if info.get('repository', implict_repo) == 'dbx']
    s3_programs = [prog for prog, info in program_info.items() if info.get('repository', implict_repo) != 'dbx']

    if (dbx_programs):
        # enumerate all of the deployments for dropbox hosted programs
        for program,rev in get_revs(dbx_rev_pattern, bucket='acm-content-updates', prefix='projects').items():
            if program in dbx_programs:
                program_info[program]['deployment_rev'] = rev

    def make_s3_getter(the_program):
        def fn():
            return get_revs(s3_rev_pattern, bucket='amplio-program-content', prefix=f'{the_program}/TB-Loaders/published')
        return fn

    if use_async:
        """
        If the caller requested async operation, run S3 queries in parallel.
        """
        async def non_blocking(executor):
            loop = asyncio.get_event_loop()
            blocking_tasks = []
            for s3_program in s3_programs:
                blocking_tasks.append(loop.run_in_executor(executor, make_s3_getter(s3_program) ))
            completed, pending = await asyncio.wait(blocking_tasks)
            results = [t.result() for t in completed]
            return results

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=20)
        event_loop = asyncio.get_event_loop()
        non_blocking_results = event_loop.run_until_complete(non_blocking(executor))
        print(non_blocking_results)
        for rm in non_blocking_results:
            for p,r in rm.items():
                if p in s3_programs:
                    program_info[p]['deployment_rev'] = r
    else:
        for s3_program in s3_programs:
            # enumerate published deployments for s3 hosted programs
            for program,rev in make_s3_getter(s3_program)().items():
                program_info[program]['deployment_rev'] = rev


@handler(roles=None, action='getprograms')
def do_get_programs(email: Claim, depls: QueryStringParam = False, use_async: QueryStringParam = False):
    add_deployments = _bool_arg(depls)
    use_async = _bool_arg(use_async)
    program_info, implicit_repo = get_program_info_for_user(email)
    if add_deployments:
        _add_deployment_revs(program_info, implicit_repo, use_async)

    return {'result': {  # 'output': program_roles,
        'status': STATUS_OK,
        'programs': program_info,
        'implicit_repository': implicit_repo
    }}


@handler(roles=None, action='getadminobjects')
def do_get_admin_objects(email: Claim):
    admin_orgs, admin_progs, _ = roles_manager.get_admin_objects_for_user(email)
    return {'result': {'output': {'orgs': admin_orgs, 'programs': admin_progs, 'name': email}, 'status': STATUS_OK}}


@handler(roles=None, action='updateroles')
def do_update_roles(email: Claim, updates: JsonBody):
    print('updates: {}'.format(updates))
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

    return {'result': {'status': STATUS_OK}}


#TODO: Delete the obsolete handler
# noinspection PyUnusedLocal,PyBroadException
# def lambda_handler0(event, context):
#     start = time.time_ns()
#
#     result = {'output': [],
#               'status': ''}
#
#     # print('Event path parameters: {}'.format(event.get('pathParameters')))
#     parts = [x for x in event.get('pathParameters', {}).get('proxy', 'reserve').split('/') if x != 'data']
#     action = parts[0].lower()
#     body = event.get('body')
#     print(str(body))
#     try:
#         data = json.loads(body)
#         # print('body length {}, data: {}'.format(len(body), body))
#     except Exception as ex:
#         data = ''
#         # print('exception getting body length')
#
#     print('Action: {}'.format(action))
#
#     path = event.get('path', {})
#     path_parameters = event.get('pathParameters', {})
#     query_string_params = event.get('queryStringParameters', {})
#     # print('Query string parameters: {}'.format(query_string_params))
#
#     claims = event.get('requestContext', {}).get('authorizer', {}).get('claims', {})
#     email = claims.get('email', None)
#
#     try:
#         if action == 'getprograms':
#             result = do_get_programs(email=email, depls=query_string_params.get('depls'))
#
#         elif action == 'getadminobjects':
#             result = do_get_admin_objects(email=email)
#
#         elif action == 'updateroles':
#             result = do_update_roles(email=email, updates=data)
#
#     except Exception as ex:
#         traceback.print_exception(type(ex), ex, ex.__traceback__)
#         result['status'] = STATUS_FAILURE
#         result['exception'] = 'Exception: {}'.format(ex)
#
#     end = time.time_ns()
#     print(f'Action: {action}, claims: {claims}, data: {data}, result: {result}, time {(end - start) / 1000000} msec.')
#
#     return {
#         'statusCode': 200,
#         "headers": {"Access-Control-Allow-Origin": "*"},
#         'body': json.dumps({'msg': 'Role Helper Utility',
#                             'result': result.get('result', result), # embedded result if there is one
#                             'claims': claims,
#                             'action': action,
#                             'path': path,
#                             'path_parameters': path_parameters,
#                             'query_string_params': query_string_params,
#                             'msec': (end - start) / 1000000})
#     }


def lambda_handler(event, context):
    the_router = LambdaRouter(event, context)
    action = the_router.path_param(0).lower()  # or however else the action is determined.
    return the_router.dispatch(action)


# region Testing Code
if __name__ == '__main__':
    def test1():
        def test_get_programs(email=None, depls: bool = False):
            if email is None:
                email = claims['email']

            submit_event = {'requestContext': {
                'authorizer': {'claims': {'email': email, 'edit': claims['edit'], 'view': claims['view']}}},
                'pathParameters': {'proxy': 'getprograms'},
                'queryStringParameters': {}
            }
            if depls:
                submit_event['queryStringParameters']['depls']='t'
            start = time.time_ns()
            result = lambda_handler(submit_event, {})
            end = time.time_ns()
            print(f'get_programs sync Query took {(end - start) / 1000000} ms, depls={depls}')
            x = json.loads(result['body']).get('result', {})

            if depls:
                submit_event['queryStringParameters']['use_async'] = 't'
                start = time.time_ns()
                result2 = lambda_handler(submit_event, {})
                end = time.time_ns()
                print(f'get_programs async Query took {(end - start) / 1000000} ms, depls={depls}')
                x2 = json.loads(result2['body']).get('result', {})
                if x != x2:
                    print('Sync vs async results differ')

            # result0 = lambda_handler0(submit_event, {})
            # x0 = json.loads(result0['body']).get('result', {})

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

        roles_result = test_get_programs(depls=True)
        print(roles_result)

        roles_result = test_get_programs(email='mdakura@literacybridge.org')
        print(roles_result)

        print(test_get_programs(email='hacker@evil.com'))
        print(test_get_admin_objects(email='bill@amplio.org'))

        start = time.time_ns()
        pi, implicit_repo = get_program_info_for_user('bill@amplio.org')
        end = time.time_ns()
        print(f'get_program_info_for_user Query took {(end - start) / 1000000} ms: {pi}')
        print(implicit_repo)


    def _main():
        test1()


    sys.exit(_main())
# endregion
