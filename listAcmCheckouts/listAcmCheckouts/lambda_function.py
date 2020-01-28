import json
import os
import re
import sys
import time
import traceback

import boto3

"""
Management of TB-Loader ids and serial numbers.

TB-Loaders table
    Key is email name of the TB-Loader user. Obviously, these must be unique.
    The "tbloaderid" value is the TB-Loader ID assigned to that email address.
    The "reserved" value is the highest reserved serial number. When a TB-Loader
    needs more serial numbers, it calls the "reserve" function. That function
    reads the current reserved value, adds the number being allocated, and
    writes the new value. The range thus reserved is returned to the caller.
    
    In the event that the email is not in the table, a new TB-Loader Id is 
    allocated. This is kept in a special, invalid email address, 
    max-tbloader@@amplio.org. The value "maxtbloader" is read, and incremented.
    The requesting email address is written, with the newly assigned tbloaderid,
    and a reserved value of 0.  
"""
ACM_CHECKOUT_TABLE = 'acm_check_out'

dynamodb_client = boto3.client('dynamodb')  # specify amazon service to be used
dynamodb_resource = boto3.resource('dynamodb')

acm_checkout_table = None

STATUS = 'status'
MESSAGE = 'message'
EXCEPTION = 'exception'
STATUS_OK = 'ok'
STATUS_FAILURE = 'failure'

def open_tables():
    global acm_checkout_table
    acm_checkout_table = dynamodb_resource.Table(ACM_CHECKOUT_TABLE)

# Given a project or ACM name, return just the project name part, uppercased. ACM-TEST -> TEST, test -> TEST
def cannonical_acm_project_name(acmdir):
    if acmdir is None:
        return None
    _, acm = os.path.split(acmdir)
    acm = acm.upper()
    if acm.startswith('ACM-'):
        acm = acm[4:]
    return acm

def _has_access(claims, project):
    if 're' in claims:
        regex = claims.get('re')
    else:
        view = claims.get('view')
        edit = claims.get('edit')
        re_str = view or ''
        if re_str and edit:
            re_str += '|'
        re_str += (edit or '')
        regex = re.compile(re_str, re.IGNORECASE)
        claims['re'] = regex
    return regex.match(project)

def do_query(params, claims):
    result = {}
    email = claims.get('email')

    acms = []
    checkouts =  acm_checkout_table.scan()['Items']
    for checkout in checkouts:
        if _has_access(claims, cannonical_acm_project_name(checkout.get('acm_name'))):
            acms.append(checkout)


    # We return the old value. The caller can use old_value to old_value + n
    result = {STATUS: STATUS_OK, 'acms': acms}
    return result


def lambda_handler(event, context):
    global acm_checkout_table
    start = time.time_ns()
    if acm_checkout_table is None:
        open_tables()

    result = {'output': [],
              'status': ''}

    print('Event path parameters: {}'.format(event.get('pathParameters')))
    parts = [x for x in event.get('pathParameters', {}).get('proxy', 'reserve').split('/') if x != 'data']
    action = parts[0]
    print('Action: {}'.format(action))

    path = event.get('path', {})
    path_parameters = event.get('pathParameters', {})
    query_string_params = event.get('queryStringParameters', {})
    print('Query string parameters: {}'.format(query_string_params))

    claims = event.get('requestContext', {}).get('authorizer', {}).get('claims', {})


    try:
        result = do_query(query_string_params, claims)
        result = result.get('acms', [])
    except Exception as ex:
        traceback.print_exception(type(ex), ex, ex.__traceback__)
        result['status'] = STATUS_FAILURE
        result['exception'] = 'Exception: {}'.format(ex)

    print('Result: {}'.format(result))
    end = time.time_ns()

    return {
        'statusCode': 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        'body': result
    }


# region Testing Code
if __name__ == '__main__':
    def test():
        def test_reserve(email=None):
            if email is None:
                email = claims['email']
            submit_event = {'requestContext': {
                'authorizer': {'claims': {'email': email, 'edit': claims['edit'], 'view': claims['view']}}},
                'queryStringParameters': {}
            }
            result = lambda_handler(submit_event, {})
            return result

        claims = {'edit': '.*', 'view': '.*', 'email': 'bill@amplio.org'}
        print('Just testing')

        reserve_result = test_reserve()
        print(reserve_result)

        claims = {'edit': '', 'view': 'demo', 'email': 'bill@amplio.org'}
        reserve_result = test_reserve('demo@amplio.org')
        print(reserve_result)


    def _main():
        open_tables()
        test()


    sys.exit(_main())
# endregion
