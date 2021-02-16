import hashlib
from datetime import datetime
from typing import List, Dict

import boto3
from amplio.rolemanager import manager

REGION_NAME = 'us-west-2'

USERS_TABLE_NAME = 'acm_users'
PROGRAMS_TABLE_NAME = 'programs'
dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
users_table = dynamodb.Table(USERS_TABLE_NAME)
programs_table = dynamodb.Table(PROGRAMS_TABLE_NAME)

USER_POOL_ID = 'us-west-2_6EKGzq75p'
cognito_client = boto3.client('cognito-idp')

manager.open_tables()

WARNING_DATE = None
OLD_POOL_ID = 'us-west-2_6EKGzq75p'

def _email_from_event(event: dict) -> str:
    """
    Given a Cognito Trigger event, return the user's email address.
    """
    try:
        request = event['request']
        user_attributes = request['userAttributes']
        email = user_attributes['email'].lower()
    except Exception as e:
        print('Exception retrieving email:  ' + str(e))
        print('Event: ' + str(event))
        raise e

    return email

def get_configured_repositories(programs: List[str], default: str = 'dbx', repositories=['s3']) -> Dict[str,List[str]]:
    """
    Given a list of programs, find the ones that do not use the default repository.
    """
    default = default.lower()
    if default != 'dbx':
        print('Warning! get_configured_repositories called with non-standard default repository: {}'.format(default))
    result = {}
    program_items = programs_table.scan()['Items']
    for item in program_items:
        program = item.get('program')
        repository = item.get('repository', default).lower()
        if program in programs and repository in repositories:
            list = result.setdefault(repository, [])
            list.append(program)
    return result


def get_user_access(email, user_pool_id=None):
    """
    Given a user's email address, return their access as given in the acm_users table.
    Note that the access may be given to all users at the domain.
    """
    # First look up by user's full email address.
    acm_user = users_table.get_item(Key={'email': email})
    user_info = acm_user.get('Item')
    if not user_info:
        # Nothing specific to the user; look up by email domain.
        parts = email.split('@')
        if len(parts) != 2:
            # This should never happen, because a user needs a valid email address to sign up.
            raise Exception("'{}' is not a valid email address.".format(email))
        org = parts[1]
        acm_user = users_table.get_item(Key={'email': org})
        user_info = acm_user.get('Item')
        # If the user's organization isn't in the table, give them no access at all.
        if not user_info:
            user_info = {'edit': '', 'view': '', 'admin': False}

    # program:roles;program:roles...
    #programs = ';'.join([p+':'+r for p,r in manager.get_programs_for_user(email).items()])
    programs_for_user = manager.get_programs_for_user(email)
    # "s3:TEST;dbx:DEMO,XTEST-2"
    configured_repositories = ';'.join(f'{k}:{",".join(v)}' for k,v in get_configured_repositories(list(programs_for_user.keys())).items())
    print('Repositories: {}'.format(configured_repositories))
    programs = ';'.join([p + ':' + r for p, r in programs_for_user.items()])

    try:
        md5 = hashlib.md5()
        md5.update(email.lower().encode('utf-8'))
        hash = md5.hexdigest()
    except:
        hash=None
    user_access = {'edit': user_info.get('edit', ''),
                   'view': user_info.get('view', ''),
                   'admin': user_info.get('admin', False),
                   'programs': programs
                   }
    if configured_repositories:
        user_access['repositories'] = configured_repositories
    if hash:
        user_access['hash'] = hash
    # Warning to create new ID? Use Message-Of-Day facility.
    if user_pool_id == OLD_POOL_ID:
        global WARNING_DATE
        if not WARNING_DATE:
            WARNING_DATE = datetime(2021,1,25,0,0,0,0)
        if datetime.now() > WARNING_DATE:
            user_access['mod'] = 'Your sign-in was successful, and you may proceed.\n'+\
                                 'However, due to necessary software maintenance, you\n'+\
                                 'need to create a new Amplio ID by 2021-February-1.\n\n'+\
                                 'Contact support@amplio.org if you need assistance.'
            user_access['modButton'] = "Proceed"

    print('Access for user {}: {}'.format(email, user_access))
    return user_access


# noinspection PyPep8Naming
def was_email_already_confirmed_to_user(email: str, UserPoolId: str = USER_POOL_ID) -> tuple:
    """
    Look up an email address to see if it belongs to a confirmed user. If so, also return
    the username.
    """
    response = cognito_client.list_users(
        UserPoolId=UserPoolId,
        AttributesToGet=[],
        Limit=0,
        Filter='email = "' + email + '"'
    )
    users = response.get('Users', [])
    exists = len(users) == 1 and users[0].get('UserStatus', '') == 'CONFIRMED'
    user = users[0].get('Username', '') if len(users) == 1 else ''
    return exists, user


def pre_signup_handler(event):
    """
    Validates that a user is not already signed up, and is allowed to sign up.
    """
    email = _email_from_event(event)
    user_pool = event.get('userPoolId', USER_POOL_ID)
    print(f"Pre-signup handler checking validity for email address '{email}'.")

    # Is the email address *already* confirmed to a user?
    confirmed, user = was_email_already_confirmed_to_user(email, UserPoolId=user_pool)
    if confirmed:
        print("pre-signup event for existing user '{}' with (new) email '{}'. User pool '{}'. Event: '{}'"
              .format(user,
                      email,
                      user_pool,
                      event))
        raise Exception("That email address is in use by user '{}'.".format(user))

    # The email address isn't yet claimed. Validate the email address should have access.
    is_known, by_domain = manager.is_email_known(email)
    if not is_known:
        print("pre-signup event for unrecognized email '{}'. Event: '{}'".format(email, event))
        raise Exception("'{}' is not authorized".format(email))

    print("pre-signup event success for email '{}'. Event: '{}'".format(email, event))


def token_generation_handler(event):
    """
    Adds the user's access to the claims returned to the client application.
    """
    if "response" not in event:
        event["response"] = {}
    email = _email_from_event(event)
    user_pool_id = event.get('userPoolId')
    event["response"]["claimsOverrideDetails"] = {
        "claimsToAddOrOverride": get_user_access(email, user_pool_id)
    }


# noinspection PyUnusedLocal
def lambda_handler(event, context):
    print(event)
    trigger = event.get('triggerSource', '')
    if trigger == 'TokenGeneration_Authentication' or trigger == 'TokenGeneration_RefreshTokens' or trigger.startswith('TokenGeneration'):
        token_generation_handler(event)
    elif trigger == 'PreSignUp_SignUp':
        pre_signup_handler(event)

    return event


# region Testing Code
if __name__ == '__main__':
    import sys
    def simulate(trigger: str, email: str):
        event = {'triggerSource': trigger, 'request': {'userAttributes': {'email': email}}}
        result = None
        try:
            result = lambda_handler(event, None)
        except Exception as ex:
            result = {'exception': str(ex)}
        return result


    def test():
        def get_access(email):
            ua = get_user_access(email)
            return ua

        rc_list = []

        # Should not be able to sign up existing user
        result = simulate('PreSignUp_SignUp', 'demo@literacybridge.org')
        print(result)
        rc = 0 if not result else 1
        rc_list.append(rc)

        # Should be able to sign up new user at known domain
        result = simulate('PreSignUp_SignUp', 'NaNaHeyHeyGoodBy@amplio.org')
        print(result)
        rc = 0 if result else 1
        rc_list.append(rc)

        # Should not be able to sign up random user at random domain.
        result = simulate('PreSignUp_SignUp', 'me@example.com')
        print(result)
        rc = 0 if not result else 1
        rc_list.append(rc)

        # Should get claims for known user.
        result = simulate('TokenGeneration_Authentication', 'demo@literacybridge.org')
        view = result.get('response', {}) \
            .get('claimsOverrideDetails', {}) \
            .get('claimsToAddOrOverride', {}) \
            .get('view', '')
        rc = 0 if 'DEMO' in view else 1
        rc_list.append(rc)

        access = get_access('demo@amplio.org')
        print(access)
        rc = 0 if access['edit'] == '' and access['view'] == 'DEMO' and access['admin'] == 'false' else 1
        rc_list.append(rc)

        access = get_access('@amplio.org')
        print(access)
        rc = 0 if access['view'] == '.*' and access['admin'] == 'false' else 1
        rc_list.append(rc)

        programs = ['TEST', 'XTEST-2', 'DEMO']
        repos = ['s3', 'dbx']
        configured = get_configured_repositories(programs, repositories=repos)
        print("Configured '{}': {}".format(repos, configured))
        repos = ['s3']
        configured = get_configured_repositories(programs, repositories=repos)
        print("Configured '{}': {}".format(repos, configured))
        repos = ['dbx']
        configured = get_configured_repositories(programs, repositories=repos)
        print("Configured '{}': {}".format(repos, configured))
        repos = None
        configured = get_configured_repositories(programs)
        print("Configured '{}': {}".format(repos, configured))

        configured = get_configured_repositories(programs, 's3')
        print("Configured '{}': {}", 's3', configured)
        configured = get_configured_repositories(programs, 'dbx')
        print("Configured '{}': {}", 'dbx', configured)

        access = get_access('bill@amplio.org')
        print(access)

        return max(rc_list)


    def _main():
        rc = test()
        return rc


    sys.exit(_main())
# endregion
