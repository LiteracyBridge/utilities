import json
from datetime import datetime
from datetime import datetime
from typing import List, Dict, Iterable, Tuple

import boto3

REGION_NAME = 'us-west-2'
USERS_TABLE_NAME = 'acm_users'
PROGRAMS_TABLE_NAME = 'programs'

USER_POOL_ID = 'us-west-2_3evpQGyi5'
cognito_client = None



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


# noinspection PyPep8Naming
def was_email_already_confirmed_to_user(email: str, UserPoolId: str = USER_POOL_ID) -> tuple:
    """
    Look up an email address to see if it belongs to a confirmed user. If so, also return
    the username.
    """
    global cognito_client
    if cognito_client is None:
        cognito_client = boto3.client('cognito-idp')
    response = cognito_client.list_users(
        UserPoolId=UserPoolId,
        AttributesToGet=['name'],
        Limit=0,
        Filter='email = "' + email + '"'
    )
    users = response.get('Users', [])
    exists = False
    user = ''
    if len(users) == 1:
        attributes = {x['Name']:x['Value'] for x in users[0].get('Attributes', {})}
        exists = users[0].get('UserStatus', '') == 'CONFIRMED'
        user = attributes.get('name') or users[0].get('Username') or ''
    return exists, user


def pre_signup_handler(event):
    """
    Validates that a user is not already signed up, and is allowed to sign up.
    """
    from amplio.rolemanager import manager
    manager.open_tables()

    email = _email_from_event(event)
    user_pool = event.get('userPoolId', USER_POOL_ID)
    print(f"Pre-signup handler checking validity for email address '{email}'.")

    # Is the email address *already* confirmed to a user?
    already_in_use, user = was_email_already_confirmed_to_user(email, UserPoolId=user_pool)
    if already_in_use:
        print(f"pre-signup event for existing user '{user}' with (new) email '{email}'. User pool '{user_pool}'. Event: '{event}'")
        raise Exception(f"The email address '{email}' is in use by user '{user}'.")

    # The email address isn't yet claimed. Validate the email address should have access.
    is_known, by_domain = manager.is_email_known(email)
    if not is_known:
        print(f"pre-signup event for unrecognized email '{email}'. Event: '{event}'")
        raise Exception(f"'{email}' is not authorized")

    print(f"pre-signup event success for email '{email}'. Event: '{event}'")


def token_generation_handler(event):
    """
    Adds the user's access to the claims returned to the client application.
    """
    # De-implemented on 2022-01-12. We no longer return any additional data in claims.
    # Applications must make another call to get the user's programs. This provides better
    # scaling, and substantially reduces the size of the authorization token.
    # if "response" not in event:
    #     event["response"] = {}
    # email = _email_from_event(event)
    # user_pool_id = event.get('userPoolId')
    # event["response"]["claimsOverrideDetails"] = {
    #     "claimsToAddOrOverride": get_user_access(email, user_pool_id)
    # }

    # If we ever want to bring back the Message-Of-the-Day facility, re-configure the trigger for
    # TokenGeneration_Authentication, and add code here something like this:
    # Warning to create new ID? Use Message-Of-Day facility.
    #     global WARNING_DATE
    #     if not WARNING_DATE:
    #         WARNING_DATE = datetime(2021, 1, 25, 0, 0, 0, 0)
    #     if datetime.now() > WARNING_DATE:
    #         user_access['mod'] = 'Your sign-in was successful, and you may proceed.\n' + \
    #                              'However, due to necessary software maintenance, you\n' + \
    #                              'need to create a new Amplio ID by 2021-February-1.\n\n' + \
    #                              'Contact support@amplio.org if you need assistance.'
    #         user_access['modButton'] = "Proceed"


def pre_authentication_handler(event):
    """
    Called early in the authentication process, which may still fail. This is an opportunity to log,
    for instance, event['validationData'], which is a {k:v} of usermetadata from the application.
    """
    pass


# noinspection PyUnusedLocal
def lambda_handler(event, context):
    print(event)
    trigger = event.get('triggerSource', '')
    # If we ever want Message-Of-the-Day capability, re-implement this:
    # if trigger == 'TokenGeneration_Authentication' or trigger == 'TokenGeneration_RefreshTokens' or trigger.startswith(
    #         'TokenGeneration'):
    #     token_generation_handler(event)
    # el
    if trigger == 'PreSignUp_SignUp':
        pre_signup_handler(event)
    elif trigger == 'PreAuthentication_Authentication':
        pre_authentication_handler(event)

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
        rc_list = []

        # Should not be able to sign up existing user
        result = simulate('PreSignUp_SignUp', 'amplio.demo@gmail.com')
        print(result)
        rc = 0 if 'exception' in result else 1
        rc_list.append(rc)

        # Should be able to sign up new user at known domain
        result = simulate('PreSignUp_SignUp', 'NaNaHeyHeyGoodBy@amplio.org')
        print(result)
        rc = 0 if 'exception' not in result else 1
        rc_list.append(rc)

        # Should not be able to sign up random user at random domain.
        result = simulate('PreSignUp_SignUp', 'me@example.com')
        print(result)
        rc = 0 if 'exception' in result else 1
        rc_list.append(rc)

        return max(rc_list)


    def _main():
        rc = test()
        return rc


    sys.exit(_main())
# endregion
