import boto3

REGION_NAME = 'us-west-2'

TABLE_NAME = 'acm_users'
dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
table = dynamodb.Table(TABLE_NAME)
USER_POOL_ID = 'us-west-2_6EKGzq75p'
cognito_client = boto3.client('cognito-idp')


def _email_from_event(event: dict) -> str:
    """
    Given a Cognito Trigger event, return the user's email address.
    """
    try:
        request = event['request']
        user_attributes = request['userAttributes']
        email = user_attributes['email']
    except Exception as e:
        print('Exception retrieving email:  ' + str(e))
        print('Event: ' + str(event))
        raise e

    return email


def get_user_access(email):
    """
    Given a user's email address, return their access as given in the acm_users table.
    Note that the access may be given to all users at the domain.
    """
    # First look up by user's full email address.
    acm_user = table.get_item(Key={'email': email})
    user_info = acm_user.get('Item')
    if not user_info:
        # Nothing specific to the user; look up by email domain.
        parts = email.split('@')
        if len(parts) != 2:
            # This should never happen, because a user needs a valid email address to sign up.
            raise Exception("'{}' is not a valid email address.".format(email))
        org = parts[1]
        acm_user = table.get_item(Key={'email': org})
        user_info = acm_user.get('Item')
        # If the user's organization isn't in the table, give them no access at all.
        if not user_info:
            user_info = {'edit': '', 'view': '', 'admin': 'false'}

    user_access = {'edit': user_info.get('edit', ''),
                   'view': user_info.get('view', ''),
                   'admin': user_info.get('admin', 'false')
                   }
    print('Access for user {}: {}'.format(email, user_access))
    return user_access


def is_email_confirmed(email: str) -> tuple:
    """
    Look up an email address to see if it belongs to a confirmed user. If so, also return
    the username.
    """
    response = cognito_client.list_users(
        UserPoolId=USER_POOL_ID,
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

    confirmed, user = is_email_confirmed(email)
    if confirmed:
        raise Exception("That email address is in use by user '{}'.".format(user))

    # The email address isn't yet claimed. Validate the email address should have access.
    access = get_user_access(email)
    if not access['edit'] and not access['view']:
        raise Exception("'{}' is not authorized".format(email))


def token_generation_handler(event):
    """
    Adds the user's access to the claims returned to the client application.
    """
    if "response" not in event:
        event["response"] = {}
    email = _email_from_event(event)
    event["response"]["claimsOverrideDetails"] = {
        "claimsToAddOrOverride": get_user_access(email)
    }


# noinspection PyUnusedLocal
def lambda_handler(event, context):
    print(event)
    trigger = event.get('triggerSource', '')
    if trigger == 'TokenGeneration_Authentication' or trigger == 'TokenGeneration_RefreshTokens':
        token_generation_handler(event)
    elif trigger == 'PreSignUp_SignUp':
        pre_signup_handler(event)

    return event


# region Testing Code
if __name__ == '__main__':
    def simulate(trigger: str, email: str):
        event = {'triggerSource': trigger, 'request': {'userAttributes': {'email': email}}}
        result = None
        try:
            result = lambda_handler(event, None)
        except:
            pass
        return result


    def test():
        def get_access(email):
            ua = get_user_access(email)
            return ua

        rc_list = []

        # Should not be able to sign up existing user
        result = simulate('PreSignUp_SignUp', 'demo@literacybridge.org')
        rc = 0 if not result else 1
        rc_list.append(rc)

        # Should be able to sign up new user at known domain
        result = simulate('PreSignUp_SignUp', 'NaNaHeyHeyGoodBy@amplio.org')
        rc = 0 if result else 1
        rc_list.append(rc)

        # Should not be able to sign up random user at random domain.
        result = simulate('PreSignUp_SignUp', 'me@example.com')
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

        return max(rc_list)


    def _main():
        rc = test()
        return rc


    sys.exit(_main())
# endregion
