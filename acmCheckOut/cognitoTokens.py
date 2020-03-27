import json

import boto3

REGION_NAME = 'us-west-2'
TABLE_NAME = 'acm_users'

dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
table = dynamodb.Table(TABLE_NAME)

def get_user_access(event):
    # claims: Object
    #   aud: "5h9tg11mb73p4j2ca1oii7bhkn"
    #   auth_time: "1557509949"
    #   cognito:username: "me"
    #   email: "me@example.org"
    #   email_verified: "true"
    #   event_id: "12345678-1234-1234-1234-123456789012" # event uuid
    #   exp: "Fri May 10 21:16:19 UTC 2019"
    #   iat: "Fri May 10 20:16:19 UTC 2019"
    #   iss: "https://cognito-idp.us-west-2.amazonaws.com/us-west-1_123ABC456def # cognito pool id
    #   phone_number: "+18005551212"
    #   phone_number_verified: "false"
    #   sub: "12345678-1234-1234-1234-123456789012" # cognito user 'sub'
    #   token_use: "id"

    email = 'demo@amplio.org'
    try:
        requestContext = event['requestContext']
        authorizer = requestContext['authorizer']
        claims = authorizer['claims']
        email = claims['email']
    except Exception as e:
        print('Exception retrieving email:  ' +str(e))
        print('Event: ' + str(e))

    # Look up the user by their email.
    query_acm = table.get_item(Key={'email': email})
    userinfo = query_acm.get('Item')
    if not userinfo:
        # Nothing specific to the use; look up by organization
        parts = email.split('@')
        if len(parts) != 2:
            # This should never happen, because a user needs a valid email address to sign up.
            raise Exception('Not a valid email address: email')
        org = parts[1]
        query_acm = table.get_item(Key={'email': email})
        userinfo = query_acm.get('Item')
        if not userinfo:
            userinfo = {'edit': '', 'view': '', 'admin': 'false'}

    claimsToAddOrOverride = {'edit': userinfo.get('edit', ''),
                             'view': userinfo.get('view', ''),
                             'admin': userinfo.get('admin', 'false')
                            }

    return claimsToAddOrOverride



def lambda_handler(event, context):
    if not "response" in event:
        event["response"] = {}
    event["response"]["claimsOverrideDetails"] = {
        "claimsToAddOrOverride": get_user_access(event)
    }
    return event
