import datetime
import json
import uuid

import inspect
from typing import Annotated, Optional, get_type_hints

import boto3
import jwt
from amplio.utils import LambdaRouter, Claim, QueryStringParam, handler
from botocore.exceptions import ClientError


def get_secret():
    secret_name = "tableau_embedding"
    region_name = "us-west-2"

    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError:
        # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        # for discussion of exceptions
        return None

    secret_string = get_secret_value_response['SecretString']
    secret = json.loads(secret_string)
    return secret


@handler(action='getjwt')
def get_jwt(email: Claim[str] = '', programid: QueryStringParam[str] = 'TEST', testing: QueryStringParam[bool] = False) -> any:
    tableau_secret = get_secret()
    print(tableau_secret)
    claims = {
        "iss": tableau_secret['client'],
        "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=5),
        "jti": str(uuid.uuid4()),
        "aud": "tableau",
        "sub": email,
        "scp": ["tableau:views:embed", "tableau:metrics:embed"]
    }
    headers = {
        'kid': tableau_secret['secret_id'],
        'iss': tableau_secret['client']
    }
    secret_key = tableau_secret['secret_value']  # Tableau is unclear on this; the example mentions "key", which is undoc'd
    algorithm = 'HS256'
    token = jwt.encode(claims, secret_key, algorithm, headers=headers)

    return token


def lambda_function(event, context):
    router: LambdaRouter = LambdaRouter(event, context)
    action = router.path_param(0)
    return router.dispatch('getjwt')


if __name__ == '__main__':
    def test():
        event = {'requestContext': {
            'authorizer': {'claims': {'email': 'bill@amplio.org'}}},
            'pathParameters': {'proxy': 'getJwt'},
            'queryStringParameters': {'programid': 'TEST'}
        }
        jwt_result = lambda_function(event, {})
        print(jwt_result)
        jwt = jwt_result['body']
        print(jwt)

    parms = [x for x in inspect.signature(get_jwt).parameters.values()]
    ann_keys = get_jwt.__annotations__.keys()
    is_annotated: bool = isinstance(parms[0].annotation, type(Annotated))
    hints = get_type_hints(get_jwt)

    get_jwt("tableau1@amplio.org", "prog", testing=True)

    test()
