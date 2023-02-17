import base64
from typing import Optional, Tuple

import boto3 as boto3
from cryptography.hazmat.backends import default_backend as crypto_default_backend
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from amplio.utils import LambdaRouter, handler, QueryStringParam

PROFILE_NAME = None 
REGION_NAME = 'us-west-2'
session = boto3.Session(profile_name=PROFILE_NAME)
dynamodb = session.resource('dynamodb', region_name=REGION_NAME)
KEY_TABLE_NAME = 'uf_keys'
uf_key_table = None


def get_uf_keys(programid: str, deployment_num: int) -> Optional[Tuple[bytes, bytes]]:
    """
    Retrieve the private and public parts of a UF encryption key pair.
    :param programid: The program to which the keys apply.
    :param deployment_num: The deployment within the program to which the keys apply.
    :return: a Tuple of (private_pem,public_der) if the keys exist, or None if they do not.
    """
    global uf_key_table
    if uf_key_table is None:
        uf_key_table = dynamodb.Table(KEY_TABLE_NAME)
    query = uf_key_table.get_item(Key={'programid': programid, 'deployment_num': deployment_num})
    key_row = query.get('Item')
    if key_row:
        print(f'Retrieved key pair for {programid} deployment # {deployment_num}')
        return (key_row.get('private_pem'), key_row.get('public_der'))


def save_uf_keys(programid: str, deployment_num: int, private_pem: bytes, public_der: bytes) -> bool:
    """
    Saves the private and public parts of a key pair in a dynamodb table.
    :param programid: The program to which the keys apply.
    :param deployment_num: The deployment within the program to which the keys apply.
    :param private_pem: The private key as bytes.
    :param public_der: The public key as bytes.
    :return: True if the keys were saved, False if they already existed or could not be saved.
    """
    global uf_key_table
    if uf_key_table is None:
        uf_key_table = dynamodb.Table(KEY_TABLE_NAME)
    query = uf_key_table.get_item(Key={'programid': programid, 'deployment_num': deployment_num})
    key_row = query.get('Item')
    if key_row:
        print(f'Key pair already exists for {programid}, depl # {deployment_num}')
        return False

    print(f'Creating uf_key record for {programid} deployment # {deployment_num}')
    update_expr = 'SET private_pem = :p, public_der = :d'
    expr_values = {
        ':p': private_pem,
        ':d': public_der
    }

    try:
        uf_key_table.update_item(
            Key={'programid': programid, 'deployment_num': deployment_num},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_values
        )
    except Exception as err:
        print(f'Exception creating uf_key record for {programid} deployment # {deployment_num}: {err}')
        return False

    return True


def generate_key_pair():
    """
    Generates a private/public key pair for use in UF encryption. The model 2 Talking Book
    can AES encrypt User Feedback, and will encrypt the AES key with this public key.

    This 2048 bit PK is what the TBv2 expects/needs.
    :return: a Tuple of the (private_pem,public_der).
    """
    key = rsa.generate_private_key(
        backend=crypto_default_backend(),
        public_exponent=65537,
        key_size=2048
    )

    private_key_pem = key.private_bytes(
        crypto_serialization.Encoding.PEM,
        crypto_serialization.PrivateFormat.PKCS8,
        crypto_serialization.NoEncryption()
    )

    public_key_der = key.public_key().public_bytes(
        encoding=crypto_serialization.Encoding.DER,
        format=crypto_serialization.PublicFormat.PKCS1
    )

    return private_key_pem, public_key_der


@handler
def publickey(programid: QueryStringParam[str], deployment_num: QueryStringParam[int]):
    """
    Lambda handler function to retrieve or create & persist a private/public key
    pair for use in UF encryption.
    :param programid: The program to which the keys apply.
    :param deployment_num: The deployment within the program to which the keys apply.
    :return public_der: The public key as bytes.
    """
    # Has this key pair already been created?
    pair: Optional[Tuple[bytes, bytes]] = get_uf_keys(programid, deployment_num)
    if pair is not None:
        public_der = pair[1]
    else:
        # No, create and save the key pair.
        private_pem, public_der = generate_key_pair()
        save_uf_keys(programid, deployment_num, private_pem, public_der)
    return {"public_key": base64.b64encode(bytes(public_der)).decode('ascii')}


def lambda_router(event, context):
    the_router: LambdaRouter = LambdaRouter(event, context)
    action = the_router.path_param(0)
    print(
        f'Request {action} by user {the_router.claim("email") or "-unknown-"} for program {the_router.queryStringParam("programid") or "None"}')
    return the_router.dispatch(action)


def test_main():
    email = 'bill@amplio.org'
    event = {
        'requestContext': {
            'authorizer': {'claims': {'email': email}}
        },
        'queryStringParameters': {'programid': 'TEST', 'deployment_num': '3'},
        'pathParameters': {'proxy': 'publickey'},
        'httpMethod': 'GET',
    }
    context = {}
    result = lambda_router(event, context)
    print(result)


if __name__ == '__main__':
    test_main()
