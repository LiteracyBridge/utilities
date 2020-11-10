import binascii
import json

"""
Echo.

Echoes input parameters as output.

"""


# noinspection PyUnusedLocal
def lambda_handler(event, context):
    keys = [x for x in event.keys()]

    path = event.get('path', {})
    path_parameters = event.get('pathParameters', {})
    multi_value_query_string_parameters = event.get('multiValueQueryStringParameters', {})
    query_string_params = event.get('queryStringParameters', {})

    bodyless = {k: v for k, v in event.items() if k != 'body'}
    if 'body' in event:
        bodyless['body'] = '...'
    print(f'event: {bodyless}\n')
    print(f'context: {context}')
    print(f'keys: {keys}')
    print(f'path: {path}')
    print(f'pathParameters: {path_parameters}')
    print(f'queryStringParameters: {query_string_params}')
    print(f'multiValueQueryStringParameters: {multi_value_query_string_parameters}')

    data = None
    body = event.get('body')
    body_len = 0
    data_len = 0
    if body is None:
        print('Body is None')
    else:
        body_len = len(body)
        print(f'Body is {body_len} characters long')
    if body:
        try:
            data = binascii.a2b_base64(body)
            data_len = len(data)
            print(f'Body decodes as {data_len} bytes')
        except (binascii.Error, binascii.Incomplete):
            data = None

    claims = event.get('requestContext', {}).get('authorizer', {}).get('claims', {})
    print(f'claims: {claims}')

    return {
        'statusCode': 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        'body': json.dumps({
            'claims': claims,
            'keys': keys,
            'path': path,
            'body_len': body_len,
            'data_len': data_len,
            'path_parameters': path_parameters,
            'query_string_params': query_string_params,
            'multi_value_query_string_parameters': multi_value_query_string_parameters,
        })
    }
