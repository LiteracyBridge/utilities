import binascii
import json
import os
import time
from io import BytesIO

from programspec import errors, spreadsheet, programspec


def cannonical_acm_project_name(acmdir):
    if acmdir is None:
        return None
    _, acm = os.path.split(acmdir)
    acm = acm.upper()
    if acm.startswith('ACM-'):
        acm = acm[4:]
    return acm


def _get_errors(from_mark=None):
    result = []
    previous_severity = -1
    for error in errors.get_errors(mark=from_mark):
        if error[0] != previous_severity:
            previous_severity = error[0]
            result.append('{}:'.format(errors.severity[error[0]]))
        result.append('  {}: {}'.format(error[1], error[2]))
    return result


def _validate(data, acm_name):
    file = BytesIO(data)
    ps = spreadsheet.load(file)
    # Should re-enable this. If there is a duplicate recipientid, this will raise an exception.
    # if not errors.has_error():
    #     return programspec.get_program_spec_from_spreadsheet(ps, cannonical_acm_project_name(acm_name))
    return _get_errors()


def lambda_handler(event, context):
    start = time.time_ns()
    errors.reset()
    
    keys = [x for x in event.keys()]
    info = {'keys': keys,
            'resource': event.get('resource', '-no resource'),
            'path': event.get('path', '-no path'),
            'httpMethod': event.get("httpMethod", '-no httpMethod')}
    params = {'path': event.get('pathParameters'), 'query': event.get('queryStringParameters'), 'vars': event.get('stageVariables')}

    print('Lambda function invoked with {}: {}'.format(type(event), len(event.get('body'))))

    project = event.get('queryStringParameters', {}).get('project', '')
    issues = _validate(binascii.a2b_base64(event.get('body')), project)

    end = time.time_ns()
    return {
        'statusCode': 200,
        'body': json.dumps({'msg': 'Hello from Lambda!',
                            'keys': keys,
                            'info': info,
                            'issues': issues,
                            'params': params,
                            'msec': (end - start) / 1000000})
    }


if __name__ == '__main__':
    print('Just testing')
    bytes_read = open("../CHPS_Concept_ProgramSpecification.xlsx", "rb").read()
    print('Got {} bytes'.format(len(bytes_read)))
    _validate(bytes_read, 'UNICEF-CHPS')
