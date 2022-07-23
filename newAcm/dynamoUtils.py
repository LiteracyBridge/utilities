from datetime import datetime
from typing import Union

import boto3
import amplio
from amplio.rolemanager.Roles import ADMIN_ROLES

from utils import canonical_acm_dir_name

# specify dynamoDB table for checkout records
REGION_NAME = 'us-west-2'

dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)

CHECKOUT_TABLE_NAME = 'acm_check_out'
PROGRAM_TABLE_NAME = 'programs'
ORGANIZATION_TABLE_NAME = 'organizations'
checkout_table = dynamodb.Table(CHECKOUT_TABLE_NAME)
program_table = dynamodb.Table(PROGRAM_TABLE_NAME)
organization_table = dynamodb.Table(ORGANIZATION_TABLE_NAME)


def check_for_checkout(acm_dir) -> bool:
    """
    Checks whether there is an existing checkout record for the ACM in DynamoDB.
    :param acm_dir: to be checked
    :return: True if there is no record, False if thera already is one.
    """
    print(f"Looking for '{acm_dir}' checkout record in dynamoDb...", end='')
    query = checkout_table.get_item(Key={'acm_name': acm_dir})
    checkout_row = query.get('Item')
    if checkout_row:
        print(f'\n  Checkout record exists for {acm_dir}')
        return False
    print('ok')
    return True


def create_checkout_record(acm_name: str) -> bool:
    """
    Creates the initial checkout record for the project in DyanamoDB
    :param acm_name: to be created
    :return: True if successful, False if not
    """
    print(f'Creating checkout record for {acm_name}...', end='')
    update_expr = 'SET acm_state = :s, last_in_file_name = :f, last_in_name = :n, last_in_contact = :c, \
                last_in_date = :d, last_in_version = :v,\
                acm_comment = :z'
    condition_expr = 'attribute_not_exists(last_in_name)'  # only perform if no check-in entry exists
    expr_values = {
        ':s': 'CHECKED_IN',
        ':f': 'db1.zip',
        ':n': 'bill',
        ':c': 'techsupport@amplio.org',
        ':d': str(datetime.now()),
        ':v': 'c202002160',
        ':z': 'Created ACM'
    }

    try:
        checkout_table.update_item(
            Key={'acm_name': acm_name},
            UpdateExpression=update_expr,
            ConditionExpression=condition_expr,
            ExpressionAttributeValues=expr_values
        )
    except Exception as err:
        print(f'exception creating record: {err}')
        return False

    print('ok')
    return True


def check_for_program_record(program_id) -> bool:
    print(f"Looking for program '{program_id}' record in dynamoDb...", end='')
    query = program_table.get_item(Key={'program': program_id})
    program_row = query.get('Item')
    if program_row:
        print(f'\n  Program record exists for {program_id}')
        return False
    print('ok')
    return True


def create_program_record(program_id: str, organization: str, admin_email: Union[None, str], is_s3: bool,
                          name: str) -> bool:
    """
    Create the record in the program DynamoDB table. Lists roles, description, and repository for
    programs.

    Note the commented-out lines to update an existing program. We only create new records here.
    :param program_id: the program id.
    :param organization: the organization that owns the program.
    :admin_email: the email of the program administrator, if known.
    :return: True if successful, False otherwise.
    """
    print('Creating program record for {} in organization {}'.format(program_id, organization), end='')
    # update_expr = 'SET organization = :o, program_name = :n'
    # expr_values = {
    #     ':o': organization,
    #     ':n': name
    # }
    item = {'program': program_id, 'organization': organization, 'program_name': name}
    if admin_email:
        # Populate a new record
        item['roles'] = {admin_email: ADMIN_ROLES}
        # Update an existing one
        # update_expr += ', :rn = :r'
        # expr_values[':rn'] = 'roles'
        # expr_values[':r'] = 'M: {:e : ' + ADMIN_ROLES + ' }'
        # expr_values[':e'] = admin_email
    # If this is an S3 program, add "'repository': 's3'" to the record.
    if is_s3:
        # Populate the new record
        item['repository'] = 's3'
        # Update an existing one
        # update_expr += ', repository = :repo'
        # expr_values[':repo'] = 's3'

    try:
        program_table.put_item(Item=item)
        # program_table.update_item(
        #     Key={'program': program_name},
        #     UpdateExpression=update_expr,
        #     ExpressionAttributeValues=expr_values
        # )
    except Exception as err:
        print('exception creating record: {}'.format(err))
        return False

    print('ok')
    return True


def check_for_organization_record(organization: str, parent_organization: str) -> bool:
    print('Checking for conflicting organization record in dynamoDb...', end='')
    query = organization_table.get_item(Key={'organization': organization})
    organization_row = query.get('Item')
    if organization_row:
        existing_parent = organization_row.get('parent')
        if existing_parent != parent_organization:
            print(
                f'\n  Organization record exists for {organization}, but with different parent. {existing_parent} exists, {parent_organization} desired')
            return False
    else:
        parent_row = organization_table.get_item(Key={'organization': parent_organization})
        if not parent_row.get('Item'):
            print(f'\n  Missing parent {parent_organization} for new organization {organization}')
            return False
    print('ok')
    return True


def create_organization_record(organization: str, parent_organization: str) -> bool:
    query = organization_table.get_item(Key={'organization': organization})
    organization_row = query.get('Item')
    if organization_row:
        print(f'Organization record already exists for {organization}')
        return True

    print(f'Creating organization record for {organization} with parent {parent_organization}', end='')
    update_expr = 'SET parent = :p'
    expr_values = {
        ':p': parent_organization
    }

    try:
        organization_table.update_item(
            Key={'organization': organization},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_values
        )
    except Exception as err:
        print(f'exception creating record: {err}')
        return False

    print('ok')
    return True
