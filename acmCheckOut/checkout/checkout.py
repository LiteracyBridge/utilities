"""
checkout.py 2019-06-18

An AWS lambda function that handles ACM checkout requests.
Processes POST requests sent to API Gateway ('ACMCheckOut' API) from the java ACM checkout program.
Checks ACMs in or out, updates backend DynamoDB table ('acm_check_out'), and returns a stringified JSON object
to client based on success of requested transaction.

Six available transactions:
'checkOut
'checkIn
'statusCheck'
'revokeCheckOut'
'discard'
'new' (subroutine of checkIn)

NOTE: To be uploaded and saved to AWS Console as lambda function 'acmCheckOut' for proper integration with API Gateway

DynamoDB table schema:
acm_check_out = {
    acm_name: { type: String, primaryKey: true, required: true },   
    acm_state: { type: String, required: false },
    acm_comment: { type: String, required: false },
    last_in_name: { type: String, required: false },
    last_in_contact: { type: String, required: false },
    last_in_date: { type: String, required: false },
    last_in_comment: { type: String, required: false },
    last_in_file_name: { type: String, required: false },
    now_out_name: { type: String, required: false },
    now_out_contact: { type: String, required: false },
    now_out_date: { type: String, required: false },
    now_out_version: { type: String, required: false },
    now_out_comment: { type: String, required: false },
    now_out_key: { type: String, required: false }
}

"""
import datetime
import json
import logging
from random import randint

from amplio.rolemanager import manager as roles_manager
from amplio.rolemanager.Roles import *

import boto3

REGION_NAME = 'us-west-2'
BUCKET_NAME = 'acm-logging'
TABLE_NAME = 'acm_check_out'
CHECKED_IN = 'CHECKED_IN'
CHECKED_OUT = 'CHECKED_OUT'

STATUS = 'status'
STATUS_OK = 'ok'
STATUS_DENIED = 'denied'
STATUS_FAILURE = 'failure'

# fields that we won't return.
EXCLUDED_FIELDS = ['now_out_key']

# specify s3 bucket to store logs
s3 = boto3.resource('s3', region_name=REGION_NAME)
bucket = s3.Bucket(BUCKET_NAME)

# specify dynamoDB table
dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
table = dynamodb.Table(TABLE_NAME)

roles_manager.open_tables()


class Authorizer:
    roles_for_action = {
        'statuscheck': ['*', 'AD', 'PM', 'CO'],
        'checkout': ['*', 'AD', 'PM', 'CO'],
        'checkin': ['*', 'AD', 'PM', 'CO'],
        'discard': ['*', 'AD', 'PM', 'CO'],
        'revokecheckout': ['*' 'AD', 'PM'],
        'create': ['*', 'AD'],
        'reset': ['*', 'AD'],
        'list': ['*', 'AD', 'PM', 'CO']
    }

    # noinspection PyUnusedLocal
    @staticmethod
    def is_authorized(claims, action, program: str = None):
        email = claims.get('email')
        has_roles = roles_manager.get_roles_for_user_in_program(email, program)
        needs_roles = Authorizer.roles_for_action.get(action, ['!!'])
        ok = any(role in has_roles for role in needs_roles)
        print('Roles for {} in {}: has {}, needs {}, authorized: {}'.format(email, program, has_roles, needs_roles, ok))
        return ok


authorizer: Authorizer = Authorizer()


# Given a program or ACM name, return just the program name part, uppercased. ACM-TEST -> TEST, test -> TEST
def cannonical_program_name(acm_name):
    if acm_name is None:
        return None
    acm_name = acm_name.upper()
    if acm_name.startswith('ACM-'):
        acm_name = acm_name[4:]
    return acm_name


def now():
    return str(datetime.datetime.now())


# noinspection PyUnusedLocal
def lambda_handler(event, context):
    """
    :param event: dict -- POST request passed in through API Gateway
    :param context: object -- can be used to get runtime data (unused but required by AWS lambda)
    :return: a JSON string object that contains the status of transaction & information required by java program
    """
    try:
        kwargs = {}
        qp = event.get('queryStringParameters')
        if qp:
            kwargs['queryStringParameters'] = qp
        qp = event.get('pathParameters')
        if qp:
            kwargs['pathParameters'] = qp
        qp = event.get('requestContext', {}).get('authorizer', {}).get('claims')
        if qp:
            kwargs['claims'] = qp
        try:
            qp = None
            event_body = event.get('body')
            if event_body and isinstance(event_body, str):
                qp = json.loads(event_body)
            else:
                qp = event_body
        except Exception:
            pass
        if qp:
            kwargs['body'] = qp
        # The stage variables might be useful at some point
        # qp = event.get('stageVariables')
        # if qp:
        #     kwargs['stageVariables'] = qp

        if len(kwargs.keys()) > 0:
            print(f'Lambda integration {kwargs}')
            try:
                body = V1Handler(event, **kwargs).handle_event()
                print('return statusCode: 200')
                return {
                    'statusCode': 200,
                    "headers": {"Access-Control-Allow-Origin": "*"},
                    'body': json.dumps(body)
                }
            except Exception as e:
                logger = logging.getLogger()
                logger.setLevel(logging.DEBUG)
                logger.error(str(e), exc_info=True)
                return {
                    'data': STATUS_DENIED,
                    STATUS: STATUS_DENIED,
                    'response': 'Exception processing request'
                }
        else:
            send_ses('techsupport@amplio.org', 'v0 call to acmCheckOut', json.dumps(event), ['bill@amplio.org'])
            print('No apparent cognito info')
            print(event)

            # parameters received from HTTPS POST request:
            api_version = event.get('api')
            if not api_version:
                return v0_handler(event)
            elif api_version == '1':
                return V1Handler(event).handle_event()

        return {
            'data': STATUS_DENIED,
            STATUS: STATUS_DENIED,
            'response': 'Unknown api version'
        }

    except Exception as err:
        return {
            'response': 'Unexpected Error',
            'data': STATUS_DENIED,
            STATUS: STATUS_DENIED,
            'error': str(err)
        }


def extract_state(acm):
    return {k: v for k, v in acm.items() if k not in EXCLUDED_FIELDS}


class V1Handler:
    def __init__(self, event, **kwargs):
        self.event = event
        self._checkout_record = None
        # The event may contain several of these:
        # action (required)
        # db (required, the program)
        # name (user's name)
        # contact (user's phone or email)
        # version (version of the ACM making the request)
        # computername (name of the computer where the ACM is running)
        # key (random number, assigned at checkout, must match to checkin)
        # filename (the name of the file being checked in -- the latest db123.zip file)

        path_parameters = kwargs.get('pathParameters', {})
        query_string_parameters = kwargs.get('queryStringParameters', {})
        claims = kwargs.get('claims', {})
        # stage_variables = kwargs.get('stageVariables')

        self._body = kwargs.get('body', {})
        self._claims = claims
        self._email = claims.get('email')
        # self._stage_variables = stage_variables

        if path_parameters:
            path_parts = path_parameters.get('proxy', '').split('/')
            self._action = path_parts[0].lower()
            self._program = path_parts[1] if len(path_parts) > 1 else None
            print(f'path parameters: {self._action}, {self._program}')
        else:
            self._action = (query_string_parameters.get('action') or event.get('action', '')).lower()
            self._program = event.get('db') or event.get('program')

        self._name = query_string_parameters.get('name') or self._claims.get('email') or event.get('name')
        self._contact = query_string_parameters.get('phone_number') or event.get('contact')
        self._version = query_string_parameters.get('version') or event.get('version')
        self._computername = query_string_parameters.get('computername') or event.get('computername')
        self._key = query_string_parameters.get('key') or event.get('key')
        self._filename = query_string_parameters.get('filename')
        self._comment = query_string_parameters.get('comment') or event.get('comment')

        print(f'Arguments: action:{self._action}, program:{self._program}, name:{self._name}, contact:{self._contact}')

    def handle_event(self):
        action = self._action.lower()
        if action == 'list':
            return self.list_checkouts()
        elif action == 'report':
            return self.report()

        # Every action after this has a db associated with it. Get the db record from dynamo.
        self.query_db()

        # dispatch on the action
        if action=='statuscheck':
            return self.statuscheck()
        elif action == 'checkout':
            return self.checkout()
        elif action == 'checkin':
            return self.checkin()
        elif action == 'discard':
            return self.discard()
        elif action == 'revokecheckout':
            return self.revoke()
        elif action == 'create':
            return self.create()
        elif action == 'reset':
            return self.reset()

        return {
            STATUS: STATUS_DENIED,
            'response': 'Unknown action requested'
        }

    def update_db(self, update_expression, condition_expression, expression_values, condition_handler=None, **kwargs):
        try:
            # acm_name = self._program  # db name
            acm_name = self._program

            table.update_item(
                Key={'acm_name': acm_name},
                UpdateExpression=update_expression,
                ConditionExpression=condition_expression,
                ExpressionAttributeValues=expression_values
            )
            logger(self.event, STATUS_OK)
            # get current values for return
            self.query_db()
            return self.make_return(STATUS_OK, **kwargs)
        except Exception as err:
            # transaction intercepted: unexpected error or conditional check failed
            if 'ConditionalCheckFailedException' in str(err):
                # A condition failed; someone else slipped in before our update. Return an error
                # with updated values.
                self.query_db()
                if condition_handler is not None:
                    handled = condition_handler(err)
                    if handled is not None:
                        return handled
                logger(self.event, STATUS_DENIED)
            return self.make_return(STATUS_DENIED, error=str(err))

    def query_db(self):
        # db = self._program
        program = self._program
        query_acm = table.get_item(Key={'acm_name': program})
        self._checkout_record = query_acm.get('Item')

    def make_return(self, status, **kwargs):
        retval = {STATUS: status, 'state': {}}
        try:
            if self._checkout_record:
                retval['state'] = {k: v for k, v in self._checkout_record.items() if k not in EXCLUDED_FIELDS}
        except Exception:
            pass

        # Make sure that the returned db state includes at least the db name. Happens only if the db
        # doesn't exist.
        if 'acm_name' not in retval['state']:
            retval['state']['acm_name'] = self._program
        for k, v in kwargs.items():
            retval[k] = v
        return retval

    @property
    def db_state(self):
        if self._checkout_record is None:
            return 'nodb'
        status = 'checkedout' if self._checkout_record.get('acm_state') == CHECKED_OUT else 'available'
        return status

    def statuscheck(self):
        """
        Determine if the db exists, and if it does, can it be checked out.
        :return: 'available' if db is available for checkout, 'checkedout' if alreay checked out, 'nodb' if no db.
        """
        if self._checkout_record is None:
            return self.make_return('nodb')
        status = self.db_state
        return self.make_return(STATUS_OK if status=='available' else status)

    def list_checkouts(self):
        def has_access(program: str):
            if program in admin_targets:
                return True
            for uf in uf_targets:
                if program.startswith(uf):
                    return True
            return False

        email = self._claims.get('email')
        programs = roles_manager.get_programs_for_user(email)
        admin_targets = [k for k, v in programs.items() if SUPER_USER_ROLE in v or ADMIN_ROLE in v]
        uf_targets = [x + '-FB-' for x in admin_targets]

        acms = []
        checkouts = table.scan()['Items']
        for checkout in checkouts:
            if has_access(cannonical_program_name(checkout.get('acm_name'))):
                acms.append(checkout)

        return {STATUS: STATUS_OK, 'acms': acms}

    def checkout(self):
        """
        Check out the database, if it is in an appropriate state to do so, ie, checked in.
        :return: {status: ok|denied, state:{db record}}
        """
        if self.db_state != 'available':
            return self.make_return(STATUS_DENIED)

        # arguments to the checkout
        username = self._name    # name of requester
        contact = self._contact  # phone number of requester
        version = self._version  # current ACM version in format: r YY MM DD n (e.g. r1606221)
        comment = self._comment  # to allow for internal comments if later required
        computername = self._computername  # computer where checkout is taking place

        # Create a check-out entry for ACM in table. Pick a number between 0 and 10 million.
        new_key = str(randint(0, 10000000))  # generate new check-out key to use when checking ACM back in
        # Add it to the self.event object, so it will be logged
        self.event['key'] = new_key
        self.event['filename'] = self._checkout_record.get('last_in_file_name')

        update_expr = 'SET acm_state = :s, now_out_name = :n, now_out_contact = :c, now_out_key = :k, \
                                now_out_version = :v, now_out_comment = :t, now_out_date = :d, \
                                now_out_computername = :m'
        condition_expr = 'attribute_not_exists(now_out_name) and acm_state = :in'
        expr_values = {
            ':s': CHECKED_OUT,
            ':in': CHECKED_IN,
            ':n': username,
            ':c': contact,
            ':k': new_key,
            ':v': version,
            ':t': comment,
            ':d': now(),
            ':m': computername
        }

        return self.update_db(update_expr, condition_expr, expr_values, key=new_key)

    def checkin(self):
        # parameters received from HTTPS POST request
        checkout_key = self._key  # key must match check-key assigned at ACM check-out for check-in

        if self._checkout_record.get('acm_state') != CHECKED_OUT or self._checkout_record.get(
                'now_out_key') != checkout_key:
            return self.make_return(STATUS_DENIED, error='Not checked out by user')

        filename = self._filename  # tracks number of times ACM has been checked out, format db##.zip (e.g. db12.zip)
        version = self._version    # current ACM version in format: r YY MM DD n (e.g. r1606221)

        # passed into AWS update table item function: delete check-out info from entry and update check-in info
        update_expr = build_remove_now_out(self._checkout_record) + '\
                    SET acm_state = :s, \
                    last_in_file_name = :f, last_in_name = now_out_name, last_in_contact = now_out_contact, \
                    last_in_date = :d, last_in_comment = now_out_comment, last_in_version = :v'

        condition_expr = 'now_out_key = :k'  # only check-in if the user has THIS check-out
        expr_values = {  # expression values (required by AWS boto3)
            ':k': checkout_key,
            ':s': CHECKED_IN,
            ':f': filename,
            ':v': version,
            ':d': now()
        }

        return self.update_db(update_expr, condition_expr, expr_values)

    def discard(self):
        # noinspection PyUnusedLocal
        def condition_handler(err):
            """ Handler for conditonal update failure. It's fine if the failure is because someone else already
            revoked our checkout.
            :param err: ignored. 
            :return: a result structure if the exception is really OK, None otherwise, for default error handling. 
            """
            if self._checkout_record.get('acm_state') == CHECKED_IN or self._checkout_record.get(
                    'now_out_key') != checkout_key:
                # Someone else released the record from under us. Count it as success.
                return self.make_return(STATUS_OK)

        # passed in POST parameters
        checkout_key = self._key  # key must match check-key assigned at ACM check-out for check-in

        if self._checkout_record.get('acm_state') != CHECKED_OUT or self._checkout_record.get(
                'now_out_key') != checkout_key:
            return self.make_return(STATUS_OK)

        username = self._name

        # passed to AWS update table item function: delete check-out info and mark ACM as checked-in
        update_expr = build_remove_now_out(self._checkout_record) + 'SET acm_state = :s'
        condition_expr = 'now_out_key = :k'  # only check-in if the user has THIS check-out
        expr_values = {
            ':s': CHECKED_IN,
            ':k': checkout_key
        }

        return self.update_db(update_expr, condition_expr, expr_values, condition_handler)

    def revoke(self):
        # noinspection PyUnusedLocal
        def condition_handler(err):
            """ Handler for conditonal update failure. It's fine if the failure is because the acm is no longer
            checked out.
            :param err: ignored. 
            :return: a result structure if the exception is really OK, None otherwise, for default error handling. 
            """
            if self._checkout_record.get('acm_state') == CHECKED_IN:
                # Someone else released the record from under us. Count it as success.
                return self.make_return(STATUS_OK)

        update_expr = 'SET acm_state = :s ' + build_remove_now_out(self._checkout_record)
        condition_expr = 'now_out_key = :k and acm_state = :o'  # revoke check-out should always execute
        expr_values = {
            ':s': CHECKED_IN,
            ':k': self._key,
            ':o': CHECKED_OUT
        }

        return self.update_db(update_expr, condition_expr, expr_values, condition_handler)

    def create(self):
        if self.db_state != 'nodb':
            return self.make_return(STATUS_DENIED, error='DB already exists')

        filename = self._filename  # tracks number of times ACM has been checked out, format db##.zip (e.g. db12.zip)
        username = self._name  # name of requester
        contact = self._contact  # phone number of requester
        comment = self._comment  # to allow for internal comments if later required
        version = self._version  # current ACM version in format: r YY MM DD n (e.g. r1606221)

        # passed to AWS update table item function: create entry for ACM in db
        update_expr = 'SET acm_state = :s, last_in_file_name = :f, last_in_name = :n, last_in_contact = :c, \
                    last_in_date = :d, last_in_version = :v, \
                    acm_comment = :z'
        condition_expr = 'attribute_not_exists(last_in_name)'  # only perform if no check-in entry exists
        expr_values = {
            ':s': CHECKED_IN,
            ':f': filename,
            ':n': username,
            ':c': contact,
            ':d': now(),
            ':v': version,
            ':z': comment
        }

        return self.update_db(update_expr, condition_expr, expr_values)

    def reset(self):
        if self._checkout_record.get('resettable', 'false').lower() != 'true':
            return self.make_return(STATUS_DENIED, error='DB is not resettable')

        filename = self._filename or 'db1.zip'  # by convention, counts # of checkins
        username = self._name or 'system'       # name of requester
        contact = self._contact or 'techsupport@amplio.org'  # phone number of requester
        comment = self._comment  # to allow for internal comments if later required
        version = self._version  # current ACM version in format: r YY MM DD n (e.g. r1606221)

        # passed to AWS update table item function: create entry for ACM in db
        update_expr = build_remove_now_out(self._checkout_record) + 'SET acm_state = :s, last_in_file_name = :f, \
                    last_in_name = :n, last_in_contact = :c, last_in_date = :d, last_in_version = :v, \
                    acm_comment = :z'
        condition_expr = ':s = :s'  # always perform
        expr_values = {
            ':s': CHECKED_IN,
            ':f': filename,
            ':n': username,
            ':c': contact,
            ':d': now(),
            ':v': version,
            ':z': comment
        }

        return self.update_db(update_expr, condition_expr, expr_values)

    def report(self):
        fromaddr = self._body.get('from', "techsupport@amplio.org")
        subject = self._body.get('subject')
        body = self._body.get('body')
        recipient = self._body.get('recipient', 'techsupport@amplio.org')
        recipients = recipient if isinstance(recipient, list) else [recipient]
        html = True if self._body.get('html') else False

        ses_result = send_ses(fromaddr, subject, body, recipients, html)
        print(f'ses_result: {str(ses_result)}')
        return ses_result


def v0_handler(event):
    try:
        action = event.get('action').lower()  # 'checkIn', 'revokeCheckOut', 'checkOut', 'statusCheck', 'discard'
        if action == 'report':
            return send_report(event)

        acm_name = event.get('db')  # name of ACM (e.g. 'ACM-FB-2013-01') - primary key of dynamoDB table
        # check_key = event.get('key')  # distinguish between ACM creation (i.e. key = 'new') or standard check-in

        # query table to get stored acm entry from db
        query_acm = table.get_item(Key={'acm_name': acm_name})
        acm = query_acm.get('Item')

        # available transactions
        if action == 'revokecheckout':
            return revoke_check_out(event, acm)
        if action == 'discard':
            return discard(event, acm)
        if action == 'statuscheck':
            return status_check(acm)
        if action == 'checkin':
            return check_in(event, acm)
        if action == 'checkout':
            return check_out(event, acm)
        # user requested an unknown action
        logger(event, STATUS_DENIED, action_name='unknownAction')
        return {
            'data': STATUS_DENIED,
            STATUS: STATUS_DENIED,
            'response': 'Unknown action requested'
        }
    except Exception as err:
        logger(event, STATUS_DENIED)
        return {
            'response': 'Unexpected Error',
            'data': STATUS_DENIED,
            STATUS: STATUS_DENIED,
            'error': str(err)
        }


def build_remove_now_out(acm):
    """
    Given an acm record, build a list of "now_out_*" keys into a string like "REMOVE now_out_foo, now_out_bar ".
    If there are no "now_out_*" items, return an empty string.
    :param acm: An acm item record.
    :return: A string like "REMOVE now_out_user " or ""
    """
    # A list like now_out_name, now_out_contact, now_out_date, now_out_key, now_out_comment
    list_to_remove = [k for k in acm.keys() if k.startswith('now_out_')]
    if len(list_to_remove) == 0:
        return ''
    string_to_remove = ', '.join(list_to_remove)
    return 'REMOVE ' + string_to_remove + ' '


def check_in(event, acm):
    """
    A successful 'checkIn' request deletes the existing ACM check-out parameters from the db and updates the check-in
    entry with the requested user's info iff specified conditions are met (to protect against multi-user overwrites).

    :param event: dict -- data passed in through POST request
    :param acm: dict -- stored acm info from db
    :return: a stringified JSON object (similar to check_out)

            must return following data parameters for each event case:
                SUCCESS -- STATUS_OK
                FAILURE -- STATUS_DENIED
    """
    # parameters received from HTTPS POST request
    check_key = event.get('key')  # key must match check-key assigned at ACM check-out for check-in
    file_name = event.get(
        'filename')  # tracks number of times ACM has been checked out, format db##.zip (e.g. db12.zip)
    user_name = event.get('name')  # name of requester
    contact = event.get('contact')  # phone number of requester

    # If the key is 'new' instead of a pseudo-random number, this is a new db. Different code path.
    if check_key == 'new':
        return new_check_in(event, acm)

    # early return if ACM does not exist
    if not acm:
        logger(event, STATUS_DENIED)
        return {
            'response': 'Create new ACM',
            'data': STATUS_DENIED,
            STATUS: STATUS_DENIED
        }

    # early return if ACM not checked-out
    if acm.get('acm_state') == 'CHECKED_IN':
        logger(event, STATUS_DENIED)
        return {
            'response': 'ACM is already checked-in',
            'data': STATUS_DENIED,
            STATUS: STATUS_DENIED
        }

    # passed into AWS update table item function: delete check-out info from entry and update check-in info

    update_expr = build_remove_now_out(acm) + '\
                SET acm_state = :s, \
                last_in_file_name = :f, last_in_name = :n, last_in_contact = :c, last_in_date = :d, \
                last_in_comment = now_out_comment'
    condition_expr = 'now_out_key = :k and now_out_name = :n'  # only perform check-in if THIS user has THIS check-out
    expr_values = {  # expression values (required by AWS boto3)
        ':k': check_key,
        ':s': CHECKED_IN,
        ':f': file_name,
        ':n': user_name,
        ':c': contact,
        ':d': now()
    }

    # JSON response parameters
    json_resp = {
        'success': {'response': 'SUCCESS. Checked in by ' + str(user_name), 'data': STATUS_OK, STATUS: STATUS_OK},
        'failure': {'response': 'FAILED. Do not have permission to perform check-in', 'data': STATUS_DENIED,
                    STATUS: STATUS_DENIED}
    }

    # update dynamodb
    return update_dynamo(event, update_expr, condition_expr, expr_values, json_resp)


def new_check_in(event, acm):
    """
    Allows the user to create a new ACM by specifying the action as 'checkIn' and the key as 'new.' Only works for ACMs
    that do not already exist

    :param event: dict -- data passed in through POST request
    :param acm: dict -- stored acm info from db
    :return: same as check_in function -- a stringified JSON object

            must return following data parameters for each event case:
                SUCCESS -- STATUS_OK
                FAILURE -- STATUS_DENIED
    """
    if acm:
        logger(event, STATUS_DENIED)
        return {
            'response': 'ACM already exists',
            'data': STATUS_DENIED,
            STATUS: STATUS_DENIED
        }

    # parameters received from HTTPS POST request
    file_name = event.get(
        'filename')  # tracks number of times ACM has been checked out, format db##.zip (e.g. db12.zip)
    user_name = event.get('name')  # name of requester
    contact = event.get('contact')  # phone number of requester
    comment = event.get('comment')  # to allow for internal comments if later required

    # passed to AWS update table item function: create entry for ACM in db
    update_expr = 'SET acm_state = :s, last_in_file_name = :f, last_in_name = :n, last_in_contact = :c, \
                last_in_date = :d, acm_comment = :v'
    condition_expr = 'attribute_not_exists(last_in_name)'  # only perform if no check-in entry exists
    expr_values = {
        ':s': CHECKED_IN,
        ':f': file_name,
        ':n': user_name,
        ':c': contact,
        ':d': now(),
        ':v': comment
    }

    # JSON response parameters
    json_resp = {
        'success': {'response': 'SUCCESS. Created new ACM', 'data': STATUS_OK, STATUS: STATUS_OK},
        'failure': {'response': 'FAILED. Do not have permission to perform check-in', 'data': STATUS_DENIED,
                    STATUS: STATUS_DENIED}
    }

    return update_dynamo(event, update_expr, condition_expr, expr_values, json_resp)


def discard(event, acm):
    """
    Allows user to delete his/her own ACM check-out entry from the db (i.e. if the check-out key and user-names match)

    :param event: dict -- data passed in through POST request
    :param acm: dict -- stored acm info from db
    :return: a stringified JSON object

            must return following data parameters for each event case:
                SUCCESS -- STATUS_OK
                FAILURE -- STATUS_DENIED
    """
    # passed in POST parameters
    user_name = event.get('name')
    check_key = event.get('key')

    # passed to AWS update table item function: delete check-out info and mark ACM as checked-in
    update_expr = build_remove_now_out(acm) + 'SET acm_state = :s'
    condition_expr = 'now_out_key = :k and now_out_name = :n'  # only perform check-in if THIS user has THIS check-out
    expr_values = {
        ':s': CHECKED_IN,
        ':k': check_key,
        ':n': user_name
    }

    # JSON response parameters
    json_resp = {
        'success': {'response': 'SUCCESS. Discarded check-out info', 'data': STATUS_OK, STATUS: STATUS_OK},
        'failure': {'response': 'FAILED. Do not have permission to perform discard', 'data': STATUS_DENIED,
                    STATUS: STATUS_DENIED}
    }

    return update_dynamo(event, update_expr, condition_expr, expr_values, json_resp)


def check_out(event, acm):
    """
    A successful 'checkOut' request creates the ACM check-out entry in the db with the requested user's info iff a
    check-out entry doesn't already exist (conditional update to protect against multi-user overwrites)

    :param event: dict -- data passed in through POST request
    :param acm: dict -- stored acm info from db

    :return: a stringified JSON object that reflects the status of transaction containing a user-readable response,
            data that will be parsed by the java ACM checkout program, & any errors

            must return following data parameters for each event case:
                SUCCESS checked out         -- 'key=RANDOM_KEY', 'filename=COUNTER '
                FAILURE already checked out -- 'possessor=NAME', 'filename=COUNTER '
                FAILURE ACM does not exist  -- 'filename=NULL'
                FAILURE error occurred      -- STATUS_DENIED    ###THIS NEEDS TO BE IMPLEMENTED ON JAVA SIDE
    """
    # parameters received from HTTPS POST request
    user_name = event.get('name')  # name of requester
    contact = event.get('contact')  # phone number of requester
    version = event.get('version')  # current ACM version in format: r YY MM DD n (e.g. r1606221)
    comment = event.get('comment')  # to allow for internal comments if later required
    computer_name = event.get('computername')  # computer where checkout is taking place

    # Make sure ACM is available to be checked out
    status = status_check(acm)
    if status['response'] != 'ACM available':
        return status

    # Create a check-out entry for ACM in table
    new_key = str(randint(0, 10000000))  # generate new check-out key to use when checking ACM back in
    # Add it to the event object, so it will be logged
    event['key'] = new_key
    event['filename'] = acm.get('last_in_file_name')

    update_expr = 'SET acm_state = :s, now_out_name = :n, now_out_contact = :c, now_out_key = :k, \
                            now_out_version = :v, now_out_comment = :t, now_out_date = :d, \
                            now_out_computername = :m'
    condition_expr = 'attribute_not_exists(now_out_name)'  # check for unexpected check-out entry
    expr_values = {
        ':s': CHECKED_OUT,
        ':n': user_name,
        ':c': contact,
        ':k': new_key,
        ':v': version,
        ':t': comment,
        ':d': now(),
        ':m': computer_name
    }

    # JSON response parameters
    json_resp = {
        'success': {'response': 'SUCCESS. Checked out to ' + str(user_name),
                    'data': {'1': 'key=' + new_key,
                             '2': 'filename=' + str(acm.get('last_in_file_name'))},
                    'key': new_key,
                    'filename': str(acm.get('last_in_file_name')),
                    STATUS: STATUS_OK
                    },
        'failure': {'response': 'Your transaction was intercepted.',
                    'data': STATUS_DENIED,
                    STATUS: STATUS_DENIED}
        # data: to be updated with interceptor's name & new file_name in update_dynamo function
    }

    return update_dynamo(event, update_expr, condition_expr, expr_values, json_resp)


def revoke_check_out(event, acm):
    """
    A successful 'revokeCheckOut' request deletes any ACM check-out entry from the db.
    :param event: dict -- data passed in through POST request
    :param acm: dict -- stored acm info from db
    :return: a stringified JSON object

            must return following data parameters for each event case:
                SUCCESS -- STATUS_OK
                FAILURE -- STATUS_DENIED
    """

    update_expr = 'SET acm_state = :s ' + build_remove_now_out(acm)
    condition_expr = ':s = :s'  # revoke check-out should always execute
    expr_values = {
        ':s': CHECKED_IN,
    }

    # JSON response parameters
    json_resp = {
        'success': {'response': 'Deleted check out entry',
                    'data': STATUS_OK,
                    STATUS: STATUS_OK},
        'failure': {'response': 'Unexpected Error',
                    'data': STATUS_DENIED,
                    STATUS: STATUS_DENIED}
        # data: updated with interceptor's name & new file_name in update_dynamo function
    }

    return update_dynamo(event, update_expr, condition_expr, expr_values, json_resp)


def status_check(acm):
    """
    Queries check-out status of ACM.

    :param acm: dict -- row entry for acm in db
    :return: stringified JSON object of ACM status

            must return following data parameters for each event case:
                FAILURE already checked out -- 'possessor=NAME filename=COUNTER'
                FAILURE ACM does not exist  -- 'filename=NULL'
                FAILURE error occurred      -- STATUS_DENIED    ###THIS NEEDS TO BE IMPLEMENTED ON JAVA SIDE
    """
    # First time for a new ACM. Note that the filename is the string "NULL". That is most likely a PHP artifact
    # from the previous implementation of the checkout/checkin code.
    if not acm:
        return {
            'response': 'Create new ACM',
            'data': 'filename=NULL',  # filename:tracks number of times acm has been checked-out
            'filename': 'NULL',
            STATUS: STATUS_OK
        }

    if acm.get('acm_state') == CHECKED_OUT:
        return {
            'response': 'Already checked out',
            'data': {'1': 'possessor=' + str(acm.get('now_out_name')),
                     '2': 'filename=' + str(acm.get('last_in_file_name')),
                     '3': 'contact=' + str(acm.get('now_out_contact')),
                     '4': 'date=' + str(acm.get('now_out_date'))},
            'openby': str(acm.get('now_out_name')),
            'contact': str(acm.get('now_out_contact')),
            'filename': str(acm.get('last_in_file_name')),
            'opendate': str(acm.get('now_out_date')),
            'computername': str(acm.get('now_out_computername')),
            STATUS: STATUS_DENIED
        }
    return {
        'response': 'ACM available',
        'data': {'1': 'filename=' + str(acm.get('last_in_file_name')),
                 '2': 'updater=' + str(acm.get('last_in_name')),
                 '3': 'contact=' + str(acm.get('last_in_contact')),
                 '4': 'date=' + str(acm.get('last_in_date'))},
        'filename': str(acm.get('last_in_file_name')),
        'saveby': str(acm.get('last_in_name')),
        'contact': str(acm.get('last_in_contact')),
        'savedate': str(acm.get('last_in_date')),
        STATUS: STATUS_OK
    }


# helper function to update dynamoDB
def update_dynamo(event, update_expr, condition_expr, expr_values, json_resp):
    """
    :param event: dict -- data passed in through POST request
    :param update_expr: string -- dynamodb table update expression
    :param condition_expr: string -- conditional update expression
    :param expr_values: string -- expression values (required by AWS)
    :param json_resp: dict -- json response data
    :return: JSON response
    """
    # parameters received from HTTPS POST request
    acm_name = event.get('db')  # name of ACM (e.g. 'ACM-FB-2013-01') - primary key of dynamoDB table
    action = event.get('action')

    try:
        table.update_item(
            Key={
                'acm_name': acm_name
            },
            UpdateExpression=update_expr,
            ConditionExpression=condition_expr,
            ExpressionAttributeValues=expr_values
        )
        logger(event, STATUS_OK)
        return json_resp['success']
    except Exception as err:
        # transaction intercepted: unexpected error or conditional check failed
        if 'ConditionalCheckFailedException' in str(err):
            query_acm = table.get_item(  # retrieve acm entry from db
                Key={
                    'acm_name': acm_name,
                })
            acm = query_acm.get('Item')
            if acm:
                if not acm.get('now_out_name'):  # no check-out info exists
                    if action == 'discard':  # interceptor deleted check-out for us - consider it success!
                        logger(event, 'nop')  # log as no operation
                        return json_resp['success']

                logger(event, STATUS_DENIED)
                if action == 'checkOut':  # update with interceptor name & new filename
                    json_resp['failure']['data'] = {'1': 'possessor=' + str(acm.get('now_out_name')),
                                                    '2': 'filename=' + str(acm.get('last_in_file_name'))}
                    json_resp['failure']['openby'] = str(acm.get('now_out_name'))
                    json_resp['failure']['contact'] = str(acm.get('now_out_contact'))
                    json_resp['failure']['computername'] = str(acm.get('now_out_computername'))
                    json_resp['failure']['filename'] = str(acm.get('last_in_file_name'))
                return json_resp['failure']
        logger(event, STATUS_DENIED)
        return {
            'response': 'Unexpected Error',
            'data': STATUS_DENIED,
            STATUS: STATUS_DENIED,
            'error': str(err)
        }


# adds quotes to a string with embedded commas. For data destined for a .csv
def enquote(v):
    # escape any quotes
    v = v.replace('"', '\\"')
    # if any commas, enclose in quotes
    if v.find(',') > 0:
        v = '"' + v + '"'
    return v


# helper function to write plain txt log files & upload to s3 bucket 'acm-logging'
def logger(event, response, action_name=None):
    """
    Writes a single line .txt file with filename as current datetime + random integer in s3 bucket 'acm-logging'
    in the 'logs/year/month' directory.

    Each log string contains the following parameters, where name:value are the values from the event:
        datetime, action, response:{resp}, name:{value},...

    :param event: dict -- passed in parameters from POST request
    :param response: string -- transaction result
    :param action_name: string -- if given, use this as the action name, and also log 'action:{action}' (for
            unrecognized actions)
    :return: None -- creates an s3 object
    """
    action = str(event.get('action'))  # 'checkIn', 'revokeCheckOut', 'checkOut', 'statusCheck', 'discard'
    date = datetime.date.today()

    # override action if an override was given
    if action_name is not None:
        action = action_name
    body = datetime.datetime.utcnow().isoformat() + "," + action + ",response:" + str(response)
    for k in event.keys():
        v = event.get(k)
        # Don't log 'None'. Don't log the action, unless there was an override.
        if v is not None and (k != 'action' or action_name is not None):
            body += ',' + k + ':' + enquote(str(v))

    try:
        # noinspection PyUnusedLocal
        response = bucket.put_object(
            Body=body,  # timestamp,action,k1:v1,k2:v2,... log entry
            Key='logs/' + str(date.year) + '/' + str(date.month) + '/' + now() + '_' + str(randint(0, 1000)),
            # unique filename
            ContentType='text/plain',
        )
    except Exception as err:
        print('LOGGING ERROR: ' + str(err))
        pass


def send_report(event):
    fromaddr = event.get('from') or 'ictnotifications@literacybridge.org'
    subject = event.get('subject')
    body = event.get('body')
    recipient = event.get('to') or event.get('recipient') or 'ictnotifications@literacybridge.org'
    recipients = recipient if isinstance(recipient, list) else [recipient]
    html = event.get('html') or False

    return send_ses(fromaddr, subject, body, recipients, html)

# Format and send an ses message. Options are
# html    - if true, send as html format
# dry_run - if true, do not actually send email
def send_ses(fromaddr,
             subject,
             body_text,
             recipients,
             html=False):
    """Send an email via the Amazon SES service.

    Example:
      send_ses('me@example.com, 'greetings', "Hi!", 'you@example.com)

    Return:
      If 'ErrorResponse' appears in the return message from SES,
      return the message, otherwise return an empty '' string.
    """

    message = {'Subject': {'Data': subject}}
    if html:
        message['Body'] = {'Html': {'Data': body_text}}
    else:
        message['Body'] = {'Text': {'Data': body_text}}

    client = boto3.client('ses')
    response = client.send_email(
        Source=fromaddr,
        Destination={
            'ToAddresses': recipients
        },
        Message=message
    )

    return response


if __name__ == "__main__":
    def fn(action, **kwargs):
        args = {'api': '1', 'action': action}
        for k, v in kwargs.items():
            args[k] = v
        if 'db' not in args:
            args['db'] = 'ACM-TEST-NEW'
        return lambda_handler(args, None)


    print('Testing...')
    result = fn('dbcheck', db='ACM-TEST-NONE')
    print(result)
    result = fn('dbcheck')
    print(result)
    result = fn('checkout', name='Bill', contact='555-1212', version='123')
    print(result)
    key = result.get('key')
    result = fn('abandon', key=key, name='Bill')
    print(result)
    result = fn('reset', db='ACM-TEST.BOO')
    print(result)
