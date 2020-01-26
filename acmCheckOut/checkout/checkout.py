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
from random import randint

import boto3

REGION_NAME = 'us-west-2'
BUCKET_NAME = 'acm-logging'
TABLE_NAME = 'acm_check_out'
CHECKED_IN = 'CHECKED_IN'
CHECKED_OUT = 'CHECKED_OUT'

# fields that we won't return.
EXCLUDED_FIELDS = ['now_out_key']

# specify s3 bucket to store logs
s3 = boto3.resource('s3', region_name=REGION_NAME)
bucket = s3.Bucket(BUCKET_NAME)

# specify dynamoDB table
dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
table = dynamodb.Table(TABLE_NAME)


def now():
    return str(datetime.datetime.now())


# noinspection PyUnusedLocal
def lambda_handler(event, context):
    """
    :param event: dict -- POST request passed in through API Gateway
    :param context: object -- can be used to get runtime data (unused but required by AWS lambda)
    :return: a JSON string object that contains the status of transaction & information required by java program
    """
    print(event)
    try:
        # parameters received from HTTPS POST request:
        api_version = event.get('api')
        if not api_version:
            return v0_handler(event)
        elif api_version == '1':
            return V1Handler(event).handle_event()

        return {
            'data': 'denied',
            'status': 'denied',
            'response': 'Unknown api version'
        }

    except Exception as err:
        return {
            'response': 'Unexpected Error',
            'data': 'denied',
            'status': 'denied',
            'error': str(err)
        }


def extract_state(acm):
    return {k: v for k, v in acm.items() if k not in EXCLUDED_FIELDS}


class V1Handler:
    def __init__(self, event):
        self.event = event
        self.acm_record = None

    def handle_event(self):
        # Every action has a db associated with it. Get the db record from dynamo.
        self.query_db()
        action = self.event.get('action').lower()

        # dispatch on the action
        if action == 'dbcheck':
            return self.db_check()
        elif action == 'checkout':
            return self.checkout()
        elif action == 'checkin':
            return self.checkin()
        elif action == 'abandon':
            return self.abandon()
        elif action == 'revoke':
            return self.revoke()
        elif action == 'create':
            return self.create()
        elif action == 'reset':
            return self.reset()

        return {
            'status': 'denied',
            'response': 'Unknown action requested'
        }

    def update_db(self, update_expression, condition_expression, expression_values, condition_handler=None, **kwargs):
        try:
            acm_name = self.event.get('db')  # db name

            table.update_item(
                Key={'acm_name': acm_name},
                UpdateExpression=update_expression,
                ConditionExpression=condition_expression,
                ExpressionAttributeValues=expression_values
            )
            logger(self.event, 'ok')
            # get current values for return
            self.query_db()
            return self.make_return('ok', **kwargs)
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
                logger(self.event, 'denied')
            return self.make_return('denied', error=str(err))

    def query_db(self):
        db = self.event.get('db')
        query_acm = table.get_item(Key={'acm_name': db})
        self.acm_record = query_acm.get('Item')

    def make_return(self, status, **kwargs):
        retval = {'status': status,
                  'state': {k: v for k, v in self.acm_record.items() if k not in EXCLUDED_FIELDS}
                  }
        # Make sure that the returned db state includes at least the db name. Happens only if the db
        # doesn't exist.
        if 'acm_name' not in retval['state']:
            retval['state'].set('acm_name', self.event.get('db'))
        for k, v in kwargs.items():
            retval[k] = v
        return retval

    @property
    def db_status(self):
        if self.acm_record is None:
            return 'nodb'
        status = 'checkedout' if self.acm_record.get('acm_state') == CHECKED_OUT else 'available'
        return status

    def db_check(self):
        """
        Determine if the db exists, and if it does, can it be checked out.
        :return: 'available' if db is available for checkout, 'checkedout' if alreay checked out, 'nodb' if no db.
        """
        if self.acm_record is None:
            return self.make_return('nodb')
        status = self.db_status
        return self.make_return(status)

    def checkout(self):
        """
        Check out the database, if it is in an appropriate state to do so, ie, checked in.
        :return: {status: ok|denied, state:{db record}}
        """
        if self.db_status != 'available':
            return self.make_return('denied')

        # arguments to the checkout
        username = self.event.get('name')  # name of requester
        contact = self.event.get('contact')  # phone number of requester
        version = self.event.get('version')  # current ACM version in format: r YY MM DD n (e.g. r1606221)
        comment = self.event.get('comment')  # to allow for internal comments if later required
        computername = self.event.get('computername')  # computer where checkout is taking place

        # Create a check-out entry for ACM in table
        new_key = str(randint(0, 10000000))  # generate new check-out key to use when checking ACM back in
        # Add it to the self.event object, so it will be logged
        self.event['key'] = new_key
        self.event['filename'] = self.acm_record.get('last_in_file_name')

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
        checkout_key = self.event.get('key')  # key must match check-key assigned at ACM check-out for check-in

        if self.acm_record.get('acm_state') != CHECKED_OUT or self.acm_record.get('now_out_key') != checkout_key:
            return self.make_return('denied', error='Not checked out by user')

        username = self.event.get('name')
        filename = self.event.get(
            'filename')  # tracks number of times ACM has been checked out, format db##.zip (e.g. db12.zip)
        version = self.event.get('version')  # current ACM version in format: r YY MM DD n (e.g. r1606221)

        # passed into AWS update table item function: delete check-out info from entry and update check-in info
        update_expr = build_remove_now_out(self.acm_record) + '\
                    SET acm_state = :s, \
                    last_in_file_name = :f, last_in_name = now_out_name, last_in_contact = now_out_contact, \
                    last_in_date = :d, last_in_comment = now_out_comment, last_in_version = :v'

        condition_expr = 'now_out_key = :k and now_out_name = :n'  # only check-in if THIS user has THIS check-out
        expr_values = {  # expression values (required by AWS boto3)
            ':k': checkout_key,
            ':s': CHECKED_IN,
            ':f': filename,
            ':v': version,
            ':n': username,
            ':d': now()
        }

        return self.update_db(update_expr, condition_expr, expr_values)

    def abandon(self):
        # noinspection PyUnusedLocal
        def condition_handler(err):
            """ Handler for conditonal update failure. It's fine if the failure is because someone else already
            revoked our checkout.
            :param err: ignored. 
            :return: a result structure if the exception is really OK, None otherwise, for default error handling. 
            """
            if self.acm_record.get('acm_state') == CHECKED_IN or self.acm_record.get('now_out_key') != checkout_key:
                # Someone else released the record from under us. Count it as success.
                return self.make_return('ok')

        # passed in POST parameters
        checkout_key = self.event.get('key')  # key must match check-key assigned at ACM check-out for check-in

        if self.acm_record.get('acm_state') != CHECKED_OUT or self.acm_record.get('now_out_key') != checkout_key:
            return self.make_return('denied', error='Not checked out by user')

        username = self.event.get('name')

        # passed to AWS update table item function: delete check-out info and mark ACM as checked-in
        update_expr = build_remove_now_out(self.acm_record) + 'SET acm_state = :s'
        condition_expr = 'now_out_key = :k and now_out_name = :n'  # only check-in if THIS user has THIS check-out
        expr_values = {
            ':s': CHECKED_IN,
            ':k': checkout_key,
            ':n': username
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
            if self.acm_record.get('acm_state') == CHECKED_IN:
                # Someone else released the record from under us. Count it as success.
                return self.make_return('ok')

        update_expr = 'SET acm_state = :s ' + build_remove_now_out(self.acm_record)
        condition_expr = ':s = :s'  # revoke check-out should always execute
        expr_values = {
            ':s': CHECKED_IN,
        }

        return self.update_db(update_expr, condition_expr, expr_values, condition_handler)

    def create(self):
        if self.db_status != 'nodb':
            return self.make_return('denied', error='DB already exists')

        filename = self.event.get(
            'filename')  # tracks number of times ACM has been checked out, format db##.zip (e.g. db12.zip)
        username = self.event.get('name')  # name of requester
        contact = self.event.get('contact')  # phone number of requester
        comment = self.event.get('comment')  # to allow for internal comments if later required
        version = self.event.get('version')  # current ACM version in format: r YY MM DD n (e.g. r1606221)

        # passed to AWS update table item function: create entry for ACM in db
        update_expr = 'SET acm_state = :s, last_in_file_name = :f, last_in_name = :n, last_in_contact = :c, \
                    last_in_date = :d, last_in_version = :v\
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
        if self.acm_record.get('resettable', 'false').lower() != 'true':
            return self.make_return('denied', error='DB is not resettable')

        filename = self.event.get('filename', 'db1.zip')  # by convention, counts # of checkins
        username = self.event.get('name', 'system')  # name of requester
        contact = self.event.get('contact', 'techsupport@amplio.org')  # phone number of requester
        comment = self.event.get('comment')  # to allow for internal comments if later required
        version = self.event.get('version')  # current ACM version in format: r YY MM DD n (e.g. r1606221)

        # passed to AWS update table item function: create entry for ACM in db
        update_expr = build_remove_now_out(self.acm_record) + 'SET acm_state = :s, last_in_file_name = :f, \
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
        logger(event, 'denied', action_name='unknownAction')
        return {
            'data': 'denied',
            'status': 'denied',
            'response': 'Unknown action requested'
        }
    except Exception as err:
        logger(event, 'denied')
        return {
            'response': 'Unexpected Error',
            'data': 'denied',
            'status': 'denied',
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
                SUCCESS -- 'ok'
                FAILURE -- 'denied'
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
        logger(event, 'denied')
        return {
            'response': 'Create new ACM',
            'data': 'denied',
            'status': 'denied'
        }

    # early return if ACM not checked-out
    if acm.get('acm_state') == 'CHECKED_IN':
        logger(event, 'denied')
        return {
            'response': 'ACM is already checked-in',
            'data': 'denied',
            'status': 'denied'
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
        'success': {'response': 'SUCCESS. Checked in by ' + str(user_name), 'data': 'ok', 'status': 'ok'},
        'failure': {'response': 'FAILED. Do not have permission to perform check-in', 'data': 'denied',
                    'status': 'denied'}
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
                SUCCESS -- 'ok'
                FAILURE -- 'denied'
    """
    if acm:
        logger(event, 'denied')
        return {
            'response': 'ACM already exists',
            'data': 'denied',
            'status': 'denied'
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
        'success': {'response': 'SUCCESS. Created new ACM', 'data': 'ok', 'status': 'ok'},
        'failure': {'response': 'FAILED. Do not have permission to perform check-in', 'data': 'denied',
                    'status': 'denied'}
    }

    return update_dynamo(event, update_expr, condition_expr, expr_values, json_resp)


def discard(event, acm):
    """
    Allows user to delete his/her own ACM check-out entry from the db (i.e. if the check-out key and user-names match)

    :param event: dict -- data passed in through POST request
    :param acm: dict -- stored acm info from db
    :return: a stringified JSON object

            must return following data parameters for each event case:
                SUCCESS -- 'ok'
                FAILURE -- 'denied'
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
        'success': {'response': 'SUCCESS. Discarded check-out info', 'data': 'ok', 'status': 'ok'},
        'failure': {'response': 'FAILED. Do not have permission to perform discard', 'data': 'denied',
                    'status': 'denied'}
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
                FAILURE error occurred      -- 'denied'    ###THIS NEEDS TO BE IMPLEMENTED ON JAVA SIDE
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
                    'status': 'ok'
                    },
        'failure': {'response': 'Your transaction was intercepted.',
                    'data': 'denied',
                    'status': 'denied'}
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
                SUCCESS -- 'ok'
                FAILURE -- 'denied'
    """

    update_expr = 'SET acm_state = :s ' + build_remove_now_out(acm)
    condition_expr = ':s = :s'  # revoke check-out should always execute
    expr_values = {
        ':s': CHECKED_IN,
    }

    # JSON response parameters
    json_resp = {
        'success': {'response': 'Deleted check out entry',
                    'data': 'ok',
                    'status': 'ok'},
        'failure': {'response': 'Unexpected Error',
                    'data': 'denied',
                    'status': 'denied'}
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
                FAILURE error occurred      -- 'denied'    ###THIS NEEDS TO BE IMPLEMENTED ON JAVA SIDE
    """
    # First time for a new ACM. Note that the filename is the string "NULL". That is most likely a PHP artifact
    # from the previous implementation of the checkout/checkin code.
    if not acm:
        return {
            'response': 'Create new ACM',
            'data': 'filename=NULL',  # filename:tracks number of times acm has been checked-out
            'filename': 'NULL',
            'status': 'ok'
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
            'status': 'denied'
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
        'status': 'ok'
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
        logger(event, 'ok')
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

                logger(event, 'denied')
                if action == 'checkOut':  # update with interceptor name & new filename
                    json_resp['failure']['data'] = {'1': 'possessor=' + str(acm.get('now_out_name')),
                                                    '2': 'filename=' + str(acm.get('last_in_file_name'))}
                    json_resp['failure']['openby'] = str(acm.get('now_out_name'))
                    json_resp['failure']['contact'] = str(acm.get('now_out_contact'))
                    json_resp['failure']['computername'] = str(acm.get('now_out_computername'))
                    json_resp['failure']['filename'] = str(acm.get('last_in_file_name'))
                return json_resp['failure']
        logger(event, 'denied')
        return {
            'response': 'Unexpected Error',
            'data': 'denied',
            'status': 'denied',
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
             html):
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
            args['db'] = 'ACM-TEST'
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
