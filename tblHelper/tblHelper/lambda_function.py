import json
import sys
import time
import traceback

import boto3

"""
Management of TB-Loader ids and serial numbers.

TB-Loaders table
    Key is email name of the TB-Loader user. Obviously, these must be unique.
    The "tbloaderid" value is the TB-Loader ID assigned to that email address.
    The "reserved" value is the highest reserved serial number. When a TB-Loader
    needs more serial numbers, it calls the "reserve" function. That function
    reads the current reserved value, adds the number being allocated, and
    writes the new value. The range thus reserved is returned to the caller.
    
    In the event that the email is not in the table, a new TB-Loader Id is 
    allocated. This is kept in a special, invalid email address, 
    max-tbloader@@amplio.org. The value "maxtbloader" is read, and incremented.
    The requesting email address is written, with the newly assigned tbloaderid,
    and a reserved value of 0.  
"""
TBLOADERIDS_TABLE = 'TbLoaderIds'

dynamodb_client = boto3.client('dynamodb')  # specify amazon service to be used
dynamodb_resource = boto3.resource('dynamodb')

tbloadersid_table = None

# limit read/write capacity units of table (affects cost of table)
default_provisioning = {'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}

# Property & key names
TBL_EMAIL = 'email'
TBLOADERS_KEY_NAME = TBL_EMAIL
TBL_ID = 'tbloaderid'
TBL_HEX_ID = 'hexid'
TBL_RESERVE = 'reserved'

MAX_LOADER_KEY = 'max-tbloader@@amplio.org'
MAX_TB_LOADER = 'maxtbloader'

STATUS = 'status'
MESSAGE = 'message'
EXCEPTION = 'exception'
STATUS_OK = 'ok'
STATUS_FAILURE = 'failure'

MAX_VALUE = 65000
MAX_ID = 65000
DEFAULT_RESERVATION = 1024

# region Table Initialization Code
def create_table(table_name=None, attributes=None):
    """
    Creates table with the given name and attributes.
    :param table_name: String, name of the table.
    :param attributes: List of tuples of (attribute_name, attribute_type, key_type)
    """
    attribute_defs = []
    key_schema = []
    for name, attr_type, keytype in attributes:
        attribute_defs.append({'AttributeName': name, 'AttributeType': attr_type})
        key_schema.append({'AttributeName': name, 'KeyType': keytype})

    try:
        table = dynamodb_client.create_table(
            AttributeDefinitions=attribute_defs,
            TableName=table_name,
            KeySchema=key_schema,
            ProvisionedThroughput=default_provisioning
        )
        print('Successfully created table: ', table['TableDescription'])

    except Exception as err:
        print('ERROR: ' + str(err))


def delete_tables():
    start_time = time.time()
    num_updated = 0

    def delete(table_name):
        nonlocal num_updated
        if table_name in existing_tables:
            table = dynamodb_resource.Table(table_name)
            table.delete()
            # Wait for the table to be deleted before exiting
            print('Waiting for', table_name, '...')
            waiter = dynamodb_client.get_waiter('table_not_exists')
            waiter.wait(TableName=table_name)
            num_updated += 1

    existing_tables = dynamodb_client.list_tables()['TableNames']
    delete(TBLOADERIDS_TABLE)

    end_time = time.time()
    print('Deleted {} tables in {:.2g} seconds'.format(num_updated, end_time - start_time))


def create_tables():
    start_time = time.time()
    num_updated = 0

    def create(table_name, args):
        nonlocal num_updated
        if table_name not in existing_tables:
            create_table(table_name, args)
            # Wait for the table to be created before exiting
            print('Waiting for', table_name, '...')
            waiter = dynamodb_client.get_waiter('table_exists')
            waiter.wait(TableName=table_name)
            num_updated += 1

    existing_tables = dynamodb_client.list_tables()['TableNames']
    create(TBLOADERIDS_TABLE, [(TBLOADERS_KEY_NAME, 'S', 'HASH')])

    end_time = time.time()
    print('Created {} tables in {:.2g} seconds'.format(num_updated, end_time - start_time))


def load_tbloaderids_table(data=None):
    start_time = time.time()
    if data is None:
        data = [(1, "toffic@literacybridge.org", 966),
                (2, "sandesimon@yahoo.com", 1460),
                (3, "fidelis@literacybridge.org", 1351),
                (4, "augustina@literacybridge.org", 1322),
                (6, "ken@literacybridge.org", 1326),
                (7, "fred@literacybridge.org", 1126),
                (9, "gandaakuuf@yahoo.com", 852),
                (11, "gaetink@gmail.com", 641),
                (12, "bill@amplio.org", 905),
                (13, "mdakura@literacybridge.org", 1485),
                (17, "simon@literacybridge.org", 1465),
                (18, "linus@literacybridge.org", 1461),
                (19, "Ivynayiri@gmail.com", 1),
                (20, "cbcc01@literacybridge.org", 1235),
                (25, "bertrand@literacybridge.org", 1285),
                (27, "princevitus111@gmail.com", 1),
                (28, "bigbrosab@yahoo.com", 1),
                (29, "tbmahama@ghdf.org.rw", 396),
                (30, "tpamba@centreforbcc.org", 162),
                (31, "zechariah@literacybridge.org", 1),
                (32, "eastern@literacybridge.org", 1),
                (33, "stephen@literacybridge.org", 1),
                (34, "leonard@literacybridge.org", 1),
                (35, "pius@literacybridge.org", 1315),
                (36, "nathalia@literacybridge.org", 1),
                (37, "amasika@centreforbcc.com", 1),
                (38, "tleokoe@centreforbcc.com", 9),
                (39, "sejore@centreforbcc.com", 2),
                (40, "phyllis.epure@centreforbcc.com", 1),
                (41, "patrick.lokorio@centreforbcc.com", 1),
                (42, "silantoi@centreforbcc.com", 252),
                (44, "rebecca.mayabi@centreforbcc.com", 1),
                (45, "ckarimi@kcdmsd.rti.org", 1),
                (46, "rndune@kcdmsd.rti.org", 1),
                (49, "wanjohijosphat@gmail.com", 1),
                (53, "cameronryall@informedinternational.org", 7),
                (54, "lily@farmerline.co", 1),
                (55, "jsr988@gmail.com", 1),
                (56, "tbaratier@forba.net", 1),
                (57, "bmag@saltpixel.net", 1)]
    num_updated = 0
    max_id = -1
    for id, email, reserved in data:
        if email and id:
            max_id = max(id, max_id)
            item = {TBL_EMAIL: email, TBL_ID: id, TBL_HEX_ID: '{:#06x}'.format(id), TBL_RESERVE: reserved + 1}
            tbloadersid_table.put_item(Item=item)
            num_updated += 1
    if max_id > 0:
        item = {TBL_EMAIL: MAX_LOADER_KEY, MAX_TB_LOADER: max_id}
        tbloadersid_table.put_item(Item=item)

    end_time = time.time()
    print('Added {} organizations in {:.2g} seconds'.format(num_updated, end_time - start_time))


def populate():
    delete_tables()
    create_tables()
    open_tables()
    load_tbloaderids_table()


def merge():
    EXISTING = [
        (0, "Cliff (Windows Parallels)", "", ""),
        (1, "toffic", "toffic@literacybridge.org", ""),
        (2, "Simon Sande (CAPECS)", "sandesimon@yahoo.com", ""),
        (3, "Fidelis", "fidelis@literacybridge.org", ""),
        (4, "Augustina", "augustina@literacybridge.org", ""),
        (5, "Jirapa Five", "", ""),
        (6, "Ken", "ken@literacybridge.org", ""),
        (7, "Fred", "fred@literacybridge.org", ""),
        (8, "Jirapa Eight", "", ""),
        (9, "Gandaakuu Felix (PRUDA)", "gandaakuuf@yahoo.com", ""),
        (11, "Gaeten (CARD)", "gaetink@gmail.com", ""),
        (12, "Bill (Windows Parallels)", "bill@literacybridge.org", ""),
        (13, "Michael Dakura", "mdakura@literacybridge.org", ""),
        (16, "Bill's Windows Laptop", "", "Seattle field test Windows laptop"),
        (17, "Simon's laptop", "simon@literacybridge.org", ""),
        (18, "Linus's Laptop", "linus@literacybridge.org", ""),
        (19, "Ivay (Pronet)", "Ivynayiri@gmail.com", ""),
        (20, "CBCC01", "cbcc01@literacybridge.org", ""),
        (22, "Kipo", "", "Program Manager"),
        (23, "Ubald", "", "Program Manager"),
        (24, "Elma", "", "Content"),
        (25, "Bertrand", "bertrand@literacybridge.org", "Program Associate"),
        (26, "Charles", "", "Country Director"),
        (27, "Prince Vitus", "princevitus111@gmail.com", "TUDRIDEP KFP"),
        (28, "Richard Sabule", "bigbrosab@yahoo.com", "TUDRIDEP KFP"),
        (29, "Mahama Project", "tbmahama@ghdf.org.rw", ""),
        (30, "Teobald Pamba", "tpamba@centreforbcc.org", ""),
        (31, "Zechariah Biimi", "zechariah@literacybridge.org", ""),
        (32, "Eastern Khana-khali", "eastern@literacybridge.org", "UNICEF PM"),
        (33, "Tengan Stephen", "stephen@literacybridge.org", "CHPS project"),
        (34, "Leonard Yaghr", "leonard@literacybridge.org", "CHPS project"),
        (35, "Pius Gbare", "pius@literacybridge.org", "CHPS project"),
        (36, "Nathalia Batuu", "nathalia@literacybridge.org", "CHPS project"),
        (37, "Alex Masika", "amasika@centreforbcc.com", "AT: Samburu"),
        (38, "Titus Leokoe", "tleokoe@centreforbcc.com", "AT: Samburu"),
        (39, "Sammy Ejore", "sejore@centreforbcc.com", "AT: Turkana"),
        (40, "Phyllis Epure", "phyllis.epure@centreforbcc.com", "AT: Turkana"),
        (41, "Patrick Lokorio", "patrick.lokorio@centreforbcc.com", "Afya-Timiza"),
        (42, "Suzanne Silantoi", "silantoi@centreforbcc.com", "Afya-Timiza"),
        (43, "tbdemo", "", "Demo Laptop"),
        (44, "Rebecca Mayabi", "rebecca.mayabi@centreforbcc.com", "AT: Turkana"),
        (45, "Charles Karimi", "ckarimi@kcdmsd.rti.org", "CBCC RTI project"),
        (46, "Rachel Ndune", "rndune@kcdmsd.rti.org", "CBCC RTI project"),
        (48, "Teobald Pamba", "", "Replacement ID for CBCC"),
        (49, "Josphat Wanjohi", "wanjohijosphat@gmail.com", "RTI Eastern region"),
        (50, "User 1", "", "Smart Villages"),
        (51, "User 2", "", "Smart Villages"),
        (52, "User 3", "", "Smart Villages"),
        (53, "Camron Ryall", "cameronryall@informedinternational.org", "Nepal"),
        (54, "Lily Akorfa Keledorme", "lily@farmerline.co", "Farmerline"),
        (55, "Jasmine L. R. Chavis", "jsr988@gmail.com", "Instructional Designer"),
        (56, "Thomas Baratier", "tbaratier@forba.net", "UNFM - Senegal"),
        (57, "Bruno Magnone", "bmag@saltpixel.net", "UNFM - Senegal")]
    MAXES = [
        (0x0000, 0x0000, 0x0000, 0x0000, 0x0000),
        (0x0001, 0x03B1, 0x03C6, 0x03C6, 0x03C6),
        (0x0002, 0x05B4, 0x0000, 0x0000, 0x0000),
        (0x0003, 0x02D6, 0x0000, 0x0000, 0x0547),
        (0x0004, 0x038D, 0x038D, 0x052A, 0x052A),
        (0x0005, 0x033B, 0x0000, 0x0000, 0x0000),
        (0x0006, 0x052A, 0x052E, 0x052E, 0x052E),
        (0x0007, 0x0466, 0x0466, 0x0466, 0x0466),
        (0x0008, 0x02E0, 0x0000, 0x0000, 0x0000),
        (0x0009, 0x0354, 0x0000, 0x0000, 0x0000),
        (0x000B, 0x0281, 0x0000, 0x0000, 0x0000),
        (0x000C, 0x0389, 0x0000, 0x0000, 0x0000),
        (0x000D, 0x05CD, 0x0549, 0x0549, 0x0549),
        (0x0011, 0x05B5, 0x05B9, 0x0000, 0x0000),
        (0x0012, 0x05B2, 0x05B5, 0x0000, 0x0000),
        (0x0014, 0x04D2, 0x04D2, 0x04D3, 0x0000),
        (0x0018, 0x0002, 0x0000, 0x0000, 0x0000),
        (0x0019, 0x03B7, 0x0505, 0x0000, 0x0000),
        (0x001D, 0x018C, 0x018C, 0x0000, 0x0000),
        (0x001E, 0x0097, 0x0094, 0x00A2, 0x00A2),
        (0x0023, 0x0523, 0x0523, 0x0000, 0x0000),
        (0x0026, 0x0000, 0x0000, 0x0009, 0x0009),
        (0x0027, 0x0000, 0x0002, 0x0002, 0x0002),
        (0x002A, 0x00C1, 0x00FC, 0x00FC, 0x00FC),
        (0x002D, 0x0001, 0x0001, 0x0001, 0x0001),
        (0x002F, 0x003E, 0x0044, 0x0044, 0x0044),
        (0x0030, 0x0000, 0x0007, 0x0007, 0x0007),
        (0x0032, 0x0000, 0x0000, 0x0001, 0x0001),
        (0x0035, 0x0007, 0x0007, 0x0007, 0x0007)]

    max_values = {}
    for x in MAXES:
        num = x[0]
        max_value = max([v for v in x[1:]])
        max_values[num] = max_value
    existing_tbids = {}
    for x in EXISTING:
        num = int(x[0])
        email = x[2]
        if email:
            max_value = max_values.get(num) or 1
            existing_tbids[num] = (email, max_value)

    for num in sorted(existing_tbids.keys()):
        print('({}, "{}", {}),'.format(num, existing_tbids[num][0], existing_tbids[num][1]))

    print(existing_tbids)
    print(max_values)


# endregion


def open_tables():
    global tbloadersid_table
    tbloadersid_table = dynamodb_resource.Table(TBLOADERIDS_TABLE)


def allocate_tbid_item(claims):
    result = {STATUS: STATUS_FAILURE}
    email = claims.get('email')
    max_loader = tbloadersid_table.get_item(Key={TBL_EMAIL: MAX_LOADER_KEY}, ConsistentRead=True)
    if not max_loader or 'Item' not in max_loader:
        result[MESSAGE] = 'Unable to read MAX_LOADER_KEY in tbloaderids'
        return result

    item = max_loader.get('Item')
    old_max_id = int(item.get(MAX_TB_LOADER, 0))
    if not old_max_id:
        result[MESSAGE] = 'Unable to retrieve max tbloader id from tbloaderids'
        return result

    new_max_id = old_max_id + 1
    new_item = {TBL_ID: new_max_id, TBL_HEX_ID: '{:04x}'.format(new_max_id), TBL_EMAIL: email, TBL_RESERVE: 1}

    # Update the max counter
    key = {TBL_EMAIL: MAX_LOADER_KEY}
    update_expression = 'SET ' + MAX_TB_LOADER + ' = :newid'
    condition_expression = MAX_TB_LOADER + ' = :oldid'
    expression_values = {':oldid': old_max_id, ':newid': new_max_id}
    try:
        tbloadersid_table.update_item(Key=key,
                                      UpdateExpression=update_expression,
                                      ConditionExpression=condition_expression,
                                      ExpressionAttributeValues=expression_values
                                      )
    except Exception as err:
        # transaction intercepted: unexpected error or conditional check failed
        if 'ConditionalCheckFailedException' in str(err):
            result[MESSAGE] = 'Update race failure, try again'
        else:
            result[MESSAGE] = 'Unable to update max tbloaderid'
        result[EXCEPTION] = str(err)
        return result

    # Add the new tbloader id
    tbloadersid_table.put_item(Item=new_item)

    result[STATUS] = STATUS_OK
    result['item'] = new_item
    return result


def do_reserve(params, claims):

    result = {}
    num = int(params.get('n', DEFAULT_RESERVATION))
    email = claims.get('email')

    # See if there's an existing tbid for the email
    tbid_data = tbloadersid_table.get_item(Key={TBL_EMAIL: email})

    # if so, use it, otherwise try to allocate one
    item = None
    if tbid_data and 'Item' in tbid_data:
        item = tbid_data.get('Item')
        id = int(item.get(TBL_ID, 0))
        hex_id = item.get(TBL_HEX_ID)
        cur_value = int(item.get(TBL_RESERVE, 0))
        if not (id and hex_id and cur_value) or cur_value + num > MAX_VALUE:
            item = None # force allocation of a new id

    if item is None:
        allocate_result = allocate_tbid_item(claims)
        if allocate_result.get(STATUS) != STATUS_OK:
            return allocate_result
        item = allocate_result.get('item')

    # verify that we have all of the required fields
    id = int(item.get(TBL_ID, 0))
    hex_id = item.get(TBL_HEX_ID)
    cur_value = int(item.get(TBL_RESERVE, 0))
    if not (id and hex_id and cur_value):
        return {STATUS: STATUS_FAILURE, MESSAGE: 'Missing values in "{}"'.format(item)}

    # we can now allocate the next block of tb ids
    new_value = cur_value + num
    # Update the reserve counter
    key = {TBL_EMAIL: email}
    update_expression = 'SET ' + TBL_RESERVE + ' = :new_value'
    condition_expression = TBL_RESERVE + ' = :cur_value'
    expression_values = {':cur_value': cur_value, ':new_value': new_value}
    try:
        tbloadersid_table.update_item(Key=key,
                                      UpdateExpression=update_expression,
                                      ConditionExpression=condition_expression,
                                      ExpressionAttributeValues=expression_values
                                      )
    except Exception as err:
        # transaction intercepted: unexpected error or conditional check failed
        if 'ConditionalCheckFailedException' in str(err):
            result[MESSAGE] = 'Allocate block race failure, try again'
        else:
            result[MESSAGE] = 'Unable to update with new block'
        result[EXCEPTION] = str(err)
        result[STATUS] = STATUS_FAILURE
        return result

    # We return the old value. The caller can use old_value to old_value + n
    result = {STATUS: STATUS_OK, 'begin': cur_value, 'end': new_value, 'n': num, 'id': id, 'hexid': hex_id}
    return result


def lambda_handler(event, context):
    global tbloadersid_table
    start = time.time_ns()
    if tbloadersid_table is None:
        open_tables()

    result = {'output': [],
              'status': ''}

    print('Event path parameters: {}'.format(event.get('pathParameters')))
    parts = [x for x in event.get('pathParameters', {}).get('proxy', 'reserve').split('/') if x != 'data']
    action = parts[0]
    print('Action: {}'.format(action))

    path = event.get('path', {})
    path_parameters = event.get('pathParameters', {})
    query_string_params = event.get('queryStringParameters', {})
    print('Query string parameters: {}'.format(query_string_params))

    claims = event.get('requestContext', {}).get('authorizer', {}).get('claims', {})

    try:
        if action == 'reserve':
            result = do_reserve(query_string_params, claims)
    except Exception as ex:
        traceback.print_exception(type(ex), ex, ex.__traceback__)
        result['status'] = STATUS_FAILURE
        result['exception'] = 'Exception: {}'.format(ex)

    print('Result: {}'.format(result))
    end = time.time_ns()

    return {
        'statusCode': 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        'body': json.dumps({'msg': 'TB Id Reservation Utility',
                            'result': result,
                            'claims': claims,
                            'action': action,
                            'path': path,
                            'path_parameters': path_parameters,
                            'query_string_params': query_string_params,
                            'msec': (end - start) / 1000000})
    }


# region Testing Code
if __name__ == '__main__':
    def test():
        def test_reserve(n=100, email=None):
            if email is None:
                email = claims['email']
            print('\nReserve {}:'.format(n))

            submit_event = {'requestContext': {
                'authorizer': {'claims': {'email': email, 'edit': claims['edit'], 'view': claims['view']}}},
                'pathParameters': {'proxy': 'reserve', 'n': n},
                'queryStringParameters': {'n': n}
            }
            result = lambda_handler(submit_event, {})
            reserve_result = json.loads(result['body']).get('result', {})
            return reserve_result

        claims = {'edit': '.*', 'view': '.*', 'email': 'bill@amplio.org'}
        print('Just testing')

        reserve_result = test_reserve()
        print(reserve_result)
        reserve_result = test_reserve(10)
        print(reserve_result)
        print(test_reserve(10, email='new.user@amplio.org'))
        print(test_reserve(10, email='new.user@amplio.org'))


    def _main():
        # merge()
        # populate()
        open_tables()
        test()


    sys.exit(_main())
# endregion
