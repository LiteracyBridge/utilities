import time

import boto3

"""
Roles Database interface.

Provides simple get and put operations for roles.

Roles are attached to organizations and to programs. A role consists of a user's
email address and a string of granted permissions (someday we may need to also implemtn
denied permissions). 

Roles are interited from "containing" or "owning" organizations.

"""

ORGANIZATIONS_TABLE = 'organizations'
ORGS_ORGANIZATION_FIELD: str = 'organization'
ORGS_PARENT_FIELD: str = 'parent'
ORGS_ROLES_FIELD: str = 'roles'

_ORGANIZATIONS_TABLE_CREATE_ARGS = {
    'TableName': ORGANIZATIONS_TABLE,
    'AttributeDefinitions': [{'AttributeName': ORGS_ORGANIZATION_FIELD, 'AttributeType': 'S'}],
    'KeySchema': [{'AttributeName': ORGS_ORGANIZATION_FIELD, 'KeyType': 'HASH'}]
}

PROGRAMS_TABLE = 'programs'
PROGRAMS_PROGRAM_FIELD = 'program'
PROGRAMS_ORG_FIELD = 'organization'
PROGRAMS_ROLES_FIELD = 'roles'

_CACHE_TIMEOUT_SECONDS = 30

_PROGRAMS_TABLE_CREATE_ARGS = {
    'TableName': PROGRAMS_TABLE,
    'AttributeDefinitions': [{'AttributeName': PROGRAMS_PROGRAM_FIELD, 'AttributeType': 'S'}],
    'KeySchema': [{'AttributeName': PROGRAMS_PROGRAM_FIELD, 'KeyType': 'HASH'}]
}

# limit read/write capacity units of table (affects cost of table)
_DEFAULT_PROVISIONING = {'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}


class RolesDb:
    def __init__(self, **kwargs):
        self._localdb = kwargs.get('localdb', False)
        if self._localdb:
            endpoint_url = kwargs.get('endpoint_url', 'http://localhost:8008')
            self._ORGANIZATIONS_TABLE = 'local_organizations'
            self._PROGRAMS_TABLE = 'local_programs'
            self._dynamodb_client = boto3.client('dynamodb', endpoint_url=endpoint_url,
                                                 region_name='us-west-2')
            self._dynamodb_resource = boto3.resource('dynamodb', endpoint_url=endpoint_url,
                                                     region_name='us-west-2')

        else:
            self._ORGANIZATIONS_TABLE = 'organizations'
            self._PROGRAMS_TABLE = 'programs'

            self._dynamodb_client = boto3.client('dynamodb')  # specify amazon service to be used
            self._dynamodb_resource = boto3.resource('dynamodb')

        self._organizations_table = None
        self._programs_table = None
        self._organizations_items_cache = {}
        self._organizations_items_cache_expiry = 0
        self._programs_items_cache = {}
        self._programs_items_cache_expiry = 0

        if kwargs.get('delete_tables', False):
            self._delete_tables()
        if kwargs.get('create_tables', False):
            self._create_tables()
        self._open_tables()

    def _delete_tables(self):
        start_time = time.time()
        num_updated = 0
        wait_list = []

        def delete(table_name):
            nonlocal num_updated, wait_list
            if table_name in existing_tables:
                table = self._dynamodb_resource.Table(table_name)
                table.delete()
                wait_list.append(table_name)

        def await_tables():
            nonlocal num_updated, wait_list
            for table_name in wait_list:
                # Wait for the table to be deleted before exiting
                print('Waiting for', table_name, '...')
                waiter = self._dynamodb_client.get_waiter('table_not_exists')
                waiter.wait(TableName=table_name)
                num_updated += 1

        existing_tables = self._dynamodb_client.list_tables()['TableNames']
        delete(ORGANIZATIONS_TABLE)
        delete(PROGRAMS_TABLE)

        await_tables()

        end_time = time.time()
        print('Deleted {} tables in {:.2g} seconds'.format(num_updated, end_time - start_time))

    def _create_tables(self):
        start_time = time.time()
        num_updated = 0
        wait_list = []

        def create(create_params):
            nonlocal num_updated, wait_list
            table_name = create_params['TableName']
            if table_name not in existing_tables:
                try:
                    table = self._dynamodb_client.create_table(
                        AttributeDefinitions=create_params['AttributeDefinitions'],
                        TableName=table_name,
                        KeySchema=create_params['KeySchema'],
                        ProvisionedThroughput=_DEFAULT_PROVISIONING
                    )
                    print('Successfully created table: ', table['TableDescription'])

                except Exception as err:
                    print('ERROR: ' + str(err))

                wait_list.append(table_name)

        def await_tables():
            nonlocal num_updated, wait_list
            for table_name in wait_list:
                # Wait for the table to be deleted before exiting
                print('Waiting for', table_name, '...')
                waiter = self._dynamodb_client.get_waiter('table_exists')
                waiter.wait(TableName=table_name)
                num_updated += 1

        existing_tables = self._dynamodb_client.list_tables()['TableNames']
        create(_ORGANIZATIONS_TABLE_CREATE_ARGS)
        create(_PROGRAMS_TABLE_CREATE_ARGS)

        await_tables()

        end_time = time.time()
        print('Created {} tables in {:.2g} seconds'.format(num_updated, end_time - start_time))

    def _open_tables(self):
        self._organizations_table = self._dynamodb_resource.Table(ORGANIZATIONS_TABLE)
        self._programs_table = self._dynamodb_resource.Table(PROGRAMS_TABLE)

    def invalidate_caches(self):
        """
        Empties the cache. Will be re-filled with the next request.
        """
        self._organizations_items_cache = {}
        self._organizations_items_cache_expiry = 0
        self._programs_items_cache = {}
        self._programs_items_cache_expiry = 0

    def get_organization_items(self):
        """
        Gets the list of organization items from the database. The list is cached for performance.
        """
        if time.time() > self._organizations_items_cache_expiry:
            print('Caching organizations items')
            self._organizations_items_cache = {x[ORGS_ORGANIZATION_FIELD]: x for x in
                                               self._organizations_table.scan()['Items']}
            self._organizations_items_cache_expiry = time.time() + _CACHE_TIMEOUT_SECONDS
        return self._organizations_items_cache

    def put_organization_item(self, item=None):
        """
        Put one organization item into the organizations table. Also keeps the cache up to date, if
        the cache is active.
        """
        key = item[ORGS_ORGANIZATION_FIELD]
        self._organizations_table.put_item(Item=item)
        if len(self._organizations_items_cache) > 0:
            self._organizations_items_cache[key] = item

    def get_program_items(self):
        """
        Gets the list of program items. The list is cached for performace.
        """
        if time.time() > self._programs_items_cache_expiry:
            print('Caching program items')
            self._programs_items_cache = {x[PROGRAMS_PROGRAM_FIELD]: x for x in self._programs_table.scan()['Items']}
            self._programs_items_cache_expiry = time.time() + _CACHE_TIMEOUT_SECONDS
        return self._programs_items_cache

    def put_program_item(self, item=None):
        """
        Put one program item into the programs table. Also keeps the cache up to date, if
        the cache is active.
        """
        key = item[PROGRAMS_PROGRAM_FIELD]
        self._programs_table.put_item(Item=item)
        if len(self._programs_items_cache) == 0:
            self._programs_items_cache[key] = item
