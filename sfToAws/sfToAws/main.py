import argparse
import sys

from sqlalchemy import text

from db import get_db_engine, get_db_connection
from sfToAws import _list_objects, SF_TO_AWS_BUCKET, lambda_handler, read_json_from_s3


def do_import(programs: list[str], commit: bool):
    def update_db(ids_list: list[dict]):
        with get_db_connection() as conn:
            # transaction = conn.begin()
            updates = [{'program_id': ids['program_id'], 'salesforce_id': ids['salesforce_id'],
                        'tableau_id': (ids['tableau_id'] or '')} for ids in ids_list]

            field_names = ['salesforce_id', 'tableau_id']
            command_u = text(f'UPDATE programs SET ({",".join(field_names)}) '
                             f'= (:{",:".join(field_names)}) WHERE program_id=:program_id;')
            result_u = conn.execute(command_u, updates)
            print(f'{result_u.rowcount} program records updated.')

            # transaction.rollback()

    bucket_name = 'amplio-sf-to-aws'
    sf_records = [x for x in _list_objects(Bucket=SF_TO_AWS_BUCKET, Prefix='')]
    keys = [K['Key'] for K in sf_records]
    ids_list = []
    for key in keys:
        sf_data = read_json_from_s3(key=key, bucket=bucket_name)
        program_id = sf_data.get('program_id')
        if program_id and (not programs or program_id in programs):
            if program_id == 'CARE-HTI': print(sf_data)
            ids = {'program_id': program_id,
                   'salesforce_id': sf_data.get('salesforce_id', '??'),
                   'tableau_id': sf_data.get('tableau_id'),
                   's3_key': key
                   }
            ids_list.append(ids)
    ids_list.sort(key=lambda ids: ids['program_id'])
    update_db(ids_list)
    for ids in ids_list:
        print(
            f'program_id: {ids.get("program_id"):>17}, salesforce_id: {ids.get("salesforce_id")}, tableau_id: {ids.get("tableau_id")}, sf_key: {ids.get("sf_key")}')


def do_tests():
    programs = ["Niger Smart Villages Parent Program",
                "VSO Zambia",
                "CARE Ethiopia",
                "ECHOES World Cocoa Foundation",
                "Lumana Sale 2010",
                "Performances (Burkina Faso)",
                "UNFM - Cameroon Pilot",
                "UNICEF Rwanda 2017",
                "War Child Holland",
                "Ghana Health Service 2011 Maternal & Child Health Program",
                "Ghana Health Service 2012-2014 Maternal & Child Health Program",
                "Al Turay-Sierra Leon Program",
                "LDS/Papua New Guinea Program - Brett Macdonald - 1/24/2012",
                "LDS/Papua New Guinea Program - Brett Macdonald - 10/6/2011",
                "LDS/Papua New Guinea Program - Brett Macdonald - 4/29/2012",
                "Cecily's Fund",
                "Busara Somalia Talking Book Program Initial Pilot",
                "Busara Somalia Talking Book Pilot PHASE II",
                "LDS/Papua New Guinea Parent Program 2011-12",
                "RTI / FIP Pilot Agriculture and Nutrition Project 2018/2019",
                "IFDC 2scale Meru",
                "IFDC 2scale Uganda",
                "GAIN Maternal, Infant, & Young Child Nutrition",
                "AFYA TIMIZA 2018",
                "AFYA TIMIZA 2019 Q1",
                "AFYA TIMIZA 2019 Q2",
                "AFYA TIMIZA 2019 Q3",
                "Anzilisha 2017-2018",
                "Afya Timiza Parent Program",
                "MEDA GROW Ghana Parent Program 2013-2018",
                "MEDA/Esoko FEATS Project",
                "APME.2A",
                "CARE Ghana Parent Program 2014-2018",
                "UNICEF Ghana Parent Program 2013-2020",
                "Ghana MoFA/VSO Upper West Sale - 2/25/2011",
                "Ghana MoFA/VSO Upper West Sale - 6/17/2010",
                "MEDA Ghana 2013-2014",
                "MEDA Ghana 2015-2018",
                "MEDA Ghana 2018",
                "MEDA Ghana, UNICEF 2017-2018 extra 20",
                "UNICEF 2013-2017",
                "UNICEF 2017-2020",
                "UNICEF CHPS Program 2019",
                "Winrock Ghana 2018",
                "AGRA EISFERM TUDRIDEP Project",
                "CARE Ghana Pathways 2014-2015",
                "CARE Ghana Pathways 2016",
                "CARE Ghana Pathways 2017",
                "CARE Ghana Pathways 2018",
                "CARE Ghana Pathways 2017-2018 with UNICEF 20",
                "LBG COVID19 & Meningitis Response",
                "GHS Parent Program 2011-2014",
                "Ghana MoFA/VSO Parent Program 2010-11",
                "Tsehai Loves English Parent Program",
                "Tsehai Loves English Phase I"]

    affiliates = ['Amplio',
                  'Whiz Kids Workshop',
                  'Literacy Bridge Ghana (LBG)',
                  'Centre for Behaviour Change and Communication (CBCC)']

    def test_handler():
        event = {'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'us-west-2',
                              'eventTime': '2020-09-10T19:01:29.427Z', 'eventName': 'ObjectCreated:Put',
                              'userIdentity': {'principalId': 'AWS:AIDAJGWFCEB6RK5NV6SBG'},
                              'requestParameters': {'sourceIPAddress': '172.92.94.105'},
                              'responseElements': {'x-amz-request-id': '567BE63738FECD23',
                                                   'x-amz-id-2': 'J33azBbvcofuKhk6CZoVmw1BGZcTZecQLiQ+V2cbMTmWUgZT1L0M387VOqrdz6CNJUy7T9otUb/g4O28kiCSQLi0cRQiMkwA'},
                              's3': {'s3SchemaVersion': '1.0', 'configurationId': 'SfToAwsPut',
                                     'bucket': {'name': 'amplio-sf-to-aws',
                                                'ownerIdentity': {'principalId': 'A3MRMLZEL3RVRR'},
                                                'arn': 'arn:aws:s3:::amplio-sf-to-aws'},
                                     'object': {'key': 'a1t3l000006t8QCAAY.json', 'size': 1453,
                                                'eTag': '47aa7471b336f417227849cab868e99f',
                                                'sequencer': '005F5A780A3AF59216'}}}]}

        event2 = {'Records': [{'s3': {'bucket': {'name': 'amplio-sf-to-aws'},
                                      'object': {'key': 'a1t3l000006tJKsAAM.json'}
                                      }
                               }]
                  }

        sf_records = [x for x in _list_objects(Bucket=SF_TO_AWS_BUCKET, Prefix='')]
        records = [{'s3': {'bucket': {'name': 'amplio-sf-to-aws'}, 'object': {'key': K['Key']}}} for K in
                             sf_records]
        event2['Records'] = records

        lambda_handler(event2, None)
        # lambda_handler(event2, None)

    def test_s3():
        bucket_name = 'amplio-sf-to-aws'
        sf_records = [x for x in _list_objects(Bucket=SF_TO_AWS_BUCKET, Prefix='')]
        keys = [K['Key'] for K in sf_records]
        ids_list = []
        for key in keys:
            sf_data = read_json_from_s3(key=key, bucket=bucket_name)
            ids = {'program_id': sf_data.get('program_id', '??'),
                   'salesforce_id': sf_data.get('salesforce_id', '??'),
                   'tableau_id': sf_data.get('tableau_id', '??'),
                   's3_key': key
                   }
            ids_list.append(ids)
        ids_list.sort(key=lambda ids: ids['program_id'])
        for ids in ids_list:
            print(
                f'program_id: {ids.get("program_id"):>17}, salesforce_id: {ids.get("salesforce_id")}, tableau_id: {ids.get("tableau_id")}, sf_key: {ids.get("sf_key")}')


    # sys.exit(test_s3())
    test_handler()

    # x = {'Event': {'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'us-west-2',
    #                             'eventTime': '2020-09-10T19:01:29.427Z', 'eventName': 'ObjectCreated:Put',
    #                             'userIdentity': {'principalId': 'AWS:AIDAJGWFCEB6RK5NV6SBG'},
    #                             'requestParameters': {'sourceIPAddress': '172.92.94.105'},
    #                             'responseElements': {'x-amz-request-id': '567BE63738FECD23',
    #                                                  'x-amz-id-2': 'J33azBbvcofuKhk6CZoVmw1BGZcTZecQLiQ+V2cbMTmWUgZT1L0M387VOqrdz6CNJUy7T9otUb/g4O28kiCSQLi0cRQiMkwA'},
    #                             's3': {'s3SchemaVersion': '1.0', 'configurationId': 'SfToAwsPut',
    #                                    'bucket': {'name': 'amplio-sf-to-aws',
    #                                               'ownerIdentity': {'principalId': 'A3MRMLZEL3RVRR'},
    #                                               'arn': 'arn:aws:s3:::amplio-sf-to-aws'},
    #                                    'object': {'key': 'a1t3l000007rnRUAAY.json', 'size': 1453,
    #                                               'eTag': '47aa7471b336f417227849cab868e99f',
    #                                               'sequencer': '005F5A780A3AF59216'}}}]}}


def main():
    global args
    default_dropbox_directory = '~/Dropbox (Amplio)'

    arg_parser = argparse.ArgumentParser(description="Synchronize published content to S3.")
    arg_parser.add_argument('--verbose', action='count', default=0, help="More verbose output.")

    arg_parser.add_argument('--disposition', choices=['abort', 'commit'], default='abort',
                            help='Database disposition for the import operation.')

    arg_parser.add_argument('--import', dest='import_sf', action='store_true', default=False,
                            help='Import Salesforce from S3.')
    arg_parser.add_argument('--test', action='store_true', default=False, help='Run the test function')

    arg_parser.add_argument('--db-host', default=None, metavar='HOST',
                            help='Optional host name, default from secrets store.')
    arg_parser.add_argument('--db-port', default=None, metavar='PORT',
                            help='Optional host port, default from secrets store.')
    arg_parser.add_argument('--db-user', default=None, metavar='USER',
                            help='Optional user name, default from secrets store.')
    arg_parser.add_argument('--db-password', default=None, metavar='PWD',
                            help='Optional password, default from secrets store.')
    arg_parser.add_argument('--db-name', default='dashboard', metavar='DB',
                            help='Optional database name, default "dashboard".')

    arg_parser.add_argument('programs', nargs='*', help='Program(s) to import and/or export.')

    arglist = None
    ######################################################
    #
    #
    arglist = sys.argv[1:] + ['--db-host', 'localhost']
    #
    #
    ######################################################

    args = arg_parser.parse_args(arglist)

    get_db_engine(args=args)

    if args.import_sf:
        do_import(args.programs, args.disposition == 'commit')
    else:  # elif args.test:
        do_tests()


# region Testing Code
if __name__ == '__main__':
    main()
