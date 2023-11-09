# -*- coding: utf-8 -*-

from amplio.rolemanager import manager
from amplio.utils.AmplioLambda import *
from sqlalchemy import text

from utils import query_to_csv, query_to_json, get_status, get_db_connection
# import requests
from utils.SimpleUsage import get_usage2

debug = False

manager.open_tables()

DEPLOYMENT_BY_COMMUNITY = '''
SELECT DISTINCT 
        td.project, 
        td.deployment, 
        d.deploymentnumber,
        td.contentpackage as package,
        td.recipientid, 
        r.communityname, 
        r.groupname,
        r.agent,
        r.language as languagecode,
        d.startdate,
        d.enddate,
       COUNT(DISTINCT td.talkingbookid) AS deployed_tbs
    FROM tbsdeployed td
    JOIN recipients r
      ON td.recipientid = r.recipientid
    LEFT OUTER JOIN deployments d
      ON d.project=td.project AND d.deployment ilike td.deployment
    WHERE td.project = :programid
    GROUP BY td.project, 
        td.deployment, 
        package, 
        d.deploymentnumber,
        td.recipientid, 
        r.communityname, 
        r.groupname, 
        r.agent,
        r.language, 
        d.startdate,
        d.enddate
'''

# DEPLOYMENT_HISTORY = 'SELECT * FROM tbsdeployed WHERE project = %s;'
# COLLECTION_HISTORY = 'SELECT * from tbscollected WHERE project = %s;'

DEPLOYMENT_HISTORY = '''
SELECT tbd.* 
  FROM tbsdeployed tbd
 WHERE project=:programid AND talkingbookid NOT IN ('UNKNOWN', '-- TO BE ASSIGNED --');   
'''
DEPLOYMENT_HISTORY_LATEST = '''
WITH latest AS (SELECT DISTINCT talkingbookid, max(deployedtimestamp) AS deployedtimestamp 
                FROM tbsdeployed  
               WHERE project=:programid AND talkingbookid NOT IN ('UNKNOWN', '-- TO BE ASSIGNED --')
            GROUP BY talkingbookid  
            ORDER BY talkingbookid)
SELECT tbd.* FROM tbsdeployed tbd
  JOIN latest latest
    ON latest.talkingbookid=tbd.talkingbookid AND latest.deployedtimestamp=tbd.deployedtimestamp
;   
'''

COLLECTION_HISTORY = '''
SELECT tbc.* 
  FROM tbscollected tbc
 WHERE project=:programid AND talkingbookid NOT IN ('UNKNOWN', '-- TO BE ASSIGNED --');
'''
COLLECTION_HISTORY_LATEST = '''
WITH latest AS (SELECT DISTINCT talkingbookid, max(collectedtimestamp) AS collectedtimestamp 
                  FROM tbscollected  
                 WHERE project=:programid AND talkingbookid NOT IN ('UNKNOWN', '-- TO BE ASSIGNED --')
              GROUP BY talkingbookid  
              ORDER BY talkingbookid)
SELECT tbc.* FROM tbscollected tbc
  JOIN latest latest
    ON latest.talkingbookid=tbc.talkingbookid AND latest.collectedtimestamp=tbc.collectedtimestamp
;
'''

TB_CHANGE_HISTORY = '''
SELECT insn as "from", outsn as "to" 
  FROM tbdataoperations 
 WHERE project=:programid AND insn~'[ABC]-[A-F0-9]{8}' AND outsn~'[ABC]-[A-F0-9]{8}' AND insn != outsn 
 ORDER BY outsn;
'''


handler
def usage2(programid: QueryStringParam, deployment: QueryStringParam,
           columns: QueryStringParam = 'deploymentnumber,startdate,category,sum(completions),sum(played_seconds)') -> Any:
    return get_usage2(programid, columns, deployment, debug=debug)


# noinspection SqlNoDataSourceInspection
@handler
def recipients(programid: QueryStringParam) -> Any:
    # noinspection SqlResolve
    query = 'SELECT * FROM recipients WHERE project ILIKE :programid'
    params = {'programid':programid}
    recips, numrecips = query_to_csv(query, params=params)
    print('{} recipients found for program {}'.format(numrecips, programid))
    return recips


# noinspection SqlNoDataSourceInspection
@handler
def deployments(programid: QueryStringParam) -> Any:
    # noinspection SqlResolve
    query = 'SELECT * FROM deployments WHERE project ILIKE :programid'
    params = {'programid':programid}
    depls, numdepls = query_to_csv(query, params=params)
    print('{} deployments found for program {}'.format(numdepls, programid))
    return depls


# noinspection SqlNoDataSourceInspection
@handler
def tbsdeployed(programid: QueryStringParam) -> Any:
    # noinspection SqlResolve
    query = '''
    SELECT tbd.talkingbookid,tbd.recipientid,tbd.deployedtimestamp,dep.deploymentnumber,tbd.deployment,
           tbd.contentpackage,tbd.username,tbd.tbcdid,tbd.action,tbd.newsn,tbd.testing
      FROM tbsdeployed tbd
      JOIN deployments dep
        ON tbd.project=dep.project AND tbd.deployment=dep.deployment
     WHERE tbd.project ILIKE :programid
     ORDER BY dep.deploymentnumber, tbd.recipientid;
    '''
    params = {'programid':programid}
    tbsdepl, numtbs = query_to_csv(query, params=params)
    print('{} tbs deployed found for {}'.format(numtbs, programid))
    return tbsdepl


@handler
def tb_depl_history(programid: QueryStringParam, latest: QueryStringParam = 'F') -> Any:
    latest_only = latest and re.match(r'(?i)[yt1].*', latest)
    print(f'Get deployment history for {programid}, latest: {latest_only}.')
    params = {'programid':programid}
    deployed, numdeployed = query_to_csv(DEPLOYMENT_HISTORY_LATEST if latest_only else DEPLOYMENT_HISTORY, params=params)
    collected, numcollected = query_to_csv(COLLECTION_HISTORY_LATEST if latest_only else COLLECTION_HISTORY, params=params)
    changed, numchanged = query_to_csv(TB_CHANGE_HISTORY, params=params)
    print(f'{numdeployed} tbsdeployed, {numcollected} tbscollected for {programid}')
    return {'tbsdeployed': deployed, 'tbscollected': collected, 'tbschanged': changed}


# noinspection SqlNoDataSourceInspection
@handler
def depl_by_community(programid: QueryStringParam) -> Any:
    # noinspection SqlResolve
    params = {'programid':programid}
    tbsdepl, numdepls = query_to_csv(DEPLOYMENT_BY_COMMUNITY, params=params)
    print('{} deployments-by-community found for {}'.format(numdepls, programid))
    return tbsdepl


@handler(roles=None)
def supported_languages(programid: QueryStringParam):
    name_map = {'languagecode': 'code', 'languagename': 'name', 'comments': 'comments'}
    # Only global "supportedlanguages" supported at this point; per-program language support TBD
    supported_langs, numlangs = query_to_json('SELECT * FROM supportedlanguages', name_map=name_map)
    print('{} supported languages'.format(numlangs))
    return supported_langs


@handler(roles=None)
def supported_categories(programid: QueryStringParam):
    name_map = {'categorycode': 'code', 'parentcategory': 'parent_category', 'is_leaf': 'isleafnode',
                'categoryname': 'name',
                'fullname': 'full_name'}
    # Only global "supportedlanguages" supported at this point; per-program language support TBD
    supported_cats, numcats = query_to_json('SELECT * FROM supportedcategories', name_map=name_map)
    print('{} supported categories'.format(numcats))
    return supported_cats


@handler
def get_roadmap(programid: QueryStringParam):
    params = {'programid':programid}
    roadmap, _ = query_to_json('SELECT * FROM roadmap WHERE program_id = :programid', params=params)
    roadmap = roadmap[0]
    print(f'Roadmap for {programid}: {roadmap}')
    return roadmap


@handler(roles='AD,PM')
def put_roadmap(programid: QueryStringParam, completed: JsonBody):
    # noinspection SqlResolve
    query = 'UPDATE roadmap SET completed = :completed WHERE program_id ILIKE :programid;'
    completed_str = json.dumps(completed)
    params = {'programid':programid, 'completed': completed_str}
    with get_db_connection() as conn:
        db_result = conn.execute(text(query), params)
        rowcount = db_result.rowcount 
    print(f'Updated {rowcount} row(s)')
    return rowcount


@handler
def status(programid: QueryStringParam, selector: QueryStringParam):
    return get_status(programid, selector)


def lambda_handler(event, context):
    print(f'Event is {event}')
    the_router = LambdaRouter(event, context)
    action = the_router.path_param(0)
    print('Action is {}'.format(action))
    return the_router.dispatch(action)


if __name__ == '__main__':
    def tests():
        """
        Tests.
        """
        global debug
        debug = True
        #             claims = event['requestContext']['authorizer'].get('claims', {})
        event = {'requestContext': {'authorizer': {'claims': {'email': 'bill@amplio.org'}}},
                 'pathParameters': {
                     'proxy': 'usage/TEST/1'
                 },
                 'queryStringParameters': {
                     'cols': 'deploymentnumber,district,sum(completions),sum(played_seconds)'}}

        event['pathParameters'] = {'proxy': 'status'}
        event['queryStringParameters']['programid'] = 'TOSTAN-SEN'
        event['queryStringParameters']['selector'] = 'ByDepl'
        result = lambda_handler(event, None)
        print(result)

        event['pathParameters'] = {'proxy': 'usage2/TEST/1'}
        event['queryStringParameters'] = {'columns': 'deploymentnumber,district,sum(completions),sum(played_seconds)', 'programid':'TEST', 'deployment':4}
        result = lambda_handler(event, None)
        body = json.loads(result['body'])
        status_code = result['statusCode']
        print(f'\n\nStatus code {status_code}\n     Result {body}')

        event['queryStringParameters']['cols'] = 'unknown,missing,invalid,caps(nothing)'
        result = lambda_handler(event, None)
        body = json.loads(result['body'])
        status_code = result['statusCode']
        print(f'\n\nStatus code {status_code}\n     Result {body}')

        event['queryStringParameters']['programid'] = 'TEST'
        event['queryStringParameters']['deployment'] = 1
        del event['queryStringParameters']['cols']
        event['queryStringParameters']['columns'] = 'deploymentnumber,district,sum(completions),sum(played_seconds)'
        event['pathParameters'] = {'proxy': 'usage2'}
        result = lambda_handler(event, None)
        body = json.loads(result['body'])
        status_code = result['statusCode']
        print(f'\n\nStatus code {status_code}\n     Result {body}')

        event['pathParameters'] = {'proxy': 'recipients'}
        result = lambda_handler(event, None)
        print(result)

        event['pathParameters'] = {'proxy': 'tbsdeployed'}
        result = lambda_handler(event, None)
        print(result)

        event['pathParameters'] = {'proxy': 'depl_by_community'}
        result = lambda_handler(event, None)
        print(result)

        event['pathParameters'] = {'proxy': 'supported_languages'}
        result = lambda_handler(event, None)
        print(result)

        event['pathParameters'] = {'proxy': 'get_roadmap'}
        result = lambda_handler(event, None)
        body = json.loads(result['body'])
        status_code = result['statusCode']
        print(f'\n\nStatus code {status_code}\n     Result {body}')

        body = [1, 2]
        event['pathParameters'] = {'proxy': 'put_roadmap'}
        event['body'] = json.dumps(body)
        result = lambda_handler(event, None)
        status_code = result['statusCode']
        print(f'\n\nStatus code {status_code}')




        print('Done.')


    tests()
