import re

from .db import query_to_json

STATUS_BY_DEPLOYMENT = '''
WITH status_by_deployment AS (
    SELECT tbd.project AS programid, d.deploymentnumber, tbd.deployment, MIN(tbd.deployedtimestamp::date) AS earliest,
           MAX(tbd.deployedtimestamp::date) AS latest,
           COUNT(distinct tbd.talkingbookid) AS deployed,
           COUNT(distinct ui.talkingbookid) AS collected
      FROM tbsdeployed tbd
      FULL OUTER JOIN usage_info ui
        ON tbd.deployment_uuid = ui.deployment_uuid
      JOIN deployments d
        ON tbd.project=d.project AND tbd.deployment=d.deployment
     WHERE NOT tbd.testing
     GROUP BY tbd.project, d.deploymentnumber, tbd.deployment
     ORDER BY tbd.project, d.deploymentnumber
)
SELECT * FROM status_by_deployment WHERE programid=:programid;
'''

STATUS_BY_TB = '''

WITH latest_deployments AS (
    WITH ld_tbs AS (SELECT DISTINCT project, talkingbookid, MAX(deployedtimestamp) AS latest
        FROM tbsdeployed WHERE deployedtimestamp>'2020-01-01' AND NOT testing GROUP BY project, talkingbookid)
    SELECT ld_tba.project, NULLIF(ld_tba.recipientid, '') as recipientid, ld_tba.talkingbookid, ld_tba.deployment as latest_deployment, d.deploymentnumber as deployment_num, ld_tb.latest as deployment_time, ld_tba.username as deployment_user
      FROM tbsdeployed ld_tba
      JOIN ld_tbs ld_tb
        ON ld_tb.project=ld_tba.project AND ld_tb.talkingbookid=ld_tba.talkingbookid AND ld_tb.latest=ld_tba.deployedtimestamp
      JOIN deployments d
        ON d.deployment=ld_tba.deployment  
), latest_collections AS (
    WITH lc_tbs AS (SELECT DISTINCT project, talkingbookid, MAX(collectedtimestamp) AS latest
        FROM tbscollected WHERE collectedtimestamp>'2020-01-01' AND NOT testing GROUP BY project, talkingbookid)
    SELECT lc_tba.project, NULLIF(lc_tba.recipientid, '') as recipientid, lc_tba.talkingbookid, lc_tba.deployment as latest_collection, d.deploymentnumber as collection_num, lc_tb.latest as collection_time, lc_tba.username as collection_user
      FROM tbscollected lc_tba
      JOIN lc_tbs lc_tb
        ON lc_tb.project=lc_tba.project AND lc_tb.talkingbookid=lc_tba.talkingbookid AND lc_tb.latest=lc_tba.collectedtimestamp
      JOIN deployments d
        ON d.deployment=lc_tba.deployment  
),status_by_tb AS (
    WITH tb_details as (SELECT COALESCE(l1.project, l2.project) as programid, COALESCE(l1.recipientid, 
            l2.recipientid) as recipientid, COALESCE(l1.talkingbookid, l2.talkingbookid) as talkingbookid, 
            l1.latest_deployment, l1.deployment_num, l1.deployment_time::date, l1.deployment_user, 
            l2.latest_collection, l2.collection_num, l2.collection_time::date, l2.collection_user
      FROM latest_deployments l1
      FULL OUTER JOIN latest_collections l2
                   ON l1.recipientid=l2.recipientid AND l1.talkingbookid=l2.talkingbookid
      WHERE l1.recipientid != '' or l2.recipientid != ''
     --WHERE NOT l1.testing AND NOT l2.testing
     )
    SELECT r.region, r.district, r.communityname, r.groupname, r.agent, r.language, tbd.* 
      FROM tb_details tbd
      JOIN recipients r
        ON tbd.recipientid=r.recipientid 
)
SELECT * FROM status_by_tb WHERE programid=:programid 
 ORDER BY region, district, communityname, groupname, agent, language, talkingbookid
'''


def status_by_deployment(programid: str):
    status, _ = query_to_json(STATUS_BY_DEPLOYMENT, params={'programid': programid})
    print(f'Status by deployment for {programid}: {status}')
    return status


def status_by_tb(programid: str):
    status, _ = query_to_json(STATUS_BY_TB, params={'programid': programid})
    print(f'Status by TB for {programid}: {status}')
    return status


def get_status(programid: str, selector: str):
    # Turn ByDepl, by_depl, by-depl, etc., to bydepl
    selector = re.sub(r'[-_]+', '', selector.lower())
    if selector == 'bydepl':
        return status_by_deployment(programid)
    elif selector == 'bytb':
        return status_by_tb(programid)
