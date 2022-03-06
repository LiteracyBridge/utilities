#!/usr/bin/env zsh

psql=/Applications/Postgres.app/Contents/Versions/latest/bin/psql
dbcxn=(--host=localhost --port=5432 --username=lb_data_uploader --dbname=dashboard)
#dbcxn=(--host=lb-device-usage.ccekjtcevhb7.us-west-2.rds.amazonaws.com --port=5432 --username=lb_data_uploader --dbname=dashboard)

${psql} ${dbcxn} <<SQL_EOF
begin transaction;
\set ECHO queries
-- #1: Update playstatistics
-- Create a table with the right shape.
CREATE TEMPORARY TABLE ps AS (SELECT * FROM playstatistics WHERE FALSE);
-- Populate from CSV
\COPY ps FROM 'playstatistics.csv' CSV HEADER;
-- SELECT project, talkingbookid from ps;
INSERT INTO playstatistics SELECT * FROM ps
    ON CONFLICT ON CONSTRAINT playstatistics_pk DO
        UPDATE SET
            contentpackage=EXCLUDED.contentpackage,
            community=EXCLUDED.community,
            started=EXCLUDED.started,
            quarter=EXCLUDED.quarter,
            half=EXCLUDED.half,
            threequarters=EXCLUDED.threequarters,
            completed=EXCLUDED.completed,
            played_seconds=EXCLUDED.played_seconds,
            survey_taken=EXCLUDED.survey_taken,
            survey_applied=EXCLUDED.survey_applied,
            survey_useless=EXCLUDED.survey_useless,
            stats_timestamp=EXCLUDED.stats_timestamp,
            deployment_timestamp=EXCLUDED.deployment_timestamp,
            recipientid=EXCLUDED.recipientid,
            deployment_uuid=EXCLUDED.deployment_uuid;

-- #2: Update tbsdeployed
-- Create a table with the right shape.
CREATE TEMPORARY TABLE ds AS (SELECT * FROM tbsdeployed WHERE FALSE);
-- Populate from CSV
\COPY ds FROM 'tbsdeployed.csv' CSV HEADER;
INSERT INTO tbsdeployed SELECT * FROM ds
     ON CONFLICT ON CONSTRAINT tbdeployments_pkey DO
        UPDATE SET
              recipientid=EXCLUDED.recipientid,
              project=EXCLUDED.project,
              deployment=EXCLUDED.deployment,
              contentpackage=EXCLUDED.contentpackage,
              firmware=EXCLUDED.firmware,
              location=EXCLUDED.location,
              coordinates=EXCLUDED.coordinates,
              username=EXCLUDED.username,
              tbcdid=EXCLUDED.tbcdid,
              action=EXCLUDED.action,
              newsn=EXCLUDED.newsn,
              testing=EXCLUDED.testing;

SELECT DISTINCT project, tb_version, COUNT(DISTINCT talkingbookid) from (
      SELECT project, recipientid, talkingbookid,
          CASE WHEN talkingbookid ILIKE '%.%.%.%' THEN 'TB-2' ELSE 'TB-1' END AS "tb_version"
        FROM tbsdeployed
    ) tbc GROUP BY project, tb_version ORDER BY tb_version, project;
-- FROM tbsdeployed WHERE talkingbookid ILIKE '%.%.%.%';
SELECT DISTINCT timestamp, project, deployment, talkingbookid FROM playstatistics WHERE talkingbookid ILIKE '%.%.%.%' LIMIT 10;

commit;
SQL_EOF

