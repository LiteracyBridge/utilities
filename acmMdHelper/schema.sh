#!/usr/bin/env zsh

psql=/Applications/Postgres.app/Contents/Versions/latest/bin/psql
#dbcxn=(--host=localhost --port=5432 --username=lb_data_uploader --dbname=dashboard)
dbcxn=(--host=lb-device-usage.ccekjtcevhb7.us-west-2.rds.amazonaws.com --port=5432 --username=lb_data_uploader --dbname=dashboard)

${psql} ${dbcxn} <<SQL_EOF
begin transaction;

-- ALTER TABLE categoriesinpackage RENAME COLUMN "order" TO position;
-- ALTER TABLE contentinpackage RENAME COLUMN "order" TO position;

\echo =-=-=-=-=-=-=-= Fix dups in CATEGORIESINPACKAGE =-=-=-=-=-=-=-=
SELECT COUNT(project) FROM categoriesinpackage;
SELECT * FROM (SELECT DISTINCT project, contentpackage, categoryid, position, COUNT(position) AS count
                 FROM categoriesinpackage
                GROUP BY project, contentpackage, categoryid, position
                ORDER BY project, contentpackage, categoryid) x
 WHERE count>1;

DELETE FROM categoriesinpackage
 WHERE ctid IN
    (SELECT ctid FROM
          (SELECT ctid, ROW_NUMBER() OVER( PARTITION BY project, contentpackage, categoryid ORDER BY ctid) AS row_num
             FROM categoriesinpackage) t
     WHERE t.row_num > 1);

SELECT COUNT(project) FROM categoriesinpackage;
SELECT * FROM (SELECT DISTINCT project, contentpackage, categoryid, position, COUNT(position) AS count
                 FROM categoriesinpackage
                GROUP BY project, contentpackage, categoryid, position
                ORDER BY project, contentpackage, categoryid) x
 WHERE count>1;

\echo =-=-=-=-=-=-=-= Fix dups in PACKAGESINDEPLOYMENT =-=-=-=-=-=-=-=
SELECT COUNT(project) FROM packagesindeployment;
SELECT * FROM (SELECT DISTINCT project, deployment, contentpackage, languagecode, groups, packagename, COUNT(packagename) AS count
                 FROM packagesindeployment
                GROUP BY project, deployment, contentpackage, languagecode, groups, packagename
                ORDER BY project, deployment, contentpackage, languagecode, groups, packagename) x
 WHERE count>1;

DELETE FROM packagesindeployment
 WHERE ctid IN
    (SELECT ctid FROM
          (SELECT ctid, ROW_NUMBER() OVER( PARTITION BY project, deployment, contentpackage, languagecode, groups ORDER BY ctid) AS row_num
             FROM packagesindeployment) t
     WHERE t.row_num > 1);

SELECT COUNT(project) FROM packagesindeployment;
SELECT * FROM (SELECT DISTINCT project, deployment, contentpackage, languagecode, groups, packagename, COUNT(packagename) AS count
                 FROM packagesindeployment
                GROUP BY project, deployment, contentpackage, languagecode, groups, packagename
                ORDER BY project, deployment, contentpackage, languagecode, groups, packagename) x
 WHERE count>1;

ALTER TABLE categories ADD PRIMARY KEY (categoryid, categoryname, projectcode);
ALTER TABLE categoriesinpackage ADD PRIMARY KEY (project, contentpackage, categoryid);
ALTER TABLE languages ADD PRIMARY KEY (languagecode, language, projectcode);
ALTER TABLE packagesindeployment ADD PRIMARY KEY (project, deployment, contentpackage, languagecode, groups);

ALTER TABLE contentinpackage DROP CONSTRAINT contentinpackage_pkey;
select * from contentinpackage where contentpackage != upper(contentpackage);
update contentinpackage
   set contentpackage = upper(contentpackage), project = upper(project);
select * from contentinpackage where contentpackage != upper(contentpackage);

\echo =-=-=-=-=-=-=-= Fix dups in CONTENTINPACKAGE =-=-=-=-=-=-=-=
SELECT COUNT(project) FROM contentinpackage;
SELECT * FROM (SELECT DISTINCT project, contentpackage, contentid, categoryid, position, count(position) as count
                 FROM contentinpackage
                GROUP BY project, project, contentpackage, contentid, categoryid, position
                ORDER BY project, contentpackage, contentid, categoryid, position) x
 WHERE count>1;

DELETE FROM contentinpackage
 WHERE ctid IN
    (SELECT ctid FROM
          (SELECT ctid, ROW_NUMBER() OVER( PARTITION BY project, contentpackage, contentid, categoryid, position ORDER BY ctid) AS row_num
             FROM contentinpackage) t
     WHERE t.row_num > 1);

SELECT COUNT(project) FROM contentinpackage;
SELECT * FROM (SELECT DISTINCT project, contentpackage, contentid, categoryid, position, COUNT(position) AS count
                 FROM contentinpackage
                GROUP BY project, contentpackage, contentid, categoryid, position
                ORDER BY project, contentpackage, contentid, categoryid, position) x
 WHERE count>1;

ALTER TABLE contentinpackage ADD PRIMARY KEY (project, contentpackage, contentid, categoryid, position);


\d categories
\d categoriesinpackage
\d contentinpackage
\d languages
\d packagesindeployment

abort;
--commit;
SQL_EOF
