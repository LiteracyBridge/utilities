Utilities
=========

A collection of:
* Stand alone tools 
* AWS Lambda functions
* Shared Python code

<br>
Stand Alone Tools
=================
|Name|Description|
|----|-----------|
|README.md|This file|
|logfileS3Consolidator||
|psutil|A stand-alone program spec utility application. Validates program specs. Reconciles spec with directory (mostly for legacy).|
|stats_log_consolidation||
|newAcm|Helps create a new ACM database. Validates that no files or table entries already exist, then creates template database, shares in Dropbox, creates records in PostgreSQL and DynamoDB.|
|ufHelper|Utility functions in support of extracting user feedback recordings from statistics, and making them available for listening.|


<br>
Lambda Functions
================
|Directory|Lambda|API Gateway|Description|
|---------|------|-----------|-----------|
|acmCheckOut|acmCheckOut|CheckOut|Used by the ACM to check out the database for a program.|
|listAcmCheckouts|listAcmCheckouts|CheckOut|Provides a list of checkouts to the dashboard.|
|psHelper|programSpecification|programSpecification|AWS Lambda function to manage server side of program spec. Upload, download, approve.|
|tblHelper|tblHelper|tblHelper|Manages TB serial numbers.|
|queryHelper|dashboardQueries|dashboardQueries|AWS Lambda function to help with dashboard queries. Re-writes queries to ensure that access is appropriately limited for all users.|
|roleHelper|roleHelper|roleHelper|Manages queries and updates of roles for dashboard. Apps that need role information should use rolemanager from pypi, or program information provided with Cognito signin.|
|cognitoTriggerHandler|cognitoTriggerHandler||Pre-validates emails before Cognito sign up. Adds user's roles to claims when authenticating.|
|psHelper|programSpecification|programSpecification|Parses a program specification .xlsx file into component .csv files. Valicates, and persists these files in S3.|
|twbxHelper|twbxHelper|twbxHelper|Uploads Tableau workbooks. Updates internal data with program specific data. This is being replaced with actual Tableau dashboard pages (on Tableau).|
|echo|echo|echo|An extremely simple Lambda function that simply returns its input.|

<br>
Lambda Layer
============
The amplio-layer directory contains a Lambda "layer" project, hosting code that can be shared among all of the Python Lambda functions. Currently has
programspec and rolemanager, plus pg8000 PostgreSQL database layer.

Note that the actual code for programspec and rolemanager still live in the pypi directory. The amplio-layer project exists to pull together the components
of the layer, and to publish them to AWS.

<br>
Shared Python Code
==================
In the pypi/amplio directory.

|Name|Description|
|----|-----------|
|programspec|Read a program specification spreadsheet, validate, export to .csv data.|
|rolemanager|Query and update 'organizations' and 'programs' tables in DyanmoDB. Manages roles for users and organizations.|

