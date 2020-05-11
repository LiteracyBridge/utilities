Utilities
=========

A collection of:
* Stand alone tools 
* AWS Lambda functions
* Shared Python code


Stand Alone Tools
=================
|Name|Description|
|----|-----------|
|README.md|This file|
|logfileS3Consolidator||
|psutil|A stand-alone program spec utility application. Validates program specs. Reconciles spec with directory (mostly for legacy).|
|stats_log_consolidation||
|newAcm|Helps create a new ACM database. Validates that no files or table entries already exist, then creates template database, shares in Dropbox, creates records in PostgreSQL and DynamoDB.|


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

Shared Python Code
==================
In the pypi/amplio directory.

|Name|Description|
|----|-----------|
|programspec|Read a program specification spreadsheet, validate, export to .csv data.|
|rolemanager|Query and update 'organizations' and 'programs' tables in DyanmoDB. Manages roles for users and organizations.|

