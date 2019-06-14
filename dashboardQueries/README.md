dashboardQueries
================

This function provides an interface between the dashboard web app and
the Postgresql database. The function accepts a simplified query, parses
it, validates it against pre-defined allowed queries, and then executes
the real Postgresql query. Results are returned in a .csv format.

Because lambda does not nativly support querying Postgresql, we need to
include the psycopg2 package in the upload. And because that package
includes an architecture specific binary, we must tweak the packages
before we zip and upload them.

For convenience, a makefile is also included to create the appropriate
.zip file, and to upload it to AWS.

|File|Description|
|----|-----------|
|README.md|This file.|
|dashboardQueries|The directory containing the Python code.|
|linux-amd64|A directory with a pre-built Linux amd-64 binary for psycopg2.|
|makefile|The makefile, with functions to set up the virtual environment, to create the package, and to upload the package to Lambda.|
|package|A transient directory, created as needed. The uploadable .zip is built here.|
|venv|The virtual environment directory. It must be here, with this name, for the makefile to work.|


