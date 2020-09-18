ACM Utilities
=============

This is a collection of utility code for Python3 in ACM applications. The following
packages are available.

Program Specification
=====================

Code to read, validate, and export from Program Specifications, in the form of an 
Excel compatible spreadsheet.


User Roles
==========

Load, update, and query user roles in projects.



Note on Development Environments
================================

It may be convenient to run a pypi mirror locally, to let you easily install and update
these packages.

You can:
* Create a directory named ~/pypi.
* Use the "make serve" command to copy the Amplio package there.
* Use  
    `cd ~/pypi`  
    `bash -c "nohup sh -c 'python3 -m http.server 8765' &"`  
to start the server.
* Use  
    `export PIP_EXTRA_INDEX_URL="http://localhost:8765"`  
to tell pip where to look.