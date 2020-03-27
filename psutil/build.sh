#!/usr/bin/env bash
deactivate
. ~/virtualenvs/programspec/bin/activate
pyinstaller --onefile psutil.py
