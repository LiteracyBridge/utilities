PROJECT = psutil
PYTHON = python3.7
# pyinstaller does not yet (2020-04-01) work with python 3.8
VIRTUAL_ENV = .virtual376

# Default commands
help: helper
# install: virtual
build: build_package_tmp

helper:
	echo "upgrade, build"

build_package_tmp:
	pyinstaller --onefile psutil.py

# virtual:
# 	@echo "--> Setup and activate virtualenv"
# 	if test ! -d "$(VIRTUAL_ENV)"; then python3 -m venv $(VIRTUAL_ENV); fi
# 	@echo "Use '. $(VIRTUAL_ENV)/bin/activate' to activate virtual environment."

upgrade:
	pip3 install --upgrade amplio


