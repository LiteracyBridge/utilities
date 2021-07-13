PROJECT = psutil
PYTHON = python3.8
VIRTUAL_ENV = psutilenv

# Default commands
help: helper
install: virtual
build: build_package_tmp

helper:
	echo "install, update, build"

build_package_tmp:
	pyinstaller --onefile psutil.py

virtual:
	@echo "--> Setup and activate virtualenv"
	if test ! -d "$(VIRTUAL_ENV)"; then python3 -m venv $(VIRTUAL_ENV); fi
	@echo "Use '. $(VIRTUAL_ENV)/bin/activate' to activate virtual environment."

update:
	pip3 install --upgrade amplio


