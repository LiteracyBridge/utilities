VIRTUAL_ENV = $(shell pipenv --venv 2>/dev/null)

# Default
help: helper
build: builder

helper:
	echo "help, build"

builder:
	source $(VIRTUAL_ENV)/bin/activate; \
	python setup.py sdist

