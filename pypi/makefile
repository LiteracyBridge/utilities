PACKAGE = amplio
PYTHON = python3.8
VIRTUAL_ENV = progspec_env

# Default commands
help: helper
install: virtual
build: clean build_package_tmp

helper:
	echo "install, build, clean_package, serve"

virtual:
	@echo "--> Setup and activate virtualenv"
	if test ! -d "$(VIRTUAL_ENV)"; then python3 -m venv $(VIRTUAL_ENV); fi
	@echo "Use '. $(VIRTUAL_ENV)/bin/activate' to activate virtual environment."

clean_package: clean

clean:
	rm -rf ./dist

build_package_tmp:
	awk -f setup.awk setup.py >setup.new; if [ $$? == 0 ]; then cp setup.new setup.py; fi
	if test -f setup.new; then rm setup.new; fi
	python3 setup.py sdist
	mkdir -p ./dist/$(PACKAGE)
	mv ./dist/*.gz ./dist/$(PACKAGE)/

serve:
	@echo "Copying to local ~/pypi."
	cp -r dist/* ~/pypi/

