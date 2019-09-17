PROJECT = dashboardQueries
PYTHON = python3.7
VIRTUAL_ENV = venv
FUNCTION_NAME = service
FUNCTION_HANDLER = lambda_handler
AWS_REGION = us-west-2
LAMBDA_ROLE = arn:aws:iam::856701711513:role/lambda-statistics-query

# Default commands
help: helper
install: virtual
build: clean_package build_package_tmp copy_python copy_psycopg2 remove_unused zip

helper:
	echo "install, build, clean_package, upload_lambda"

virtual:
	@echo "--> Setup and activate virtualenv"
	if test ! -d "$(VIRTUAL_ENV)"; then python3 -m venv $(VIRTUAL_ENV); fi
	@echo "Use '. $(VIRTUAL_ENV)/bin/activate' to activate virtual environment."

clean_package:
	rm -rf ./package/* 

build_package_tmp:
	mkdir -p ./package/tmp/lib
	cp -a ./$(PROJECT)/. ./package/tmp/

copy_python:
	if test -d $(VIRTUAL_ENV)/lib; then \
	    cp -a $(VIRTUAL_ENV)/lib/$(PYTHON)/site-packages/. ./package/tmp/; \
	fi	
	if test -d $(VIRTUAL_ENV)/lib64; then \
	    cp -a $(VIRTUAL_ENV)/lib64/$(PYTHON)/site-packages/. ./package/tmp/; \
	fi

remove_unused:
	rm -rf ./package/tmp/wheel*
	rm -rf ./package/tmp/easy-install*
	rm -rf ./package/tmp/setuptools*
	rm -rf ./package/tmp/pip*

copy_psycopg2:
	rm -rf ./package/tmp/psycopg2*
	cp -a ./linux-amd64/psycopg2 ./package/tmp/

zip:
	cd ./package/tmp && zip -r ../$(PROJECT).zip .

upload_lambda:
	aws lambda update-function-code --function-name dashboardQueries --zip-file fileb://./package/$(PROJECT).zip

