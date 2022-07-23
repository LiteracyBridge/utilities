PROJECT = tableauHelper
PYTHON_VER = 3.9
PYTHON = python$(PYTHON_VER)
VIRTUAL_ENV = $(shell pipenv --venv 2>/dev/null)
FUNCTION_NAME = tableauHelper
FUNCTION_HANDLER = main.lambda_function
FUNCTION_ROLE = arn:aws:iam::856701711513:role/lambda-checkout
FUNCTION_DESCRIPTION = Obtain a sign-in JWT for a Tableau Connected App
AWS_REGION = us-west-2

PY_LIBS = wheel easy_install setup_tools setuptools pip six _virtualenv distutils _distutils_hack
AWS_LIBS = boto3 botocore s3transfer

help: helper
install: virtual
build: clean_package build_package_tmp copy_python remove_unused zip

helper:
	echo "install, update, build, clean_package, upload_lambda"
	@echo "pipenv is $(VIRTUAL_ENV)"

virtual:
	@echo "--> Setup and activate virtualenv"
	if test ! -d "$(VIRTUAL_ENV)"; then pipenv --python $(PYTHON_VER) ; fi
	@echo "Use 'pipenv shell' to activate virtual environment."


clean_package:
	rm -rf ./package/*

build_package_tmp:
	mkdir -p ./package/tmp
	cp -a ./main.py ./package/tmp/

copy_python:
	if test -d $(VIRTUAL_ENV)/lib; then cp -a $(VIRTUAL_ENV)/lib/$(PYTHON)/site-packages/. ./package/tmp/; fi
	if test -d $(VIRTUAL_ENV)/lib64; then cp -a $(VIRTUAL_ENV)/lib64/$(PYTHON)/site-packages/. ./package/tmp/; fi

remove_unused:
	pwd
	ls
	ls ./package
	ls ./package/tmp

	@for f in $(PY_LIBS) $(AWS_LIBS); do \
	    rm -rf ./package/tmp/$$f*; \
	    rm -rf ./package/tmp/__pycache__/$$f*; \
	done

	rm -rf ./package/tmp/.gitignore
	rm -rf ./package/tmp/.idea

	rm -rf ./package/tmp/amplio/programspec*

	rm -rf ./package/tmp/openpyxl*
	rm -rf ./package/tmp/et_xmlfile*
	rm -rf ./package/tmp/amplio-*
	rm -rf ./package/tmp/dateutil*
	rm -rf ./package/tmp/docutils*
	rm -rf ./package/tmp/jmespath*
	rm -rf ./package/tmp/python_dateutil*
	rm -rf ./package/tmp/six*
	rm -rf ./package/tmp/jdcal*
	rm -rf ./package/tmp/urllib3*

	rm -rf ./package/tmp/pkg_resources

	rm -rf ./package/tmp/awscli*
	rm -rf ./package/tmp/colorama*
	rm -rf ./package/tmp/rsa*
	rm -rf ./package/tmp/pyasn1*

	rmdir ./package/tmp/__pycache__


zip:
	cd ./package/tmp && zip -r ../$(PROJECT).zip .
	date


upload_lambda:
	aws lambda update-function-code --function-name $(FUNCTION_NAME) --zip-file fileb://./package/$(PROJECT).zip
	date

create_lambda:
	aws lambda create-function --function-name $(FUNCTION_NAME) \
	--runtime $(PYTHON) --description "$(FUNCTION_DESCRIPTION)" \
	--handler $(FUNCTION_HANDLER) --role $(FUNCTION_ROLE) \
	--zip-file fileb://./package/$(PROJECT).zip

