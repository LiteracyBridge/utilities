PROJECT = roleHelper
PYTHON = python3.8
VIRTUAL_ENV = .roleHelperEnv
FUNCTION_NAME = roleHelper
FUNCTION_HANDLER = lambda_handler
FUNCTION_ROLE = arn:aws:iam::856701711513:role/lambda-checkout
FUNCTION_DESCRIPTION = User role helper
AWS_REGION = us-west-2

# Default commands
help: helper
install: virtual
build: clean_package build_package_tmp copy_python remove_unused zip

helper:
	echo "install, update, build, clean_package, upload_lambda"

virtual:
	@echo "--> Setup and activate virtualenv"
	if test ! -d "$(VIRTUAL_ENV)"; then python3 -m venv --system-site-packages $(VIRTUAL_ENV); fi
	@echo "Use '. $(VIRTUAL_ENV)/bin/activate' to activate virtual environment."

clean_package:
	rm -rf ./package/* 

build_package_tmp:
	mkdir -p ./package/tmp
	cp -a ./$(PROJECT)/. ./package/tmp/

copy_python:
	if test -d $(VIRTUAL_ENV)/lib; then cp -a $(VIRTUAL_ENV)/lib/$(PYTHON)/site-packages/. ./package/tmp/; fi
	if test -d $(VIRTUAL_ENV)/lib64; then cp -a $(VIRTUAL_ENV)/lib64/$(PYTHON)/site-packages/. ./package/tmp/; fi

remove_unused:
	rm -rf ./package/tmp/.gitignore
	rm -rf ./package/tmp/.idea
	rm -rf ./package/tmp/boto3*
	rm -rf ./package/tmp/botocore*
	rm -rf ./package/tmp/urllib3*
	rm -rf ./package/tmp/dateutil*
	rm -rf ./package/tmp/jmespath*
	rm -rf ./package/tmp/six*
	rm -rf ./package/tmp/python_dateutil*
	rm -rf ./package/tmp/easy_install*
	rm -rf ./package/tmp/pip*
	rm -rf ./package/tmp/pkg_resources*
	rm -rf ./package/tmp/s3transfer*
	rm -rf ./package/tmp/setuptools*
	rm -rf ./package/tmp/wheel*
	find ./package/tmp -iname __pycache__ -type d -exec rm -rf {} +
	rm -rf ./package/tmp/amplio-*.egg-info

	rm -rf ./package/tmp/et_xmlfile*
	rm -rf ./package/tmp/jdcal*
	rm -rf ./package/tmp/openpyxl*
	rm -rf ./package/tmp/pip*
    
	rm -rf ./package/tmp/awscli*
	rm -rf ./package/tmp/colorama*
	rm -rf ./package/tmp/pyasn1*
	rm -rf ./package/tmp/rsa*

	rm -rf ./package/tmp/amplio/programspec*


zip:
	cd ./package/tmp && zip -r ../$(PROJECT).zip .
	date

update:
	pip install --upgrade amplio

upload_lambda:
	aws lambda update-function-code --function-name $(FUNCTION_NAME) --zip-file fileb://./package/$(PROJECT).zip
	date

create_lambda:
	aws lambda create-function --function-name $(FUNCTION_NAME) \
	--runtime $(PYTHON) --description "$(FUNCTION_DESCRIPTION)" \
	--handler $(FUNCTION_HANDLER) --role $(FUNCTION_ROLE) \
	--zip-file fileb://./package/$(PROJECT).zip

