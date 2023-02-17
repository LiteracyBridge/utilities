PROJECT = ufKeysHelper
PYTHON = python3.9
VIRTUAL_ENV = $(shell pipenv --venv 2>/dev/null)
FUNCTION_NAME = ufKeysHelper
FUNCTION_HANDLER = lambda_handler.lambda_router
FUNCTION_ROLE = arn:aws:iam::856701711513:role/lambda-checkout
CRYPTO_LAYER = cryptography
FUNCTION_DESCRIPTION = Manager for UF encryption public/private key pairs.
AWS_REGION = us-west-2

PY_LIBS = wheel easy_install setup_tools setuptools pip six _virtualenv distutils _distutils_hack
AWS_LIBS = boto3 botocore s3transfer

# Default commands
help: helper
install: virtual
build: clean_package build_package_tmp copy_python remove_unused zip

helper:
	echo "install, update, build, clean_package, upload_lambda"

virtual:
	@echo "--> Setup and activate virtualenv"
	if test ! -d "$(VIRTUAL_ENV)"; then pipenv --python $(PYTHON_VER) ; fi
	@echo "Use 'pipenv shell' to activate virtual environment."

clean_package:
	rm -rf ./package/*

build_package_tmp:
	mkdir -p ./package/tmp
	cp -a ./$(PROJECT)/. ./package/tmp/
# 	cp -a ./*.py ./package/tmp/

copy_python:
	if test -d $(VIRTUAL_ENV)/lib; then cp -a $(VIRTUAL_ENV)/lib/$(PYTHON)/site-packages/. ./package/tmp/; fi
	if test -d $(VIRTUAL_ENV)/lib64; then cp -a $(VIRTUAL_ENV)/lib64/$(PYTHON)/site-packages/. ./package/tmp/; fi

remove_unused:
	pwd
	ls
	ls ./package
	ls ./package/tmp

	../amplio-layer/rm_layer.sh

	@for f in $(PY_LIBS) $(AWS_LIBS); do \
	    rm -rf ./package/tmp/$$f*; \
	    rm -rf ./package/tmp/__pycache__/$$f*; \
	done

	rm -rf ./package/tmp/wheel*
	rm -rf ./package/tmp/easy_install*
	rm -rf ./package/tmp/setuptools*
	rm -rf ./package/tmp/pip*
	rm -rf ./package/tmp/botocore*
	rm -rf ./package/tmp/boto3*
	rm -rf ./package/tmp/s3transfer*
	rm -rf ./package/tmp/.gitignore
	rm -rf ./package/tmp/.idea
	rm -rf ./package/tmp/pkg_resources*
	find ./package/tmp -iname __pycache__ -type d -exec rm -rf {} +
	rm -rf ./package/tmp/amplio-*.egg-info
	rm -rf ./package/tmp/dateutil*
	rm -rf ./package/tmp/docutils*
	rm -rf ./package/tmp/jmespath*
	rm -rf ./package/tmp/python_dateutil*
	rm -rf ./package/tmp/six*

	rm -rf ./package/tmp/cffi*
	rm -rf ./package/tmp/cryptography*
	rm -rf ./package/tmp/_cffi_backend.cpython-39-darwin.so

	rm -rf ./package/tmp/jdcal*
	rm -rf ./package/tmp/openpyxl*
	rm -rf ./package/tmp/pip*
	rm -rf ./package/tmp/et_xmlfile*

	rm -rf ./package/tmp/awscli*
	rm -rf ./package/tmp/colorama*
	rm -rf ./package/tmp/rsa*
	rm -rf ./package/tmp/pyasn1*


zip:
	cd ./package/tmp && zip -r ../$(PROJECT).zip .
	date

update:
	pip install --upgrade amplio

upload_lambda:
	aws lambda update-function-code --function-name $(FUNCTION_NAME) --zip-file fileb://./package/$(PROJECT).zip
	date

# Use either of these to find the latest layer version.
# aws lambda list-layers --query "Layers[?LayerName=='amplio-layer'].LatestMatchingVersion.LayerVersionArn|[0]"
# aws lambda list-layer-versions  --layer-name amplio-layer --query 'LayerVersions[0].LayerVersionArn'
update_layer:
	aws lambda update-function-configuration --function-name $(FUNCTION_NAME) \
	--layers $(shell aws lambda list-layers --query "Layers[?LayerName=='$(CRYPTO_LAYER)'].LatestMatchingVersion.LayerVersionArn|[0]")

create_lambda:
	aws lambda create-function --function-name $(FUNCTION_NAME) \
	--runtime $(PYTHON) --description "$(FUNCTION_DESCRIPTION)" \
	--handler $(FUNCTION_HANDLER) --role $(FUNCTION_ROLE) \
	--zip-file fileb://./package/$(PROJECT).zip

