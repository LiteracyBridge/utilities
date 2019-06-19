PROJECT = checkout
PYTHON = python3.7
VIRTUAL_ENV = vcheckout
FUNCTION_NAME = arn:aws:lambda:us-west-2:856701711513:function:acmCheckOut
FUNCTION_HANDLER = lambda_handler
AWS_REGION = us-west-2
LAMBDA_ROLE = arn:aws:iam::856701711513:role/lambda-statistics-query

# Default commands
help: helper
install: virtual
build: clean_package build_package_tmp copy_python remove_unused zip

helper:
	echo "install, build, clean_package, upload_lambda"

virtual:
	@echo "--> Setup and activate virtualenv"
	if test ! -d "$({)VIRTUAL_ENV)"; then \
		python3 -m venv $(VIRTUAL_ENV); \
	fi
	@echo "Use '. $(VIRTUAL_ENV)/bin/activate' to activate virtual environment."

clean_package:
	rm -rf ./package/* 

build_package_tmp:
	mkdir -p ./package/tmp
	cp -a ./$(PROJECT)/. ./package/tmp/

copy_python:
# 	if test -d $(VIRTUAL_ENV)/lib; then \
# 	    cp -a $(VIRTUAL_ENV)/lib/$(PYTHON)/site-packages/. ./package/tmp/; \
# 	fi	
# 	if test -d $(VIRTUAL_ENV)/lib64; then \
# 	    cp -a $(VIRTUAL_ENV)/lib64/$(PYTHON)/site-packages/. ./package/tmp/; \
# 	fi

remove_unused:
	rm -rf ./package/tmp/wheel*
	rm -rf ./package/tmp/easy-install*
	rm -rf ./package/tmp/setuptools*
	rm -rf ./package/tmp/pip*
	rm -rf ./package/tmp/.gitignore

zip:
	cd ./package/tmp && zip -r ../$(PROJECT).zip .

upload_lambda:
	aws lambda update-function-code --function-name $(FUNCTION_NAME) --zip-file fileb://./package/$(PROJECT).zip
