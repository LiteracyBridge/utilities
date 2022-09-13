PYTHON = python3.9
ZIPFILE = amplio-layer.zip
LAYER_NAME = amplio-layer
DESCRIPTION = 'Python3 code shared among Amplio Lambda functions.'

help: helper


helper:
	echo "upgrade, build, upload"
	echo $(PWD)


upgrade:
	pip3 install -r requirements.txt --upgrade -t python

build:
	zip -r $(ZIPFILE) python

upload:
	aws lambda publish-layer-version --layer-name $(LAYER_NAME) --description $(DESCRIPTION) --zip-file fileb://$(PWD)/$(ZIPFILE)

