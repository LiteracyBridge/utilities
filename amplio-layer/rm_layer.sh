#!/usr/bin/env bash
#
# Add this to the remove_unused step of a Lambda function with a makefile like this:
#build: clean_package build_package_tmp copy_python remove_unused zip
#
#clean_package:
#	rm -rf ./package/*
#
#build_package_tmp:
#	mkdir -p ./package/tmp
#	cp -a ./$(PROJECT)/. ./package/tmp/
#
#copy_python:
#	if test -d $(VIRTUAL_ENV)/lib; then cp -a $(VIRTUAL_ENV)/lib/$(PYTHON)/site-packages/. ./package/tmp/; fi
#	if test -d $(VIRTUAL_ENV)/lib64; then cp -a $(VIRTUAL_ENV)/lib64/$(PYTHON)/site-packages/. ./package/tmp/; fi
#
#remove_unused:
#	../amplio-layer/rm_layer.sh
#	rm -rf ./package/tmp/wheel*
#	rm -rf ./package/tmp/easy_install*
#	rm -rf ./package/tmp/setuptools*
# rm . . .
#
#zip:
#	cd ./package/tmp && zip -r ../$(PROJECT).zip .
#
#
# Use this to update the layer configuration to the latest layer.
#update_layer:
#	aws lambda update-function-configuration --function-name $(FUNCTION_NAME) \
#	--layers $(shell aws lambda list-layer-versions  --layer-name amplio-layer --query 'LayerVersions[0].LayerVersionArn')
#
for f in `ls -d ../amplio-layer/python/*info` sqlalchemy dateutil; do
  fn=${f##*/}
  p=${fn%%-*}
  to_rem="./package/tmp/${p}*"
  echo "rm -rf ${to_rem}"
  rm -rf ${to_rem}
done
true
