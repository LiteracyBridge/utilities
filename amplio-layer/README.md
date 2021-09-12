amplio-layer
============

A project to bring together utility code shared among multiple of our Lambda functions.

To use the amplio-layer in a Python Lambda function, add the layer to the function. In a makefile that could look like this:
```makefile
# Use either of these to find the latest layer version.
# aws lambda list-layers --query "Layers[?LayerName=='amplio-layer'].LatestMatchingVersion.LayerVersionArn|[0]"
# aws lambda list-layer-versions  --layer-name amplio-layer --query 'LayerVersions[0].LayerVersionArn'
update_layer:
    aws lambda update-function-configuration --function-name $(FUNCTION_NAME) \
    --layers $(shell aws lambda list-layers --query "Layers[?LayerName=='amplio-layer'].LatestMatchingVersion.LayerVersionArn|[0]")
```
To remove the layer code from the uploaded Lambda package, use the `rm_layer.sh` script from this directory:
```makefile
remove_unused:
	../amplio-layer/rm_layer.sh
    . . .
```
