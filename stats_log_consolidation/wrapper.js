'use strict';

var myLambda = require( './index' );

myLambda.handler({}, {}, (err,result)=>{
    if (err) {
        console.log('error:', err);
        return
    }
    console.log(result);
    process.exit(0)
});