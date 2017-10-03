'use strict';

var myLambda = require( './listAcmCheckouts' );

var event = {
    username: 'billev2k',
    email:'billev2k@gmail.com',
    request: {
        userAttributes: {
            email: 'bill@centreforbcc.com'
        }
    }
}
var context = {}

myLambda.handler(event, context, (err,result)=>{
    if (err) {
        console.log('error:', err);
        return
    }
    console.log(result);
    process.exit(0)
});