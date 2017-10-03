var aws = require('aws-sdk');
aws.config.update({region: 'us-west-2'})

var doc = require('dynamodb-doc');
var db = new doc.DynamoDB();

/**
 * Retrieve information about user from DyanmoDB. If the user has their own record, return it. Otherwise,
 * if their organization (as in user@example.org) has a record, return that. Otherwise fail.
 * This lets us set up defaults for literacybridge.org or centreforbcc.com, lets us whitelist user@gmail.com
 * and keeps everyone else out.
 * @param event The event, which must contain a 'params' object with string 'email', the user's email address
 * @returns {Promise.<TResult>} Resolves with user's information from dynamodb.
 */
function getUserInfo(event) {
    function parseUserRecord(Item) {
        var result = {
            edit: '',
            view: '',
            admin: false
        }
        result.edit = Item.edit || '';
        result.view = Item.edit || Item.view || '';
        result.admin = !!Item.admin;
        // Copy the rest of the properties to 'other'.
        var other;
        Object.keys(Item).forEach((k) => {
            if (!result.hasOwnProperty(k)) {
                other = other || {};
                other[k] = Item[k]
            }
        })
        if (other) {
            result.other = other
        }
        return result;
    }
    
    var email = event.claims.email.toLowerCase();
    params = {
        TableName: 'acm_users',
        Key: {email: email}
    };
    
    return db.getItem(params).promise()
        .then((data) => {
            if (data.Item) {
                return parseUserRecord(data.Item);
            }
            // Didn't get record for user, try just the organization.
            var parts = email.split('@');
            if (parts.length != 2) {
                throw 'Not a valid email address: ' + email;
            }
            params.Key.email = parts[1];
            return db.getItem(params).promise()
                .then((data) => {
                    if (data.Item) {
                        return parseUserRecord(data.Item)
                    }
                    throw 'Unknown user and organization';
                });
            
        })
}

/**
 * Router function for user management utility.
 * @param event
 * @param context
 * @param callback
 */
exports.handler = function (event, context, callback) {
    
    // Verify that there's an email address (should be -- required by user pool)
    if (!event.claims || !event.claims.email) {
        callback('missing email');
        return;
    }
    if (!event.body || !event.body.action) {
        callback('missing action');
        return;
    }
    
    var promise;
    
    switch (event.body.action) {
        case 'getUserInfo':
            promise = getUserInfo(event);
            break;
    }
    
    if (promise) {
        promise.then((data) => {
            callback(null, data)
        }, (err) => {
            callback(err)
        })
    } else {
        callback('unknown action: '+JSON.stringify(event));
    }
}