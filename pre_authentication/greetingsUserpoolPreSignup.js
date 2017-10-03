var aws = require('aws-sdk');
aws.config.update({region: 'us-west-2'})

var doc = require('dynamodb-doc');
var db = new doc.DynamoDB();

/**
 * Retrieve information about user from DyanmoDB. If the user has their own record, return it. Otherwise,
 * if their organization (as in user@example.org) has a record, return that. Otherwise fail.
 * This lets us set up defaults for literacybridge.org or centreforbcc.com, lets us whitelist user@gmail.com
 * and keeps everyone else out.
 * @param email The user's email address
 * @returns {Promise.<TResult>} Resolves with user's information from dynamodb.
 */
function getUserInfo(email) {
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
    
    email = email.toLowerCase();
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

exports.handler = function (event, context, callback) {
    var err = function (msg) {
        callback(msg)
    }
    
    // Verify that there's an email address (should be -- required by user pool)
    var email = event.request.userAttributes.email;
    if (!email) {
        err('No email address provided');
    }
    
    getUserInfo(email)
        .then((data) => {
            callback(null, event)
        }, (err) => {
            callback(err)
        })
}