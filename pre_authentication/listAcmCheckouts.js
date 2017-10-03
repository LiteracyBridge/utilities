console.log('Loading function');
var aws = require('aws-sdk');
aws.config.update({region: 'us-west-2'})

var doc = require('dynamodb-doc');
var db = new doc.DynamoDB();
var defaultFilter = /^(LBG-DEMO|DEMO)$/
var simpleList = /^([-\w]+\|)*?([-\w]+)$/
var acmNameMatch = /ACM-([-\w]+?)(-FB-[-\w]*)?$/


/**
 * Look up the user from their email. If not found, look for just the organization, to see if there is a default.
 * @param email of the user.
 * @returns {Promise.<TResult>} Resolved with {edit:'', view:'', admin:true/false, other:{<rest of row>}}
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
        console.log('view: '+result.view);
        return result;
    }
    
    email = email.toLowerCase();
    console.log('Retrieving info for '+email);
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
 * Given userInfo, build a filter that will accept the ACMs that the user should be able to see.
 * @param userInfo
 * @returns {*}
 */
function getFilterForUser(userInfo) {
    var filter = userInfo.view;
    if (simpleList.test(filter)) {
        filter = '^(' + filter + ')$';
    }
    console.log('filter: '+filter)
    return RegExp(filter);
}

/**
 * Given a filter, scan the ACM checkouts database, and return all items matching the filter.
 * @param filter
 * @returns {Promise.<TResult>} Resolves with [acm, acm2, acm3, ...]
 */
function getCheckoutStatusForUser(filter) {
    var tableName = 'acm_check_out';
    var params = {
        TableName: tableName
    };
    
    return db.scan(params).promise()
        .then((data) => {
            // Only enable the next line for debugging; don't clutter production logs
            //console.log('data: ', JSON.stringify(data, null, 2))
            var result = [];
            data.Items.forEach(function (row, ix) {
                var acm_name = row.acm_name;
                // The regular expression will pluck the acm's name out of ACM-XXXXX-FB-123123
                var names = acmNameMatch.exec(acm_name);
                if (names && names.length > 1) {
                    acm_name = names[1];
                }
                if (filter.test(acm_name)) {
                    result.push(row);
                }
            });
            return result;
        });
    
}

exports.handler = (event, context, callback) => {
    console.log('Received event:', JSON.stringify(event, null, 2));
    console.log('Received context:', JSON.stringify(context, null, 2));
    
    var acmNameMatch = /ACM-([-\w]+?)(-FB-[-\w]*)?$/
    
    var email = event.email;
    getUserInfo(email)
        .then(getFilterForUser)
        .then(getCheckoutStatusForUser)
        .then((list) => {
            callback(null, list);
        }, (err) => {
            console.log('error: ', err)
            callback('error getting user info')
        })
    
};
