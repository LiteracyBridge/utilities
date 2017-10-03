console.log('Loading function');
var doc = require('dynamodb-doc');
var db = new doc.DynamoDB();
var defaultFilter = /^(LBG-DEMO|DEMO)$/
var simpleList = /^([-\w]+\|)*?([-\w]+)$/

/**
 * Given a user name, look up that user in the acm_users table. If there is
 * an entry with 'projects_view', use that for the filter.
 */
function getFilterForUser(username, email, callback) {
    function takeDefault() {
        console.log('take default')
        email = email || '';
        var parts = email.toLowerCase().split('@');
        if ((parts.length == 2)) {
            if (parts[1] === 'literacybridge.org') {
                callback(RegExp('CARE|MEDA|TUDRIDEP|UWR|DEMO|LBG-DEMO'))
            } else if (parts[1] === 'centreforbcc.com') {
                callback(RegExp('CBCC|CBCC-TEST'))
            } else {
                callback(defaultFilter)
            }
            return
        }
        callback(defaultFilter);
    }
    
    
    params = {
        TableName: 'acm_users',
        Key: {username: username}
    };
    if (username) {
        console.log('making db call to look up user ', username)
        db.getItem(params, function(err, data){
            
            console.log('got db result')
            if (err || !data.Item || !data.Item.projects_view) {
                if (err) console.log('get err: ', err)
                takeDefault();
                return;
            }
            // If just a list, surround with ^( ... )$
            var filter = data.Item.projects_view;
            if (simpleList.test(filter)) {
                filter = '^(' + filter + ')$';
            }
            callback(RegExp(filter));
        });
    } else {
        takeDefault();
    }
}

exports.handler = (event, context, callback) => {
    console.log('Received event:', JSON.stringify(event, null, 2));
    console.log('Received context:', JSON.stringify(context, null, 2));
    
    var acmNameMatch = /ACM-([-\w]+?)(-FB-[-\w]*)?$/
    
    getFilterForUser(event.username, event.email, function(filter){
        
        var tableName = 'acm_check_out';
        var params = {
            TableName: tableName
        };
        //console.log('Got filter for user: ', filter)
        
        db.scan(params, function(err, data){
            if (err) {
                console.log('err: ', JSON.stringify(err, null, 2));
            } else {
                // Only enable the next line for debugging; don't clutter production logs
                //console.log('data: ', JSON.stringify(data, null, 2))
                var result = [];
                data.Items.forEach(function(row,ix){
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
                callback(null, result);
            }
        });
    });
    
};
