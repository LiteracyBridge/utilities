exports.handler = function(event, context) {
    var emailPrefix = {
        'literacybridge.org' : '',      // No prefix for literacybridge.org
        'centreforbcc.com'   : 'a-',    // a- prefix for centreforbcc.com
        'gmail.com'          : 'xyz-'   // So I can test with a non-lb email
    }
    
    var err = function(msg) {
        context.done(msg, event);
        console.log(msg);
        //throw new Error('Failed: '+msg);
    }
    
    // Verify that there's an email address (should be -- required by user pool)
    var email = event.request.userAttributes.email;
    if (!email) {
        err('No email address provided');
    }
    // Split email into email user and email domain, and make sure we have exactly that.
    // Make sure the email domain is whitelisted.
    var parts = email.toLowerCase().split('@');
    if ((parts.length != 2) || (emailPrefix[parts[1]] === undefined)) {
        err('Not a valid email address: ' + email);
    }
    // Verify that the user name + prefix is the email user.
    var userid = event.userName;
    var prefix = emailPrefix[parts[1]];
    if ((prefix+parts[0]) !== userid.toLowerCase()) {
        err('Not a valid userid: ' + userid);
    }
    
    // Return result to Cognito
    context.done(null, event);
}