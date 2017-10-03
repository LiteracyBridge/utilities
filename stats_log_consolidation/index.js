'use strict';

console.log('Loading function');

const aws = require('aws-sdk');

const s3 = new aws.S3({apiVersion: '2006-03-01'});

const BUCKET = 'acm-stats';

exports.handler = (event, context, callback) => {
    //console.log('Received event:', JSON.stringify(event, null, 2));
    
    console.log(`Consolidating logs at ${new Date().toISOString()}`)
    
    getTbLoaderList()
        .then(processTbLoaderList)
        .then((result) => {
            callback(null, result);
        })
        .catch((err) => {
            console.error(`Error consolidating logs: ${err.toString()}`);
            callback(err);
        });
    
};

/**
 * Gets the list tbloaders with data in BUCKET/logs.
 * We're not interested in any actual objects here, only the "common prefixes".
 * @returns {Promise} resolves with the list of log/tbcd1234/ values.
 */
function getTbLoaderList() {
    var params = {
        Bucket: BUCKET,
        Delimiter: '/',
        EncodingType: 'URL',
        Prefix: 'log/'
    };
    
    return s3.listObjectsV2(params).promise().then((data) => {
        // Extract just the log/tbcd1234/ values
        var paths = data.CommonPrefixes.map(o => o.Prefix);
        paths = paths.filter((item, ix) => {
            return item.toLowerCase().startsWith('log/tbcd')
        });
        
        return paths;
    })
}

/**
 * Given a list of tbloaders, process them one at a time.
 * @param list
 * @returns {Promise}
 */
function processTbLoaderList(list) {
    return walkListWithPromises(list, processTbLoader);
}

/**
 * Given a TBLoader name (log/tbcd1234), consolidate any log fragments into a single daily log.
 * @param tbcd
 * @returns {Promise.<TResult>}
 */
function processTbLoader(tbcd) {
    var keysToRemove;
    console.log(`Consolidating logs for ${tbcd}`);
    return getLogFileList(tbcd)
        .then((logFileList) => {
            keysToRemove = logFileList;
            return partitionByDays(logFileList);
        })
        .then(consolidateDailyLogs)
        .then((result) => {
            return removeLogItems(keysToRemove);
        })
}

/**
 * Given a TBLoader name (log/tbcd1234), get the list of individual log files.
 * @param tbcd like 'log/tbcd1234/'
 * @returns {Promise.<TResult>} Resolves with log file names.
 */
function getLogFileList(tbcd) {
    var params = {
        Bucket: BUCKET,
        Delimiter: '/',
        EncodingType: 'URL',
        Prefix: tbcd
    };
    
    return s3.listObjectsV2(params).promise().then((data) => {
        // Extract just the log/tbcd1234/timestamp.log values
        var files = data.Contents.map(o => o.Key);
        console.log(`Found ${files.length} log files for ${tbcd}`);
        return files;
    });
}

/**
 * Given a list of log file names (log/tbcd1234/yyyymmddThhmmss.mmmZ.log...), group them by days.
 * @param listOfLogs of log file names
 * @returns {Promise.<TResult>} Resolves with list of objects like:
 *      {year:yyyy, month:mm, day:dd, logKeys:[key1, key2, ...], tb:'tbcd1234', key:'/log/yyyy/mm/tbcd1234-dd.log'}
 *      Note that these objects will accumulate more properties as they flow through the process.
 */
function partitionByDays(listOfLogs) {
    const nameRe = /^log\/(tbcd....)\/((\d{4})(\d{2})(\d{2}))T(\d{6}(\.\d{3,6}))?Z?.log/i;
    const TBCDIX = 1;
    const DATEIX = 2;
    const YEARIX = 3;
    const MONTHIX = 4;
    const DAYIX = 5;
    const TIMEIX = 6;
    const MINLENGTH = 7;
    var listOfDailyLogInfo = [];
    var dailyLogInfo;
    var currentDate = -1;
    listOfLogs.forEach((item, ix) => {
        var parse = nameRe.exec(item);
        if (parse && parse.length >= MINLENGTH) {
            // If this is a new day, close out the old day, start the new one.
            if (currentDate !== parse[DATEIX]) {
                currentDate = parse[DATEIX];
                dailyLogInfo && listOfDailyLogInfo.push(dailyLogInfo);
                dailyLogInfo = {
                    year: parse[YEARIX],
                    month: parse[MONTHIX],
                    day: parse[DAYIX],
                    logKeys: [],
                    tb: parse[TBCDIX],
                    key: `log/${parse[YEARIX]}/${parse[MONTHIX]}/${parse[TBCDIX]}-${parse[DAYIX]}.log`
                }
            }
            dailyLogInfo.logKeys.push(item)
        }
    });
    dailyLogInfo && listOfDailyLogInfo.push(dailyLogInfo);
    return listOfDailyLogInfo
}

function consolidateDailyLogs(listOfDailyLogInfo) {
    return walkListWithPromises(listOfDailyLogInfo, consolidateDailyLog);
}

/**
 * Consolidate individual log files into a single daily file.
 * - Reads the individual files
 * - Reads any existing, previous consolidation (in case already run for this day)
 * - Writes the consolidated log file
 * - Removes the individual files
 * @param dailyLogInfo - an item from the list created by partitionByDays
 * @returns {Promise.<TResult>}
 */
function consolidateDailyLog(dailyLogInfo) {
    return readLogsForDay(dailyLogInfo)
        .then(readPriorConsolidation)
        .then(writeConsolidatedLog)
}

/**
 * Given one of the items from partionByDays, reads all the log files for that day, and concatenates them together.
 * @param dailyLogInfo - an item from the list created by partitionByDays
 * @returns {Promise.<TResult>}
 */
function readLogsForDay(dailyLogInfo) {
    return walkListWithPromises(dailyLogInfo.logKeys, readFile)
        .then((logData) => {
            // Files should already have a trailing newline, so just join them together.
            var all = logData.join('');
            dailyLogInfo.log = all;
            return dailyLogInfo;
        })
}

/**
 * Given an item from partitionByDays, read any existing consolidated log file.
 * @param dailyLogInfo - an item from the list created by partitionByDays
 * @returns {*}
 */
function readPriorConsolidation(dailyLogInfo) {
    return readFile(dailyLogInfo.key)
        .then((data) => {
            console.log(`Found existing consolidated log for ${dailyLogInfo.tb} ${dailyLogInfo.year}-${dailyLogInfo.month}-${dailyLogInfo.day}: ${dailyLogInfo.key}`);
            dailyLogInfo.priorLog = data;
            return dailyLogInfo;
        })
        .catch((err) => {
            // err.code === 'NoSuchKey' means no prior log; that's fine.
            if (err.code === 'NoSuchKey') {
                dailyLogInfo.priorLog = '';
                return dailyLogInfo
            } else {
                throw err;
            }
        })
    
}

/**
 * Given any prior consolidated logs, plus new consolidated logs, write a new consolidated log.
 * @param dailyLogInfo - an item from the list created by partitionByDays
 * @returns {Promise.<TResult>}
 */
function writeConsolidatedLog(dailyLogInfo) {
    var body = (dailyLogInfo.priorLog || '') + dailyLogInfo.log;
    var params = {
        Bucket: BUCKET,
        Key: dailyLogInfo.key,
        Body: body,
        ContentType: 'text/plain'
    };
    console.log(`Writing consolidated log for ${dailyLogInfo.tb} ${dailyLogInfo.year}-${dailyLogInfo.month}-${dailyLogInfo.day}: ${dailyLogInfo.key}`);
    return s3.putObject(params).promise()
        .then((result) => {
            return dailyLogInfo
        })
}

/**
 * Remove the small log files, now that they've been consolidated into a single log file.
 * @param list - a list of log file keys.
 * @returns {Promise.<TResult>}
 */
function removeLogItems(list) {
    var params = {
        Bucket: BUCKET,
        Delete: {
            Objects: list.map((k) => {
                return {Key: k}
            })
        }
    };
    
    console.log(`Removing log files: ${list.join(', ')}`);
    return s3.deleteObjects(params).promise()
        .then(() => {
            // Callers don't really care about the delete results.
            return 0;
        })
}

/**
 * Given a list of items, and a function that takes an item and returns a promise,
 * call the function on each item in turn, gathering the individual results into a
 * list of results. Returns a promise that resolves with the list of results, or rejects
 * with any errors.
 * @param list items to be processed
 * @param fn processing function, takes an item, returns a promise
 * @returns {Promise}
 */
function walkListWithPromises(list, fn) {
    return new Promise((resolve, reject) => {
        var resultlist = [];
        
        function processOne(ix) {
            if (ix >= list.length) {
                resolve(resultlist)
            } else {
                fn(list[ix])
                    .then((data) => {
                        resultlist.push(data);
                        processOne(ix + 1)
                    })
                    .catch(reject);
            }
        }
        
        processOne(0);
    });
}

/**
 * Reads the text file at key.
 * @param key
 * @returns {Promise.<TResult>}
 */
function readFile(key) {
    var params = {
        Bucket: BUCKET,
        Key: key
    }
    return s3.getObject(params).promise()
        .then((data) => {
            try {
                // Depending on the stored ContentType, may need to perform a conversion. This does so.
                var logData = data.Body.toString('ascii');
                // If no terminating newline, add one.
                if (logData && logData.charAt(logData.length - 1) !== '\n') {
                    logData = logData + '\n';
                }
                return logData
            } catch (ex) {
                // This would be unexpected.
                console.error(`Exception reading/parsing log (${key}): ${ex.toString()}\n`);
                return `Exception reading log (${key}): ${ex.toString()}\n`;
            }
        })
}


