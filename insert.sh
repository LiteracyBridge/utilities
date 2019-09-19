#!/usr/bin/env bash
traditionalIFS="$IFS"
#IFS="`printf '\n\t'`"
set -u

# Script to create and initially populate the recipients table.
# Extracts from UNICEF-2-recipients.csv (from UNICEF2 project spec), and scans ACM directories for "communities"
# Creates and populates recipients.

if [ -z "${psql:-}" ]; then
    if [ -e /Applications/Postgres.app/Contents/Versions/9.5/bin/psql ]; then
        psql=/Applications/Postgres.app/Contents/Versions/9.5/bin/psql
    elif [ -e /Applications/Postgres.app/Contents/Versions/9.4/bin/psql ]; then
        psql=/Applications/Postgres.app/Contents/Versions/9.4/bin/psql
    elif [ ! -z $(which psql) ]; then
        psql=$(which psql)
    else
        echo "Can't find psql!"
        exit 100
    fi
fi
if [ -z "${dbcxn:-}" ]; then
    dbcxn=" --host=lb-device-usage.ccekjtcevhb7.us-west-2.rds.amazonaws.com --port 5432 --username=lb_data_uploader --dbname=dashboard "
fi

function configure() {
    recipientsfile="recipients.csv"
    recipmapfile="recipients_map.csv"
    project="$(awk -F , 'NR==2{print $2}' recipients.csv)"

    echo $(date)>log.txt
    verbose=true
    execute=true

    echo "psql: ${psql}"
    echo "dbcxn: ${dbcxn}"
}

function main() {
    configure

    set -x
    
    importTable
}



function importTable() {
    # Import into db, and update recipients
    ${psql} ${dbcxn} -a <<EndOfQuery >>log.txt
    \\timing
    \\set ECHO all
    create temporary table temp_recip as select * from recipients where false;
    \copy temp_recip from '${recipientsfile}' with delimiter ',' csv header;
    insert into recipients select * from temp_recip
        on conflict do nothing
        ;
    update recipients set communityname = t.communityname,
                          groupname = t.groupname,
                          affiliate = t.affiliate,
                          component = t.component,
                          country = t.country,
                          region = t.region,
                          district = t.district,
                          numhouseholds = t.numhouseholds,
                          numtbs = t.numtbs,
                          supportentity = t.supportentity,
                          model = t.model,
                          language = t.language,
                          coordinates = t.coordinates,
                          agent = t.agent,
                          latitude = t.latitude,
                          longitude = t.longitude
            from temp_recip t
            where recipients.recipientid = t.recipientid;

    select * from recipients where project='${project}';

    create temporary table temp_recip_map as select * from recipients_map where false;
    \copy temp_recip_map from '${recipmapfile}' with delimiter ',' csv header;
    insert into recipients_map select * from temp_recip_map  
        on conflict do nothing
        ;

    select * from recipients_map where project='${project}';

EndOfQuery
}

main "$@"

# ta-da

