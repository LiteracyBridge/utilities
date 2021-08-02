User Feedback Processing

The TB-Loaders collect statistics and user feedback from Talking Books.

The files from a single Talking Book are zipped together into a file with a name that is an ISO format date-time, like 20210730T131711Z001q.

The timestamp.zip is uploaded to s3://acm-stats/collected-data/{tbcdid}/{timestamp}.zip

Batch processing creates a directory for the day as {dropbox}/collected-data-processed/{year}/{month}/{day}. Call this {dailyDir}.
Batch processing downloads the {tbcdid}/{timestamp}.zip files to a temporary directory.

Files in the temporary directory are unzipped. The contents of embedded directories "userrecordings" are moved to {dailyDir}/userrecordings. These files have a directory structure like .../{programid}/{deploymentname}/{tbcdid}/{communityname}. The user recordings are .a18 files, and each has a .properties "sidecar" file.

(The s3://acm-stats/collected-data/{timestamp}.zip files are moved to s3://acm-stats/archived-data/{curYear}/{curMonth}/{curDay}/{timestamp}.zip.)

Batch processing runs the Python "uf_extractor" to extract and process uf from the .a18 and .properties files. The results go into a temporary directory.

The uf_extractor iterates the .a18 files in the {dailyDir}/userrecordings directory. These .a18 files are converted into a .mp3 file in the temporary directory, as {programid}/{deployment_num}/{message_uuid}.mp3 files, with parallel .properties sidecar files. For each .mp3 file, a row is added to the uf_messages table in PostgreSQL.

The contents of the temporary directory are moved to s3://amplio-uf/collected. These are .mp3 files of user feedbac, available for listening and analysis.



