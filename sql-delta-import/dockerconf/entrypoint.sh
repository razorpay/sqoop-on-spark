#!/usr/bin/dumb-init /bin/sh
set -eux

cd /app

upload_jar()
{
    local_jar_path="sql-delta-import/target/scala-2.12/sql-delta-import-assembly-0.2.1-SNAPSHOT.jar"
    jar_s3_url="s3://${SQOOP_BUCKET}/application-jars"
    full_jar_s3_url="${jar_s3_url}/sqoop-1.0-SNAPSHOT-jar-with-dependencies.jar"

    aws s3 cp --acl "bucket-owner-full-control" ${local_jar_path} ${full_jar_s3_url}
}

# Do the basic initialization and get the app type
if [[ "${APP_MODE}" != "dev" ]]
then
    upload_jar
fi