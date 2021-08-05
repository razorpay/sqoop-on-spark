#!/usr/bin/dumb-init /bin/bash
set -eux

cd /app

upload_jar()
{
    local_jar_path="sql-delta-import/target/scala-2.12/sql-delta-import-assembly-0.2.1-SNAPSHOT.jar"
    jar_s3_url="s3://${SQOOP_BUCKET}/application-jars"
    full_jar_s3_url="${jar_s3_url}/sqoop-1.0-SNAPSHOT-jar-with-dependencies.jar"

    config_s3_url="s3://${CONFIG_BUCKET}/sqoop-config/"
    config_file_path="sql-delta-import/src/main/resources/application.conf.vault.j2"
    alohomora_cast_file_path="sql-delta-import/src/main/resources/application.conf.vault"

    aws s3 cp --acl "bucket-owner-full-control" ${local_jar_path} ${full_jar_s3_url}

    ALOHOMORA_BIN=$(which alohomora)
    $ALOHOMORA_BIN cast --region ap-south-1 --env ${APP_MODE} --app datahub ${config_file_path}

    credstash_table="credstash-${APP_MODE}-datahub"

    # x has to be disabled so that ssec_key doesn't get logged
    set +x

    # Key used for server side encryption while uploading to S3
    ssec_key=$(credstash -t ${credstash_table} -r ap-south-1 get s3_c_key)

    aws s3 cp --acl "bucket-owner-full-control" --sse-c --sse-c-key ${ssec_key} ${alohomora_cast_file_path} ${config_s3_url}

    set -x
}

# Do the basic initialization and get the app type
if [[ "${APP_MODE}" != "dev" ]]
then
    upload_jar
fi
