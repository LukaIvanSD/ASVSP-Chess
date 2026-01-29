#!/bin/bash

# Set up connections
echo ">>> Setting up Airflow connections"

airflow connections add 'AIRFLOW_DB_CONNECTION' \
    --conn-json "{
        \"conn_type\": \"postgres\",
        \"login\": \"${POSTGRES_USER}\",
        \"password\": \"${POSTGRES_PASSWORD}\",
        \"host\": \"${POSTGRES_HOST}\",
        \"port\": ${POSTGRES_PORT},
        \"schema\": \"${POSTGRES_DB}\"
    }"

airflow connections add 'HDFS_CONNECTION' \
    --conn-json '{
        "conn_type": "hdfs",
        "host": "namenode",
        "port": 9870,
        "extra": {
            "use_ssl": false
        }
    }'

airflow connections add 'SPARK_CONNECTION' \
  --conn-json '{
    "conn_type": "spark",
    "host": "spark://spark-master",
    "port": 7077
  }'

#airflow connections add 'MONGO_CONNECTION' \
#  --conn-json "{
#    \"conn_type\": \"mongo\",
#    \"host\": \"${MONGO_HOST}\",
#    \"port\": ${MONGO_PORT},
#    \"login\": \"${MONGO_USER}\",
#    \"password\": \"${MONGO_PASSWORD}\"
#  }"

airflow connections add 'LOCAL_FS_FILES' \
    --conn-json '{
        "conn_type": "fs",
        "extra": "{ \"path\": \"/opt/airflow/files\"}"
    }'

airflow connections add 'flink_http_default' \
    --conn-json '{
        "conn_type": "http",
        "host": "flink-jobmanager-1",
        "port": 8081
    }'

# Set up variables
echo ">> Setting up airflow variables"
airflow variables set HDFS_DEFAULT_FS "hdfs://namenode:9000"
airflow variables set MONGO_URI "mongodb://${MONGO_USER}:${MONGO_PASSWORD}@${MONGO_HOST}:${MONGO_PORT}"
