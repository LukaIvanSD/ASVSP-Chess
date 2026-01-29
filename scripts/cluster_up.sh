#!/bin/bash

echo "> Starting up cluster"

read -p "> Restore Metabase database? [y/N] " -n 1 -r
echo

echo "> Creating docker network 'asvsp-chess-project'"
docker network create asvsp-chess-project

echo ">> Starting up HDFS"
docker compose -f Hadoop/docker-compose.yml up -d

echo ">> Starting up Apache Spark"
docker compose -f Spark/docker-compose.yml up -d

echo ">> Starting up MongoDB"
docker compose -f MongoDB/docker-compose.yml up -d

echo ">> Starting up Metabase"
docker compose -f Metabase/docker-compose.yml up -d

echo ">> Starting up Airflow"
docker compose -f Airflow/docker-compose.yml up -d

echo ">> Starting up Kafka"
docker compose -f Kafka/docker-compose.yml up -d

echo ">> Starting up Flink"
docker compose -f Flink/docker-compose.yml up -d --scale taskmanager=2


sleep 25

echo "> Setting up services"

echo ">> Setting up Airflow objects"
cmd='bash -c "/opt/airflow/config/setupObjects.sh"'
docker exec -it airflow-airflow-apiserver-1 $cmd

echo ">> Setting up Kafka objects"
cmd='bash -c "/home/config/setupObjects.sh"'
docker exec -it kafka-broker1-1 $cmd

if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo ">> Restoring Metabase database"
    cmd='bash -c "/config/restoreDB.sh"'
    docker exec -it mb-postgres $cmd
fi