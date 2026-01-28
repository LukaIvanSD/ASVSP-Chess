#!/bin/bash

/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic chess_data --replication-factor 2 --partitions 2
