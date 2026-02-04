#!/bin/bash

/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic chess_finished_games --replication-factor 2 --partitions 2
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic chess_ongoing_games --replication-factor 2 --partitions 2
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic chess_ongoing_moves_transformed --replication-factor 2 --partitions 2
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic chess_ongoing_metadata_transformed --replication-factor 2 --partitions 2
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic chess_ongoing_moves_late --replication-factor 2 --partitions 2
