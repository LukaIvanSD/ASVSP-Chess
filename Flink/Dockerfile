ARG flink_version=1.20.3
FROM flink:${flink_version}

# install python3 and pip3
RUN apt-get update -y && \
apt-get install -y python3 python3-pip python3-dev && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

# install PyFlink
RUN pip3 install apache-flink==1.20.3

ARG mongo_version=2.0.0-1.20
RUN wget -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-mongodb/${mongo_version}/flink-connector-mongodb-${mongo_version}.jar


ARG mongo_driver_version=4.11.1
RUN wget -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/mongodb/mongodb-driver-sync/${mongo_driver_version}/mongodb-driver-sync-${mongo_driver_version}.jar && \
    wget -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/mongodb/mongodb-driver-core/${mongo_driver_version}/mongodb-driver-core-${mongo_driver_version}.jar && \
    wget -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/mongodb/bson/${mongo_driver_version}/bson-${mongo_driver_version}.jar



RUN mkdir -p /opt/flink/plugins/flink-hdfs
RUN wget -P /opt/flink/plugins/flink-hdfs \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-hadoop-fs/1.20.3/flink-hadoop-fs-1.20.3.jar


ARG kafka_connector_version=4.0.0-2.0
RUN wget -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${kafka_connector_version}/flink-sql-connector-kafka-${kafka_connector_version}.jar
