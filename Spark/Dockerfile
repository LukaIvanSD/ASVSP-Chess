FROM bde2020/spark-master:3.0.1-hadoop3.2

USER root

RUN apk add --no-cache curl

RUN mkdir -p /opt/spark/jars && \
    curl -fL \
      https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.2.0/mongo-spark-connector_2.12-10.2.0.jar \
      -o /opt/spark/jars/mongo-spark-connector_2.12-10.2.0.jar

EXPOSE 22
