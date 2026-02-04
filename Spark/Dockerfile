FROM bde2020/spark-master:3.0.1-hadoop3.2

USER root

# Instalacija SSH servera i Pythona (Alpine)
RUN apk update && \
    apk add --no-cache openssh python3 py3-pip && \
    ssh-keygen -A

# Postavljanje lozinke za root (lozinka: root)
RUN echo 'root:root' | chpasswd

# Dozvoli root login i password auth
RUN sed -i 's/#PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PasswordAuthentication.*/PasswordAuthentication yes/' /etc/ssh/sshd_config

# 🔥 KLJUČNO – master.sh mora biti executable
RUN chmod +x /master.sh

RUN mkdir -p /opt/spark/jars/

# Mongo Spark Connector i Java Driver
RUN curl -fL https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.2.0/mongo-spark-connector_2.12-10.2.0.jar \
    -o /opt/spark/jars/mongo-spark-connector_2.12-10.2.0.jar && \
    curl -fL https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.8.2/mongodb-driver-sync-4.8.2.jar \
    -o /opt/spark/jars/mongodb-driver-sync-4.8.2.jar && \
    curl -fL https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.8.2/mongodb-driver-core-4.8.2.jar \
    -o /opt/spark/jars/mongodb-driver-core-4.8.2.jar && \
    curl -fL https://repo1.maven.org/maven2/org/mongodb/bson/4.8.2/bson-4.8.2.jar \
    -o /opt/spark/jars/bson-4.8.2.jar

# Eksponirani portovi
EXPOSE 22 7077 8080

# Pokreni SSH + Spark master
CMD ["/bin/sh", "-c", "/usr/sbin/sshd && exec /master.sh"]
