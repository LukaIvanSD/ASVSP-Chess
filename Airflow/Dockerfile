FROM apache/airflow:3.1.5-python3.10

COPY requirements.txt .

USER root

RUN mkdir /shared && chown -R airflow /shared

RUN apt-get update

RUN echo "deb http://deb.debian.org/debian unstable main contrib non-free" >> /etc/apt/sources.list

RUN echo "Package: * \
Pin: release a=stable \
Pin-Priority: 900 \
\
Package: * \
Pin: release a=unstable\
Pin-Priority: 50" > /etc/apt/preferences.d/99pin-unstable

RUN apt-get update && apt-get install -y \
    gcc \
    libkrb5-dev \
    openjdk-11-jdk \
    curl \
 && apt-get clean

# Install Spark
ENV SPARK_VERSION=3.0.1
ENV HADOOP_VERSION=3.2
ENV SPARK_HOME=/opt/spark

RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt \
 && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

ENV SPARK_EXTRA_JARS_DIR=/opt/spark/jars/

RUN mkdir -p "${SPARK_EXTRA_JARS_DIR}"

# ---- Install curl (if not present) ----
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*


# Environment
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$SPARK_HOME/bin

USER airflow

RUN pip install -r requirements.txt

RUN pip install kafka-python==2.0.2

USER airflow

WORKDIR /opt/airflow