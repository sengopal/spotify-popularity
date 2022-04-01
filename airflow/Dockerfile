# First-time build can take upto 10 mins.

FROM apache/airflow:2.2.3

ENV AIRFLOW_HOME=/opt/airflow

COPY nscacert_combined.pem /etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV REQUESTS_CA_BUNDLE='/etc/ssl/certs/ca-certificates.crt'
ENV PYTHONHTTPSVERIFY=0

ARG SPARK_VERSION="3.1.2"
ARG HADOOP_VERSION="3.2"

USER root
RUN apt-get update -qq && apt-get install vim -qqq
RUN apt-get install wget -qqq
# git gcc g++ -qqq

RUN pip install --upgrade pip
RUN pip install pyspark --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org

COPY requirements.txt .
# --no-cache-dir
RUN pip install -r requirements.txt --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org

# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG CLOUD_SDK_VERSION=322.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version


###############################
## Begin JAVA installation
###############################
# Java is required in order to spark-submit work
# Install OpenJDK-8
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    apt-get install -y gnupg2 && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EB9B1D8886F44E2A && \
    add-apt-repository "deb http://security.debian.org/debian-security stretch/updates main" && \
    apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    pip freeze && \
    java -version $$ \
    javac -version

# Setup JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
RUN export JAVA_HOME
###############################
## Finish JAVA installation
###############################

###############################
## SPARK files and variables
###############################
ENV SPARK_HOME /usr/local/spark

# Spark submit binaries and jars (Spark binaries must be the same version of spark cluster)
RUN cd "/tmp" && \
        wget --no-verbose --no-check-certificate "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
        tar -xvzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
        mkdir -p "${SPARK_HOME}/bin" && \
        mkdir -p "${SPARK_HOME}/sbin" && \
        mkdir -p "${SPARK_HOME}/assembly/target/scala-2.12/jars" && \
        mkdir -p "${SPARK_HOME}/python" && \
        cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/bin/." "${SPARK_HOME}/bin/" && \
        cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/sbin/." "${SPARK_HOME}/sbin/" && \
        cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/jars/." "${SPARK_HOME}/assembly/target/scala-2.12/jars/" && \
        cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/python/." "${SPARK_HOME}/python/" && \
        rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# Create SPARK_HOME env var
RUN export SPARK_HOME

ENV PYSPARK_PYTHON python

ENV PYSPARK_SUBMIT_ARGS --master local[2] pyspark-shell
RUN export PYSPARK_SUBMIT_ARGS


ENV PATH $PATH:$JAVA_HOME/bin
ENV PATH $PATH:$SPARK_HOME/bin;$SPARK_HOME/sbin;$SPARK_HOME/python
###############################
## Finish SPARK files and variables
###############################

ENV PYTHONPATH ${SPARK_HOME}/python/:${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH

RUN cd "/opt/airflow/dags" && \
        wget --no-verbose --no-check-certificate "https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar"

WORKDIR $AIRFLOW_HOME

USER $AIRFLOW_UID
