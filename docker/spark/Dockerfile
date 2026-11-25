# Imagem versionada Apache Spark 3.5.3 (Python 3) com conector Hadoop-AWS (S3A)
# Versões compatíveis com Hadoop 3.3.4 empacotado no Apache Spark 3.5.3
FROM apache/spark:3.5.3-python3

USER root

ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

# Baixa conectores S3A oficiais do Maven Central para o diretório de JARs do Spark
RUN curl -fsSL \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      -o "/opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    && curl -fsSL \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
      -o "/opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY docker/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

WORKDIR /opt/laguna

USER spark
