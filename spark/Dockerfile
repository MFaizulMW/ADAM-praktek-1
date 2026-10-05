FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

# Connector JAR dipanggang ke image (tidak perlu --packages saat runtime)
#   - Kafka source untuk Structured Streaming
#   - S3A (hadoop-aws) untuk MinIO
#   - elasticsearch-spark untuk menulis layer gold ke Elasticsearch
ARG MAVEN=https://repo1.maven.org/maven2
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/elasticsearch/elasticsearch-spark-30_2.12/8.15.3/elasticsearch-spark-30_2.12-8.15.3.jar /opt/spark/jars/

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
RUN mkdir -p /opt/spark/work /tmp/spark-events && chown -R spark:spark /opt/spark/work /tmp/spark-events

USER spark
