FROM bitnami/spark:3.3.0

USER root

# Install Java and pip, and then install required Python packages
RUN apt-get update && \
    apt-get install -y python3-pip openjdk-11-jdk && \
    pip3 install delta-spark==2.1.0 kafka-python

# Set the Python interpreter for PySpark
ENV PYSPARK_PYTHON=python3


# ✅ Copy MySQL connector JAR into the container
# COPY ./spark/jars/mysql-connector-j-9.3.0.jar /app/jars/mysql-connector-j-9.3.0.jar
