FROM bitnami/spark:3.5.0

# Switch to root user to install the necessary packages
USER root

# Installing curl because it wasn't installed in this image
RUN apt-get update && apt-get install -y curl

# Install py4j to enable Python interaction with Java
RUN pip install py4j

# Download JDBC connector for PostgreSQL and Kafka packages
RUN curl -o /opt/bitnami/spark/jars/postgresql-42.5.1.jar https://jdbc.postgresql.org/download/postgresql-42.5.1.jar
RUN curl -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar
RUN curl -o /opt/bitnami/spark/jars/kafka-clients-3.5.0.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar

# Revert to default non-root user after installation
USER 1001

# Copy the Spark Streaming application to the container
COPY ./spark/spark_streaming.py /opt/bitnami/spark/spark_streaming.py

# Set environment variables for PostgreSQL
ENV POSTGRES_URL=jdbc:postgresql://postgres:5432/airflow
ENV POSTGRES_USER=airflow
ENV POSTGRES_PASSWORD=airflow

# Run the Spark Streaming job with the necessary packages
CMD ["./bin/spark-submit", "--master", "local[*]", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.5.1", "/opt/bitnami/spark/spark_streaming.py"]