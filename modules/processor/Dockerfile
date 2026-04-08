FROM bitnamilegacy/spark:3.5.6

# Set HOME explicitly to avoid Ivy errors
ENV HOME=/root \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SPARK_HOME=/opt/bitnami/spark \
    PYTHONPATH=/opt/bitnami/spark/python:/opt/bitnami/spark/python/lib/py4j-*.zip

WORKDIR /app

# Switch to root to install packages (Bitnami images run as non-root by default)
USER root

# 1. Copy dependency files
COPY pyproject.toml README.md ./

# 2. Copy SOURCE CODE
COPY src/ ./src/

# 3. Install dependencies (now as root, no permission issues)
RUN pip install --no-cache-dir -e .

# # 4. Download Spark Packages (Iceberg & Kafka)
# RUN curl -L -o /opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar \
#     https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar && \
#     curl -L -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
#     https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
#     curl -L -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-2.17.178.jar \
#     https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.17.178/bundle-2.17.178.jar && \
#     curl -L -o /opt/bitnami/spark/jars/url-connection-client-2.17.178.jar \
#     https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.17.178/url-connection-client-2.17.178.jar

# 5. Create checkpoint directory and ensure HOME exists
RUN mkdir -p /tmp/spark-checkpoints/retailsense && mkdir -p $HOME/.ivy2

# Optional: Switch back to non-root user for security (if desired)
# USER 1001

ENTRYPOINT ["python", "-m", "retailsense_processor.main"]