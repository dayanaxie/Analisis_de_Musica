FROM python:3.7-slim-bullseye

# 1. Java 11
RUN apt-get update && apt-get install -y \
    openjdk-11-jre-headless \
    wget \
    && rm -rf /var/lib/apt/lists/*

# 2. PySpark
RUN pip install --no-cache-dir pyspark==3.0.0 \
    flask

# 3. Descargar el driver JDBC de MySQL
RUN mkdir -p /opt/spark/jars && \
    wget -O /opt/spark/jars/mysql-connector-j-8.0.33.jar \
    https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar

# 4. Variables de entorno
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV SPARK_CLASSPATH=/opt/spark/jars/mysql-connector-j-8.0.33.jar

WORKDIR /app
COPY . .
CMD ["python", "app.py"]