FROM python:3.10-slim-bullseye

WORKDIR /app

# Instalar Java 11 y dependencias
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk-headless \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Configurar environment de Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Copiar requirements e instalar dependencias Python
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copiar toda la aplicación
COPY . .

EXPOSE 5000

CMD ["python", "app.py"]