FROM python:3.7-slim-bullseye

# 1. Instalar Java 11 (disponible en Bullseye)
RUN apt-get update && apt-get install -y \
    openjdk-11-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

# 2. Instalar paquetes Python
RUN pip install --no-cache-dir \
    pyspark==3.0.0 \
    flask

# 3. Variables de entorno
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# 4. Copiar la aplicación
WORKDIR /app
COPY . .

EXPOSE 5001
CMD ["python", "app.py"]