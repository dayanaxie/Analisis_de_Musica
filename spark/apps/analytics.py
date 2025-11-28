#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, count
import os

# ============================
# CONFIGURACIÓN
# ============================

# Variables de entorno (puedes ajustarlas o pasarlas por docker-compose)
HDFS_URL = os.getenv("HDFS_URL", "hdfs://namenode:9000")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
SPARK_DRIVER_HOST = os.getenv("SPARK_DRIVER_HOST", "analytics")

# Rutas HDFS
HDFS_PARQUET_BASE = f"{HDFS_URL}/processed_data/parquet"

# MariaDB
DB_URL = os.getenv("DB_URL", "jdbc:mysql://mariadb:3306/music_analysis")
DB_USER = os.getenv("DB_USER", "sparkuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "sparkpass") 
DB_DRIVER = "com.mysql.cj.jdbc.Driver"  

# ============================
# SPARK SESSION
# ============================

def create_spark_session(app_name="MusicAnalytics"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER_URL)
        .config("spark.driver.host", SPARK_DRIVER_HOST)
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.jars", "/opt/spark/jars/mysql-connector-j-8.0.33.jar")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

# ============================
# FUNCIONES DE ANÁLISIS
# ============================

# ---------- TOP 20 ARTISTAS ----------
def top_20_artistas(spark):
    df = spark.read.parquet(f"{HDFS_PARQUET_BASE}/user_top_artists")
    top_artists = (
        df.groupBy("artist_name")
        .agg(count("*").alias("mentions"))
        .orderBy(desc("mentions"))
        .limit(20)
    )
    return top_artists

# ---------- TOP 20 CANCIONES ----------
def top_20_canciones(spark):
    df = spark.read.parquet(f"{HDFS_PARQUET_BASE}/user_top_tracks")
    top_tracks = (
        df.groupBy("track_name")
        .agg(count("*").alias("mentions"))
        .orderBy(desc("mentions"))
        .limit(20)
    )
    return top_tracks


# ---------- TOP 20 ÁLBUMES ----------
def top_20_albumes(spark):
    df = spark.read.parquet(f"{HDFS_PARQUET_BASE}/user_top_albums")
    top_albums = (
        df.groupBy("album_name")
        .agg(count("*").alias("mentions"))
        .orderBy(desc("mentions"))
        .limit(20)
    )
    return top_albums

# ============================
# GUARDAR EN MARIADB
# ============================

def save_to_mysql(df, table_name):
    df.write.format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", table_name) \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", DB_DRIVER) \
        .mode("overwrite") \
        .save()

# ============================
# FUNCIÓN PARA CORRER EL ANÁLISIS
# ============================

def run_analysis(spark=None):
    """Ejecuta todo el análisis y guarda los resultados en MariaDB."""
    created_here = False
    if spark is None:
        spark = create_spark_session()
        created_here = True

    print("Calculando Top 20 Artistas...")
    top_artists = top_20_artistas(spark)
    top_artists.show()
    save_to_mysql(top_artists, "top_20_artists")

    print("Calculando Top 20 Canciones...")
    top_tracks = top_20_canciones(spark)
    top_tracks.show()
    save_to_mysql(top_tracks, "top_20_tracks")

    print("Calculando Top 20 Álbumes...")
    top_albums = top_20_albumes(spark)
    top_albums.show()
    save_to_mysql(top_albums, "top_20_albums")

    if created_here:
        spark.stop()

    print("Análisis completado y guardado en MariaDB.")

# ============================
# EJECUCIÓN COMO SCRIPT
# ============================

if __name__ == "__main__":
    run_analysis()
