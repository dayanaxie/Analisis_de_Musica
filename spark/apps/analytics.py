#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, desc, count, countDistinct, lit, row_number, sum as _sum, percent_rank
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


# ---------- TOP 20 ARTISTAS (CORREGIDO) ----------
def top_20_artistas(spark):
    df = spark.read.parquet(f"{HDFS_PARQUET_BASE}/user_top_artists")
    total_users = df.select("user_id").distinct().count() or 1
    top = (df.groupBy("artist_name")
             .agg(countDistinct("user_id").alias("users_count"))
             .withColumn("percentage", lit(100.0) * col("users_count") / total_users)
             .orderBy(desc("users_count"))
             .limit(20))
    return top


# ---------- TOP 20 CANCIONES ----------
def top_20_canciones(spark):
    df = spark.read.parquet(f"{HDFS_PARQUET_BASE}/user_top_tracks")
    total_users = df.select("user_id").distinct().count() or 1
    top = (df.groupBy("track_name")
             .agg(countDistinct("user_id").alias("users_count"))
             .withColumn("percentage", lit(100.0) * col("users_count") / total_users)
             .orderBy(desc("users_count"))
             .limit(20))
    return top


# ---------- TOP 20 ÁLBUMES ----------
def top_20_albumes(spark):
    df = spark.read.parquet(f"{HDFS_PARQUET_BASE}/user_top_albums")
    total_users = df.select("user_id").distinct().count() or 1
    top = (df.groupBy("album_name")
             .agg(countDistinct("user_id").alias("users_count"))
             .withColumn("percentage", lit(100.0) * col("users_count") / total_users)
             .orderBy(desc("users_count"))
             .limit(20))
    return top

# ---------- USUARIOS CON EL MISMO ARTISTA #1 ----------
def same_artista_numero1(spark):
    df = spark.read.parquet(f"{HDFS_PARQUET_BASE}/user_top_artists")
    # ranking por usuario
    w = Window.partitionBy("user_id").orderBy(desc("playcount"))
    rank_df = (df.withColumn("rn", row_number().over(w))
                 .filter("rn = 1")
                 .select("user_id", "artist_name"))
    # contar cuántos tienen el mismo artista principal
    top1 = (rank_df.groupBy("artist_name")
                   .agg(countDistinct("user_id").alias("users_count"))
                   .orderBy(desc("users_count"))
                   .limit(1))
    total_users = rank_df.select("user_id").distinct().count() or 1
    top1 = top1.withColumn("percentage", lit(100.0) * col("users_count") / total_users)
    return top1


# ---------- DISTRIBUCIÓN DE MENCIONES POR ARTISTA ----------
def artist_mention_histogram(spark):
    df = spark.read.parquet(f"{HDFS_PARQUET_BASE}/user_top_artists")
    mentions = df.groupBy("artist_name").agg(count("*").alias("mentions"))
    desc = mentions.select("mentions").summary("mean", "50%", "stddev") \
                   .withColumnRenamed("summary", "metric") \
                   .withColumnRenamed("mentions", "value")
    # cambio nombres para que la tabla quede limpia
    desc = desc.replace({"50%": "median"}, subset="metric")
    return desc


# ---------- LONG TAIL 80 % ----------
def long_tail_80(spark):
    df = spark.read.parquet(f"{HDFS_PARQUET_BASE}/user_top_artists")
    mentions = df.groupBy("artist_name").agg(count("*").alias("mentions"))
    total_mentions = mentions.agg(_sum("mentions")).collect()[0][0] or 1
    # ordenar de más a menos mencionado
    ordered = mentions.orderBy(desc("mentions"))
    # columna acumulada
    ordered = ordered.withColumn("cum", _sum("mentions").over(Window.orderBy(desc("mentions"))))
    # filtrar hasta el 80 % del total
    tail = ordered.filter(col("cum") <= 0.8 * total_mentions)
    total_artists = mentions.count()
    tail_artists = tail.count()
    tail_pct = 100.0 * tail_artists / total_artists if total_artists else 0.0
    # devolvemos un solo-row DataFrame
    return spark.createDataFrame(
        [(total_artists, tail_artists, round(tail_pct, 2))],
        ["total_artists", "artists_in_tail", "tail_percentage"]
    )


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

    print("Calculando usuarios que comparten el mismo artista #1...")
    same_top1 = same_artista_numero1(spark)
    same_top1.show()
    save_to_mysql(same_top1, "same_top1_artist")

    print("Calculando estadísticos de menciones por artista...")
    hist_stats = artist_mention_histogram(spark)
    hist_stats.show()
    save_to_mysql(hist_stats, "artist_mention_stats")

    print("Calculando long-tail 80 %...")
    tail_df = long_tail_80(spark)
    tail_df.show()
    save_to_mysql(tail_df, "long_tail_80")

    if created_here:
        spark.stop()

    print("Análisis completado y guardado en MariaDB.")

# ============================
# EJECUCIÓN COMO SCRIPT
# ============================

if __name__ == "__main__":
    run_analysis()
