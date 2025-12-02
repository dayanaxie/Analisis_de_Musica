#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, desc, count, countDistinct, lit, row_number,
    sum as _sum, isnan, 
    collect_list, sort_array, sha2, 
    concat_ws, explode, first, avg,
    max as Fmax, when         
)
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

#----------------------------------------------
# Metricas de Popularidad y frecuencias básicas
#----------------------------------------------

# ---------- TOP 20 ARTISTAS ----------
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

#----------------------------------------------
# Métricas de Conteos Simples por Usuario
#----------------------------------------------

# ---------- Items por usuario ----------
def estadisticas_items_por_usuario(spark, path, item_col, count_col_name):
    df = spark.read.parquet(path)

    # Conteo por usuario
    counts = df.groupBy("user_id").agg(
        countDistinct(item_col).alias(count_col_name)
    )

    # Media y mediana usando summary (mean, 50%)
    stats = (
        counts.select(count_col_name)
        .summary("mean", "50%")
        .withColumnRenamed("summary", "metric")
        .withColumnRenamed(count_col_name, "value")
    )
    stats = stats.replace({"50%": "median"}, subset="metric")

    return counts, stats


def artistas_por_usuario_stats(spark):
    return estadisticas_items_por_usuario(
        spark,
        f"{HDFS_PARQUET_BASE}/user_top_artists",
        "artist_name",
        "artist_count",
    )


def canciones_por_usuario_stats(spark):
    return estadisticas_items_por_usuario(
        spark,
        f"{HDFS_PARQUET_BASE}/user_top_tracks",
        "track_name",
        "track_count",
    )


def albumes_por_usuario_stats(spark):
    return estadisticas_items_por_usuario(
        spark,
        f"{HDFS_PARQUET_BASE}/user_top_albums",
        "album_name",
        "album_count",
    )


# ---------- Artistas únicos----------
def conteo_items_unicos(spark):
    df_art = spark.read.parquet(f"{HDFS_PARQUET_BASE}/user_top_artists")
    df_track = spark.read.parquet(f"{HDFS_PARQUET_BASE}/user_top_tracks")
    df_album = spark.read.parquet(f"{HDFS_PARQUET_BASE}/user_top_albums")

    total_artistas = df_art.select("artist_name").distinct().count()
    total_canciones = df_track.select("track_name").distinct().count()
    total_albumes = df_album.select("album_name").distinct().count()

    return spark.createDataFrame(
        [(total_artistas, total_canciones, total_albumes)],
        ["unique_artists", "unique_tracks", "unique_albums"]
    )

# ---------- Usuarios con listas top-3 idénticas----------
def usuarios_top3_identicos_desde_top10(spark):
    # 1) Leemos el parquet de top tracks
    df = spark.read.parquet(f"{HDFS_PARQUET_BASE}/user_top_tracks")

    # 2) Rankear canciones por usuario según playcount (más escuchada primero)
    w_rank = Window.partitionBy("user_id").orderBy(desc("playcount"))
    ranked = (
        df.withColumn("rn", row_number().over(w_rank))
          .filter(col("rn") <= 10)  
    )

    # 3) Pasar el ranking a columnas top1..top10 por usuario (sin arrays)
    per_user = (
        ranked.groupBy("user_id").agg(
            Fmax(when(col("rn") == 1, col("track_name"))).alias("top1"),
            Fmax(when(col("rn") == 2, col("track_name"))).alias("top2"),
            Fmax(when(col("rn") == 3, col("track_name"))).alias("top3"),
            Fmax(when(col("rn") == 4, col("track_name"))).alias("top4"),
            Fmax(when(col("rn") == 5, col("track_name"))).alias("top5"),
            Fmax(when(col("rn") == 6, col("track_name"))).alias("top6"),
            Fmax(when(col("rn") == 7, col("track_name"))).alias("top7"),
            Fmax(when(col("rn") == 8, col("track_name"))).alias("top8"),
            Fmax(when(col("rn") == 9, col("track_name"))).alias("top9"),
            Fmax(when(col("rn") == 10, col("track_name"))).alias("top10"),
        )
    )

    # 4) Crear una firma string con el top-10 completo (en orden)
    per_user = per_user.withColumn(
        "top10_signature",
        concat_ws(
            " | ",
            col("top1"), col("top2"), col("top3"),
            col("top4"), col("top5"), col("top6"),
            col("top7"), col("top8"), col("top9"),
            col("top10")
        )
    )

    # 5) Agrupar por firma de top-10 y contar usuarios
    #    Nos quedamos con top1, top2, top3 y cuántos tienen ese mismo top-10.
    duplicates = (
        per_user.groupBy("top10_signature", "top1", "top2", "top3")
                .agg(count("user_id").alias("user_count"))
                .filter(col("user_count") > 1)     
                .orderBy(desc("user_count"))
                .limit(3)                         
    )

    # 6) Tabla final: solo las 3 canciones y la cantidad de usuarios
    result = duplicates.select("top1", "top2", "top3", "user_count")

    return result

# ---------- Usuarios con gustos muy concentrados----------
def usuarios_top5_mismo_artista(spark):
    df = spark.read.parquet(f"{HDFS_PARQUET_BASE}/user_top_tracks")

    # Rankear canciones por usuario
    w_rank = Window.partitionBy("user_id").orderBy(desc("playcount"))
    ranked = df.withColumn("rn", row_number().over(w_rank)).filter(col("rn") <= 5)

    # Agrupar por usuario para ver cuántos artistas distintos hay en su top-5
    grouped = (
        ranked.groupBy("user_id")
              .agg(
                  countDistinct("artist_name").alias("distinct_artists"),
                  count("*").alias("track_count"),
                  first("artist_name").alias("main_artist")
              )
    )

    # Usuarios cuyo top-5 está 100% concentrado en un solo artista
    concentrated = grouped.filter(
        (col("track_count") == 5) & (col("distinct_artists") == 1)
    ).select("user_id", "main_artist", "track_count")

    return concentrated

#----------------------------------------------
# Metricas de Comparaciones Simples
#----------------------------------------------

# ---------- Top Artistas entre Oyentes ----------
def top_artistas_entre_oyentes_activos(spark):
    # Leer tracks y artistas
    df_tracks = spark.read.parquet(f"{HDFS_PARQUET_BASE}/user_top_tracks")
    df_artists = spark.read.parquet(f"{HDFS_PARQUET_BASE}/user_top_artists")

    # Usuarios con más de 40 canciones 
    users_active = (
        df_tracks.groupBy("user_id")
                 .agg(count("*").alias("n_tracks"))
                 .filter(col("n_tracks") > 40)
                 .select("user_id")
    )

    filtered = df_artists.join(users_active, "user_id", "inner")

    # Contar artistas entre oyentes activos
    result = (
        filtered.groupBy("artist_name")
                .agg(countDistinct("user_id").alias("active_users"))
                .orderBy(desc("active_users"))
    )

    return result

# ---------- Popularidad cruzada entre listas ----------
def popularidad_cruzada_artistas(spark):
    df_artists = spark.read.parquet(f"{HDFS_PARQUET_BASE}/user_top_artists")
    df_tracks = spark.read.parquet(f"{HDFS_PARQUET_BASE}/user_top_tracks")

    # Conteo en lista de artistas
    art_counts = (
        df_artists.groupBy("artist_name")
                  .agg(count("*").alias("mentions_in_artists"))
    )

    # Conteo en lista de canciones
    track_counts = (
        df_tracks.groupBy("artist_name")
                 .agg(count("*").alias("mentions_in_tracks"))
    )

    # Join de popularidad cruzada
    joined = (
        art_counts.join(track_counts, "artist_name", "outer")
                  .fillna(0)
                  .withColumn(
                      "difference",
                      col("mentions_in_tracks") - col("mentions_in_artists")
                  )
                  .orderBy(desc("difference"))
    )

    return joined


# ---------- Artistas mas diversos ----------
def artistas_mas_diversos(spark):
    df_artists = spark.read.parquet(f"{HDFS_PARQUET_BASE}/user_top_artists")
    df_tracks = spark.read.parquet(f"{HDFS_PARQUET_BASE}/user_top_tracks")

    # 1. Usuarios distintos por artista
    user_div = (
        df_artists.groupBy("artist_name")
                  .agg(countDistinct("user_id").alias("distinct_users"))
    )

    # 2. Canciones distintas por artista
    track_div = (
        df_tracks.groupBy("artist_name")
                 .agg(countDistinct("track_name").alias("distinct_tracks"))
    )

    # Unión final
    result = (
        user_div.join(track_div, "artist_name", "inner")
                .orderBy(desc("distinct_users"), desc("distinct_tracks"))
    )

    return result


#--------------------
# Metricas de calidad
# -------------------


# ---------- DATOS FALTANTES ----------
def missing_data_report(spark):
    df = spark.read.parquet(f"{HDFS_PARQUET_BASE}/user_top_artists")
    # usuarios con algún campo NULO o cadena vacía
    null_users = (df.filter(
        col("artist_name").isNull() |
        (col("artist_name") == "") |
        col("user_id").isNull()
    ).select("user_id").distinct().count())

    # usuarios con < 20 filas
    user_counts = df.groupBy("user_id").count()
    short_users = user_counts.filter("count < 20").count()

    # DataFrame de dos filas
    return spark.createDataFrame(
        [("null_or_empty_field", null_users),
         ("less_than_20_items", short_users)],
        ["problem", "affected_users"]
    )


# ---------- USUARIOS ATÍPICOS (percentil 99) ----------
def extreme_users(spark):
    df = spark.read.parquet(f"{HDFS_PARQUET_BASE}/user_top_artists")
    user_counts = df.groupBy("user_id").agg(count("*").alias("items_count"))
    # percentil 99
    p99 = user_counts.stat.approxQuantile("items_count", [0.99], 0)[0]
    # extremos: muy altos o muy bajos (<= p1 o >= p99)
    p1 = user_counts.stat.approxQuantile("items_count", [0.01], 0)[0]
    outliers = user_counts.filter(
        (col("items_count") >= p99) | (col("items_count") <= p1)
    ).withColumn("percentile", when(col("items_count") >= p99, 99.0).otherwise(1.0))
    return outliers


# ---------- ARTISTAS CON < 5 MENCIONES ----------
def low_coverage_artists(spark):
    df = spark.read.parquet(f"{HDFS_PARQUET_BASE}/user_top_artists")
    mentions = df.groupBy("artist_name").agg(count("*").alias("mentions"))
    low = mentions.filter("mentions < 5").orderBy("mentions", "artist_name")
    return low



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


# Conteos simples 
def run_simple_counts_analysis(spark):

    print("Calculando ítems por usuario (artistas) + media y mediana...")
    user_artist_counts, user_artist_stats = artistas_por_usuario_stats(spark)
    user_artist_counts.show()
    user_artist_stats.show()
    save_to_mysql(user_artist_counts, "user_artist_counts")
    save_to_mysql(user_artist_stats, "user_artist_stats")

    print("Calculando ítems por usuario (canciones) + media y mediana...")
    user_track_counts, user_track_stats = canciones_por_usuario_stats(spark)
    user_track_counts.show()
    user_track_stats.show()
    save_to_mysql(user_track_counts, "user_track_counts")
    save_to_mysql(user_track_stats, "user_track_stats")

    print("Calculando ítems por usuario (álbumes) + media y mediana...")
    user_album_counts, user_album_stats = albumes_por_usuario_stats(spark)
    user_album_counts.show()
    user_album_stats.show()
    save_to_mysql(user_album_counts, "user_album_counts")
    save_to_mysql(user_album_stats, "user_album_stats")

    print("Conteo total de artistas, canciones y álbumes únicos...")
    unicos_df = conteo_items_unicos(spark)
    unicos_df.show()
    save_to_mysql(unicos_df, "unique_items")

    print("Calculando usuarios con listas top-3 idénticas (basado en top-10 duplicado)...")
    top3_same_from_top10 = usuarios_top3_identicos_desde_top10(spark)
    top3_same_from_top10.show(truncate=False)
    save_to_mysql(top3_same_from_top10, "users_same_top3_from_top10")


    print("Buscando usuarios con top-5 de tracks en un solo artista...")
    concentrated_users = usuarios_top5_mismo_artista(spark)
    concentrated_users.show()
    save_to_mysql(concentrated_users, "users_top5_single_artist")


# Comparaciones simples 
def run_simple_compare_analysis(spark):
    print("Calculando Top artistas entre oyentes activos (>40 canciones)...")
    res18 = top_artistas_entre_oyentes_activos(spark)
    res18.show()
    save_to_mysql(res18, "top_artists_active_users")

    print("Calculando Popularidad cruzada artistas (tracks vs artists)...")
    res19 = popularidad_cruzada_artistas(spark)
    res19.show()
    save_to_mysql(res19, "cross_popularity_artists")

    print("Calculando artistas más diversos (usuarios y canciones)...")
    res20 = artistas_mas_diversos(spark)
    res20.show()
    save_to_mysql(res20, "artists_diversity")



# ============================
# FUNCIÓN PARA CORRER EL ANÁLISIS
# ============================

def run_analysis(spark=None):
    """Ejecuta todo el análisis y guarda los resultados en MariaDB."""
    created_here = False
    if spark is None:
        spark = create_spark_session()
        created_here = True
    try: 
        # Metricas de Popularidad
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

        # Metricas de calidad
        print("Detectando datos faltantes / calidad...")
        qual_df = missing_data_report(spark)
        qual_df.show()
        save_to_mysql(qual_df, "data_quality")

        print("Identificando usuarios atípicos (percentil 99)...")
        out_df = extreme_users(spark)
        out_df.show()
        save_to_mysql(out_df, "outlier_users")

        print("Artistas con menos de 5 menciones...")
        low_df = low_coverage_artists(spark)
        low_df.show()
        save_to_mysql(low_df, "low_coverage_artists")

        # Metricas de conteos simples
        run_simple_counts_analysis(spark)
        # Metricas de comparaciones simples
        run_simple_compare_analysis(spark)

        spark.sparkContext._jsc.sc().listenerBus().waitUntilEmpty()
        
    except Exception as e:
        print(f"Error durante el análisis: {e}")
        raise
    finally:
        # Solo cerrar Spark si lo creamos aquí
        if created_here:
            spark.stop()


    print("Análisis completado y guardado en MariaDB.")

# ============================
# EJECUCIÓN COMO SCRIPT
# ============================

if __name__ == "__main__":
    run_analysis()
