#!/usr/bin/env python3

import os
import time
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException


LOCAL_RAW_PATH = "/app/data/raw"

# Variables configurables por Docker Compose
HDFS_URL = os.getenv("HDFS_URL", "hdfs://namenode:9000")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
SPARK_DRIVER_HOST = os.getenv("SPARK_DRIVER_HOST", "loader")

HDFS_RAW_BASE = f"{HDFS_URL}/raw_data"
HDFS_PARQUET_BASE = f"{HDFS_URL}/processed_data/parquet"
HDFS_HEALTH_CHECK = f"{HDFS_URL}/_loader_health_check"


EXPECTED_FILES = [
    "user_top_albums.csv",
    "user_top_artists.csv",
    "user_top_tracks.csv",
    "users.csv"
]


# ============================
# SPARK SESSION
# ============================

def create_spark_session(app_name="CSVtoParquetLoader"):
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
        .config("spark.pyspark.driver.python", "python3")
        .config("spark.pyspark.python", "/usr/bin/python3")
        .config("spark.executorEnv.PYSPARK_PYTHON", "/usr/bin/python3")
        .config("spark.executorEnv.PYTHONHASHSEED", "0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ============================
# ARCHIVOS LOCALES
# ============================

def discover_local_files():
    files = []
    if not os.path.exists(LOCAL_RAW_PATH):
        raise FileNotFoundError(f"Directorio no encontrado: {LOCAL_RAW_PATH}")

    for name in EXPECTED_FILES:
        p = os.path.join(LOCAL_RAW_PATH, name)
        if os.path.exists(p):
            size_mb = os.path.getsize(p) / (1024 * 1024)
            files.append({"name": name, "path": p, "size_mb": size_mb})

    return files


def safe_read_csv(spark, path):
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "DROPMALFORMED")
        .csv(path)
    )


def transform_df_for_file(df, filename):
    df2 = df.dropDuplicates()

    if "user" in filename.lower() and "top" in filename.lower():
        if "user_id" in df2.columns:
            df2 = df2.filter(col("user_id").isNotNull())

    return df2


# ============================
# HDFS
# ============================

def write_parquet(df, hdfs_output_path):
    df.write.mode("overwrite").parquet(hdfs_output_path)


def backup_raw_to_hdfs(df, hdfs_path):
    df.write.mode("overwrite").option("header", "true").csv(hdfs_path)


def validate_parquet_readable(spark, hdfs_path):
    try:
        df = spark.read.parquet(hdfs_path)
        return True, df.count()
    except Exception as e:
        return False, str(e)


def health_check_hdfs_with_spark(spark):
    try:
        tmp = spark.createDataFrame([(1,)], ["ping"])
        tmp.write.mode("overwrite").parquet(HDFS_HEALTH_CHECK)

        r = spark.read.parquet(HDFS_HEALTH_CHECK)
        return r.count() == 1

    except Exception:
        return False


# ============================
# EJECUCIÓN PRINCIPAL
# ============================

def run_loader():

    start_time = time.time()

    summary = {
        "start_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "files_processed": [],
        "validation": [],
        "errors": []
    }

    try:
        spark = create_spark_session()

        ok_hdfs = health_check_hdfs_with_spark(spark)
        summary["validation"].append({"hdfs_connectivity": ok_hdfs})

        if not ok_hdfs:
            summary["errors"].append("Loader no puede leer/escribir en HDFS")
            spark.stop()
            return False, summary

        local_files = discover_local_files()

        if not local_files:
            summary["errors"].append("No se encontraron archivos CSV")
            spark.stop()
            return False, summary

        for f in local_files:

            fname = f["name"]
            local_path = f["path"]
            base_name = fname.replace(".csv", "")

            hdfs_parquet_path = f"{HDFS_PARQUET_BASE}/{base_name}"
            hdfs_backup_path = f"{HDFS_RAW_BASE}/{base_name}_backup"

            report = {
                "file": fname,
                "local_path": local_path,
                "size_mb": f["size_mb"],
                "parquet_path": hdfs_parquet_path,
                "raw_backup_path": hdfs_backup_path,
                "success": False
            }

            try:
                df = safe_read_csv(spark, f"file://{local_path}")
                report["original_rows"] = df.count()

                backup_raw_to_hdfs(df, hdfs_backup_path)

                df_clean = transform_df_for_file(df, fname)
                report["cleaned_rows"] = df_clean.count()

                write_parquet(df_clean, hdfs_parquet_path)

                ok, result = validate_parquet_readable(spark, hdfs_parquet_path)

                if ok:
                    report["final_rows"] = result
                    report["success"] = True
                else:
                    report["error"] = result

            except Exception as e:
                report["error"] = str(e)

            summary["files_processed"].append(report)

        summary["end_time"] = time.strftime("%Y-%m-%d %H:%M:%S")
        summary["elapsed_seconds"] = round(time.time() - start_time, 2)

        spark.stop()
        return True, summary

    except Exception as e:
        summary["errors"].append(str(e))
        summary["errors"].append(traceback.format_exc())
        return False, summary


if __name__ == "__main__":
    success, report = run_loader()
    print(report)
