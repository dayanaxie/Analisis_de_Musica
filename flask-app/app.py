import time
import pymysql
import requests
from flask import Flask, jsonify, render_template, redirect, url_for, request


app = Flask(__name__)

app.config['DATABASE_HOST'] = 'mariadb'
app.config['DATABASE_USER'] = 'sparkuser'
app.config['DATABASE_PASSWORD'] = 'sparkpass'
app.config['DATABASE_NAME'] = 'music_analysis'

LOADER_URL = "http://loader:5001"
ANALYSIS_URL = "http://analytics:5002"


loader_running = False
loader_status = "idle"
loader_logs = []

analysis_running = False
analysis_status = "idle"
analysis_logs = []


def get_db_connection():
    return pymysql.connect(
        host=app.config['DATABASE_HOST'],
        user=app.config['DATABASE_USER'],
        password=app.config['DATABASE_PASSWORD'],
        database=app.config['DATABASE_NAME'],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )


def check_db_connection(max_retries=10, retry_interval=5):
    for attempt in range(max_retries):
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS ok")
                cur.fetchone()
            conn.close()
            print(f"MariaDB: Conexión exitosa (intento {attempt + 1})")
            return True
        except Exception as e:
            print(f"Error de conexión (intento {attempt + 1}): {e}")
            time.sleep(retry_interval)
    return False


@app.route("/")
def index():
    return render_template("loader.html")

@app.route("/loader")
def loader():
    return render_template("loader.html")


# ---------- POPULARIDAD – JSON PARA CHARTS ----------

@app.route("/popularidad")
def popularidad():
    return render_template("popularidad.html")

def query_to_json(sql):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        return rows
    finally:
        conn.close()

@app.route("/api/top20/artists")
def api_top20_artists():
    return jsonify(query_to_json(
        "SELECT artist_name, users_count, percentage "
        "FROM top_20_artists ORDER BY users_count DESC LIMIT 20"))

@app.route("/api/top20/tracks")
def api_top20_tracks():
    return jsonify(query_to_json(
        "SELECT track_name, users_count, percentage "
        "FROM top_20_tracks ORDER BY users_count DESC LIMIT 20"))

@app.route("/api/top20/albums")
def api_top20_albums():
    return jsonify(query_to_json(
        "SELECT album_name, users_count, percentage "
        "FROM top_20_albums ORDER BY users_count DESC LIMIT 20"))

@app.route("/api/top1/artist")
def api_top1_artist():
    rows = query_to_json(
        "SELECT artist_name AS top1_artist, users_count, percentage "
        "FROM same_top1_artist")
    if not rows:
        return jsonify([])
    return jsonify(rows)

@app.route("/api/mention/stats")
def api_mention_stats():
    return jsonify(query_to_json(
        "SELECT metric, value FROM artist_mention_stats"))

@app.route("/api/longtail")
def api_longtail():
    return jsonify(query_to_json(
        "SELECT total_artists, artists_in_tail, tail_percentage FROM long_tail_80"))

# ---------- APIs de Conteos simples ----------

@app.route("/api/items_por_usuario")
def api_items_por_usuario():
    artist_total_row = query_to_json("SELECT COUNT(*) AS n FROM user_artist_counts")
    track_total_row  = query_to_json("SELECT COUNT(*) AS n FROM user_track_counts")
    album_total_row  = query_to_json("SELECT COUNT(*) AS n FROM user_album_counts")

    total_artistas = artist_total_row[0]["n"] if artist_total_row else 0
    total_tracks   = track_total_row[0]["n"]  if track_total_row else 0
    total_albums   = album_total_row[0]["n"]  if album_total_row else 0

    artist_stats_rows = query_to_json("SELECT metric, value FROM user_artist_stats")
    track_stats_rows  = query_to_json("SELECT metric, value FROM user_track_stats")
    album_stats_rows  = query_to_json("SELECT metric, value FROM user_album_stats")

    def extract_stats(label, total_users, rows):
        mean = None
        median = None
        for r in rows:
            metric = str(r["metric"]).strip().lower()
            if metric == "mean":
                mean = float(r["value"])
            elif metric == "median":
                median = float(r["value"])
        return {
            "item_type": label,
            "total_usuarios": int(total_users),
            "media": mean,
            "mediana": median,
        }

    stats = [
        extract_stats("artistas",  total_artistas, artist_stats_rows),
        extract_stats("canciones", total_tracks,   track_stats_rows),
        extract_stats("albumes",   total_albums,   album_stats_rows),
    ]

    return jsonify(stats)

@app.route("/api/artistas_unicos")
def api_artistas_unicos():
    rows = query_to_json("""
        SELECT unique_artists, unique_tracks, unique_albums
        FROM unique_items
        LIMIT 1
    """)
    return jsonify(rows[0] if rows else {})


@app.route("/api/top3_identicas")
def api_top3_identicas():
    # Ya está limitado a top 3 combos en Spark, igual ordenamos por user_count
    rows = query_to_json("""
        SELECT top1, top2, top3, user_count
        FROM users_same_top3_from_top10
        ORDER BY user_count DESC
    """)
    return jsonify(rows)


@app.route("/api/gustos_concentrados")
def api_gustos_concentrados():
    # Lista de usuarios
    users = query_to_json("""
        SELECT user_id, main_artist, track_count
        FROM users_top5_single_artist
        ORDER BY main_artist, user_id
    """)

    artists = query_to_json("""
        SELECT main_artist, COUNT(*) AS user_count
        FROM users_top5_single_artist
        GROUP BY main_artist
        ORDER BY user_count DESC
        LIMIT 20
    """)

    return jsonify({
        "users": users,
        "artists": artists
    })

@app.route("/conteos")
def conteos():
    return render_template("conteos.html")

@app.route("/concurrencia")
def concurrencia():
    return render_template("concurrencia.html")

@app.route("/comparaciones")
def comparaciones():
    return render_template("comparaciones.html")


# ---------- APIs de Comparaciones simples ----------

@app.route("/api/top_artists_active_users")
def api_top_artists_active_users():
    rows = query_to_json("""
        SELECT artist_name, active_users
        FROM top_artists_active_users
        ORDER BY active_users DESC
        LIMIT 20
    """)
    return jsonify(rows)


@app.route("/api/cross_popularity")
def api_cross_popularity():
    rows = query_to_json("""
        SELECT artist_name,
               mentions_in_artists,
               mentions_in_tracks,
               difference
        FROM cross_popularity_artists
        ORDER BY difference DESC
        LIMIT 20
    """)
    return jsonify(rows)


@app.route("/api/artists_diversity")
def api_artists_diversity():
    rows = query_to_json("""
        SELECT artist_name,
               distinct_users,
               distinct_tracks
        FROM artists_diversity
        ORDER BY distinct_users DESC, distinct_tracks DESC
        LIMIT 20
    """)
    return jsonify(rows)


# ----------- CALIDAD ----------

@app.route("/calidad")
def calidad():
    return render_template("calidad.html")

@app.route("/api/data_quality")
def api_data_quality():
    rows = query_to_json("SELECT SUM(affected_users) AS total FROM data_quality")
    return jsonify({"total": int(rows[0]["total"]) if rows else 0})
 
@app.route("/api/outliers_summary")
def api_outliers_summary():
    try:
        limit = int(request.args.get("limit", 10))
        total_row = query_to_json("SELECT COUNT(*) AS total FROM outlier_users")
        total = int(total_row[0]["total"]) if total_row else 0

        sample_rows = query_to_json(f"""
            SELECT user_id, items_count, percentile
            FROM outlier_users
            LIMIT {limit}
        """)

        for u in sample_rows:
            user_id_str = str(u["user_id"])
            u["user_id"] = user_id_str[:6] + "…" if len(user_id_str) > 6 else user_id_str

        return jsonify({
            "total_population": total,
            "sample_size": len(sample_rows),
            "sample": sample_rows
        })

    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500

@app.route("/api/low_coverage_summary")
def api_low_coverage_summary():
    try:
        limit = int(request.args.get("limit", 10))
        total_row = query_to_json("SELECT COUNT(*) AS total FROM low_coverage_artists")
        total = int(total_row[0]["total"]) if total_row else 0

        sample_rows = query_to_json(f"""
            SELECT artist_name, mentions
            FROM low_coverage_artists
            ORDER BY mentions ASC, artist_name
            LIMIT {limit}
        """)

        return jsonify({
            "total_population": total,
            "sample_size": len(sample_rows),
            "sample": sample_rows
        })

    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


# ----------- CONCURRENCIA ----------

@app.route("/api/concurrencia/pares")
def api_concurrencia_pares():
    rows = query_to_json("""
        SELECT artist1, artist2, pair_count
        FROM top_50_artist_pairs
        ORDER BY pair_count DESC
        LIMIT 50
    """)
    return jsonify(rows)


@app.route("/api/concurrencia/tripletas")
def api_concurrencia_tripletas():
    rows = query_to_json("""
        SELECT artist1, artist2, artist3, triplet_count
        FROM top_50_artist_triplets
        ORDER BY triplet_count DESC
        LIMIT 50
    """)
    return jsonify(rows)


@app.route("/api/concurrencia/overlap")
def api_concurrencia_overlap():
    row = query_to_json("""
        SELECT overlap_count, total_users, overlap_ratio
        FROM artist_track_overlap
        LIMIT 1
    """)
    return jsonify(row[0] if row else {})


@app.route("/api/concurrencia/posicion_promedio")
def api_concurrencia_posicion_promedio():
    rows = query_to_json("""
        SELECT artist_name, avg_position, user_count
        FROM artist_average_position
        ORDER BY avg_position ASC
        LIMIT 200
    """)
    return jsonify(rows)


@app.route("/api/concurrencia/top1_en_top5")
def api_concurrencia_top1_en_top5():
    row = query_to_json("""
        SELECT matched_users, total_users, ratio
        FROM top1_in_global_top5_frequency
        LIMIT 1
    """)
    return jsonify(row[0] if row else {})


@app.route("/api/concurrencia/estabilidad")
def api_concurrencia_estabilidad():
    row = query_to_json("""
        SELECT stable_users, total_users, ratio
        FROM position_stability_users
        LIMIT 1
    """)
    return jsonify(row[0] if row else {})


# ----------- ANALYSIS ---------- 

@app.route("/analysis")
def analysis_page():
    return render_template("analysis.html")

@app.route("/admin/load-data", methods=["POST"])
def trigger_loader():
    global loader_status, loader_logs, loader_running

    try:
        loader_status_resp = requests.get(f"{LOADER_URL}/status", timeout=5).json()
        if loader_status_resp.get("running"):
            return jsonify({
                "success": False,
                "message": "Loader is already running",
                "loader_status": loader_status_resp,
            }), 409
    except Exception as e:
        return jsonify({
            "success": False,
            "message": "Loader service is unreachable",
            "error": str(e)
        }), 503

    try:
        response = requests.post(f"{LOADER_URL}/run-loader", timeout=3600)
        data = response.json()

        loader_running = data.get("status") == "running"
        loader_status = data.get("status")
        loader_logs = data.get("logs", [])

        return jsonify(data)

    except Exception as e:
        return jsonify({
            "success": False,
            "message": "Failed to trigger loader",
            "error": str(e)
        }), 500

@app.route("/admin/loader-status")
def loader_status_route():
    try:
        data = requests.get(f"{LOADER_URL}/status", timeout=5).json()
        return jsonify(data)
    except Exception as e:
        return jsonify({
            "success": False,
            "message": "Loader service unreachable",
            "error": str(e)
        }), 503

@app.route("/admin/run-analysis", methods=["POST"])
def trigger_analysis():
    global analysis_status, analysis_logs, analysis_running

    try:
        response = requests.post(f"{ANALYSIS_URL}/run-analysis", timeout=3600)
        data = response.json()

        analysis_status = data.get("status", "unknown")
        analysis_running = analysis_status == "running"
        analysis_logs = data.get("logs", [])

        return jsonify(data), response.status_code

    except Exception as e:
        return jsonify({
            "success": False,
            "status": "error",
            "message": "Failed to trigger analysis service",
            "error": str(e)
        }), 500


@app.route("/health")
def health():
    db_ok = False
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT VERSION() as version")
            version = cursor.fetchone()
        conn.close()
        db_ok = True
    except Exception as e:
        version = {"error": str(e)}

    try:
        loader_resp = requests.get(f"{LOADER_URL}/status", timeout=5).json()
        loader_ok = True
    except:
        loader_resp = {"error": "loader unreachable"}
        loader_ok = False

    return jsonify({
        "status": "healthy" if db_ok and loader_ok else "unhealthy",
        "database": "connected" if db_ok else "disconnected",
        "mysql_version": version,
        "loader_service": loader_resp
    })

if __name__ == "__main__":
    print("Iniciando Flask...")
    print("Verificando conexión a MariaDB...")

    if check_db_connection():
        print("Conexión exitosa. Iniciando servidor Flask...")
        app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
    else:
        print("Error: No se pudo conectar a MariaDB.")
        exit(1)
