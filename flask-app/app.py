import time
import pymysql
import requests
from flask import Flask, jsonify, render_template, redirect, url_for


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

@app.route("/popularidad")
def popularidad():
    return render_template("popularidad.html")

@app.route("/conteos")
def conteos():
    return render_template("conteos.html")

@app.route("/concurrencia")
def concurrencia():
    return render_template("concurrencia.html")

@app.route("/comparaciones")
def comparaciones():
    return render_template("comparaciones.html")

@app.route("/calidad")
def calidad():
    return render_template("calidad.html")

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
