from flask import Flask, jsonify
import traceback
from data_loader import run_loader  

app = Flask(__name__)

loader_running = False
loader_logs = []
loader_status = "idle"

@app.route("/")
def home():
    return jsonify({"service": "loader", "status": "ok"})

@app.route("/run-loader", methods=["POST"])
def run_loader_route():
    global loader_running, loader_logs, loader_status

    if loader_running:
        return jsonify({
            "success": False,
            "message": "Loader is already running",
            "status": loader_status,
            "logs": loader_logs
        }), 409

    loader_running = True
    loader_status = "running"
    loader_logs = []

    try:
        loader_logs.append("Loader started")
        
        # ejecuta el loader
        result = run_loader()
        
        loader_status = "completed" if result else "failed"
        loader_logs.append(f"Loader result: {result}")

        return jsonify({
            "success": result,
            "status": loader_status,
            "logs": loader_logs
        })
    
    except Exception as e:
        loader_status = "failed"
        error_info = traceback.format_exc()
        loader_logs.append(str(e))
        loader_logs.append(error_info)

        return jsonify({
            "success": False,
            "status": loader_status,
            "error": str(e),
            "traceback": error_info,
            "logs": loader_logs
        }), 500
    
    finally:
        loader_running = False

@app.route("/status")
def status():
    return jsonify({
        "running": loader_running,
        "status": loader_status,
        "logs": loader_logs
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
