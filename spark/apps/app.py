#!/usr/bin/env python3

from flask import Flask, jsonify
from analytics import run_analysis  

app = Flask(__name__)

@app.route("/run-analysis", methods=["POST", "GET"])
def run_analysis_route():
    """
    Ruta para ejecutar el análisis de Spark y guardar resultados en MariaDB.
    """
    try:
        run_analysis()
        return jsonify({
            "status": "ok",
            "message": "Análisis completado y guardado en MariaDB."
        }), 200

    except Exception as e:
        # Si algo falla, lo devolvemos como error
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002, debug=True)
