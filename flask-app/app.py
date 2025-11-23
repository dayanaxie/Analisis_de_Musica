from flask import Flask, jsonify
import pymysql
import time

app = Flask(__name__)

# Configuración de la base de datos
app.config['DATABASE_HOST'] = 'mariadb'
app.config['DATABASE_USER'] = 'sparkuser'
app.config['DATABASE_PASSWORD'] = 'sparkpass'
app.config['DATABASE_NAME'] = 'music_analysis'

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
    """Verifica la conexión a la base de datos con reintentos"""
    for attempt in range(max_retries):
        try:
            connection = get_db_connection()
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1 as test")
                result = cursor.fetchone()
            
            connection.close()
            print(f"Conexión a MariaDB exitosa (intento {attempt + 1})")
            return True
            
        except Exception as e:
            print(f"Intento {attempt + 1}/{max_retries} falló: {str(e)}")
            if attempt < max_retries - 1:
                print(f"Reintentando en {retry_interval} segundos...")
                time.sleep(retry_interval)
    
    return False

@app.route('/')
def index():
    return "Bienvenido al analizador de música :D"

@app.route('/health')
def health_check():
    """Endpoint para verificar el estado de la aplicación y la BD"""
    try:
        # Verificar conexión a la base de datos
        connection = get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute("SELECT VERSION() as version")
            db_version = cursor.fetchone()
        
        connection.close()
        
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'mysql_version': db_version['version'],
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        })
    
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'database': 'disconnected',
            'error': str(e),
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }), 500


# Verificar la conexión al iniciar la aplicación
if __name__ == '__main__':
    print("Iniciando aplicación Flask...")
    print("Verificando conexión a MariaDB...")
    
    if check_db_connection():
        print("Aplicación conectada con MariaDB")
        app.run(host='0.0.0.0', port=5000, debug=True)
    else:
        print("No se pudo establecer conexión con MariaDB")
        exit(1)