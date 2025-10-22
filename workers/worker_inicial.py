import pika
import os
import time
import requests
import psycopg2

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'user')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS', 'password')
QUEUE_NAME = 'cola_indexacion_pelicula' 
DB_HOST = os.getenv('DB_HOST', 'db')
DB_NAME = os.getenv('DB_NAME', 'cinesense_ai_db')
DB_USER = os.getenv('DB_USER', 'cinesense_user')
DB_PASS = os.getenv('DB_PASS', 'cinesense_pass')
TMDB_API_KEY = os.getenv('TMDB_API_KEY')
TMDB_URL = "https://api.themoviedb.org/3/movie/popular"


def get_db_connection():
    """Espera a que la DB esté disponible y retorna la conexión."""
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASS
            )
            print("Conexión a la Base de Datos PostgreSQL establecida.")
            return conn
        except psycopg2.OperationalError:
            print("Esperando conexión con la DB...")
            time.sleep(3)

def create_tables(conn):
    """Crea las tablas Peliculas, Usuarios y Recomendaciones."""
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Peliculas (
            id SERIAL PRIMARY KEY,
            tmdb_id INT UNIQUE NOT NULL, -- ID de TMDB para evitar duplicados
            titulo VARCHAR(255) NOT NULL,
            sinopsis_original TEXT NOT NULL,
            genero VARCHAR(100),
            rating_imdb DECIMAL(3, 1),
            embedding_sinopsis TEXT  
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Usuarios (
            id SERIAL PRIMARY KEY,
            nombre VARCHAR(100) NOT NULL,
            preferencias_generales JSONB 
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Recomendaciones (
            id SERIAL PRIMARY KEY,
            usuario_id INT REFERENCES Usuarios(id),
            pelicula_id INT REFERENCES Peliculas(id),
            afinidad_predicha DECIMAL(3, 2),
            sinopsis_personalizada TEXT, 
            estado_animo_consulta VARCHAR(50), 
            fecha_consulta TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    print("Tablas de DB creadas (Peliculas, Usuarios, Recomendaciones).")
    cursor.close()

def populate_movies(conn):
    """Descarga películas de TMDB y las inserta en la DB local."""
    print("--- INICIANDO POBLACIÓN DESDE TMDB ---")
    cursor = None 
    params = {'api_key': TMDB_API_KEY, 'language': 'es-MX', 'page': 1} 
    
    try:
        response = requests.get(TMDB_URL, params=params, timeout=10)
        response.raise_for_status() 
        peliculas = response.json().get('results', [])[:10]       
        cursor = conn.cursor() 
        for pelicula in peliculas:
            cursor.execute("""
                INSERT INTO Peliculas (tmdb_id, titulo, sinopsis_original, genero, rating_imdb)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (tmdb_id) DO NOTHING;
            """, (
                pelicula.get('id'), 
                pelicula.get('title'), 
                pelicula.get('overview'),
                ', '.join([str(g) for g in pelicula.get('genre_ids', [])]), # Lista de géneros (solo IDs por ahora)
                pelicula.get('vote_average')
            ))
        
        conn.commit()
        print(f" Población exitosa: Se insertaron {cursor.rowcount} películas en la DB.")
        
    except requests.exceptions.RequestException as e:
        print(f" ERROR de conexión a TMDB: {e}")
    finally:
        if cursor: 
            cursor.close()


def test_rabbitmq_connection():
    """Verifica la conexión a RabbitMQ y publica un mensaje."""
    print("--- 🧠 INICIANDO PRUEBA DE CONEXIÓN ASÍNCRONA ---")
    while True:
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST, 
                port=5672, 
                credentials=credentials,
                heartbeat=600, 
                blocked_connection_timeout=300
            )
            
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            channel.queue_declare(queue=QUEUE_NAME, durable=True) 
            channel.basic_publish(
                exchange='',
                routing_key=QUEUE_NAME,
                body=f'Mensaje de prueba - ID: {int(time.time())}'
            )
            
            print(f" ÉXITO: Conexión establecida y mensaje publicado en la cola: {QUEUE_NAME}")
            
            connection.close()
            break 
            
        except pika.exceptions.AMQPConnectionError:
            print(" Esperando a que RabbitMQ esté disponible... Reintentando en 3 segundos.")
            time.sleep(3)
        except Exception as e:
            print(f" ERROR inesperado en la conexión a RabbitMQ: {e}")
            time.sleep(3)

def verify_population(conn):
    """Realiza un SELECT simple para verificar que la población se realizó con éxito."""
    print("---  CONSULTA DE VERIFICACIÓN DE LA BASE DE DATOS ---")
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT titulo, rating_imdb, sinopsis_original 
            FROM Peliculas 
            LIMIT 3;
        """)
        results = cursor.fetchall()
        if results:
            print(f" ÉXITO: Se encontraron {len(results)} películas de prueba en la tabla 'Peliculas'.")
            print("\n  >> EJEMPLOS DE PELÍCULAS ALMACENADAS:")
            for i, (titulo, rating, sinopsis) in enumerate(results):
                print(f"  {i+1}. TÍTULO: {titulo}")
                print(f"     RATING: {rating}/10")
                print(f"     SINOPSIS: {sinopsis[:80]}...") 
            print("-" * 50)
        else:
            print(" ADVERTENCIA: La tabla 'Peliculas' está vacía.")
            
    except Exception as e:
        print(f" ERROR al intentar consultar la DB: {e}")
    finally:
        cursor.close()

if __name__ == "__main__":
    db_conn = get_db_connection()
    create_tables(db_conn)
    populate_movies(db_conn)
    verify_population(db_conn)
    test_rabbitmq_connection()
    
