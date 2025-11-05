import pika
import os
import time
import requests
import psycopg2
import json
from sentence_transformers import SentenceTransformer


RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'user')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS', 'password')
QUEUE_NAME = 'cola_indexacion_pelicula' 
DB_HOST = os.getenv('DB_HOST', 'db')
DB_NAME = os.getenv('DB_NAME', 'cinesense_ai_db')
DB_USER = os.getenv('DB_USER', 'cinesense_user')
DB_PASS = os.getenv('DB_PASS', 'password')
TMDB_API_KEY = os.getenv('TMDB_API_KEY')
TMDB_URL = "https://api.themoviedb.org/3/movie/popular"
MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
model = SentenceTransformer(MODEL_NAME)
EMBEDDING_DIM = model.get_sentence_embedding_dimension() # Debe ser 384

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
    print("Iniciando borrado y creación de tablas...")
    

    cursor.execute("DROP TABLE IF EXISTS Recomendaciones;")
    cursor.execute("DROP TABLE IF EXISTS Usuarios;")
    cursor.execute("DROP TABLE IF EXISTS Peliculas;")
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS Peliculas (
            id SERIAL PRIMARY KEY,
            tmdb_id INT UNIQUE NOT NULL,
            titulo VARCHAR(255) NOT NULL,
            genero VARCHAR(100),
            rating_imdb DECIMAL(2, 1),
            sinopsis_original TEXT,
            embedding_sinopsis JSONB, -- Campo para almacenar el vector de embedding como JSON string
            fecha_lanzamiento DATE
        );
    """)


    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Usuarios (
            id SERIAL PRIMARY KEY,
            nombre VARCHAR(100) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Recomendaciones (
            id SERIAL PRIMARY KEY,
            usuario_id INT REFERENCES Usuarios(id),
            pelicula_id INT REFERENCES Peliculas(id),
            emocion_detectada VARCHAR(50),
            fecha_recomendacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    print("✅ Tablas creadas/recreadas exitosamente.")
    cursor.close()

def fetch_and_store_movies(conn, api_url, api_key, limit=20):
    """
    Obtiene películas de TMDB y las almacena en la base de datos, 
    encolando las IDs para el procesamiento de embeddings.
    """
    print(f"--- FETCHING {limit} PELÍCULAS DE TMDB ---")
    
    params = {
        'api_key': api_key,
        'language': 'es-ES',
        'page': 1
    }
    
    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status() # Lanza excepción para códigos de error HTTP
        data = response.json()
        
        movies_to_insert = []
        for movie in data.get('results', [])[:limit]:
            if movie.get('overview'): # Solo insertamos si hay sinopsis
                movies_to_insert.append((
                    movie.get('id'),
                    movie.get('title'),
                    ', '.join([str(g) for g in movie.get('genre_ids', [])]), # Lista de IDs de género
                    movie.get('vote_average'),
                    movie.get('overview'),
                    movie.get('release_date')
                ))

        cursor = conn.cursor()
        for movie in movies_to_insert:
            try:
                cursor.execute("""
                    INSERT INTO Peliculas (tmdb_id, titulo, genero, rating_imdb, sinopsis_original, fecha_lanzamiento)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tmdb_id) DO NOTHING;
                """, movie)
            except Exception as e:
                print(f"ERROR al insertar {movie[1]}: {e}")
                conn.rollback()
        
        conn.commit()
        print(f"✅ {len(movies_to_insert)} películas intentadas insertar.")

    except requests.exceptions.RequestException as e:
        print(f"⚠️ ERROR de red al obtener películas: {e}")
    except Exception as e:
        print(f"⚠️ ERROR inesperado durante la inserción: {e}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()

def get_movie_ids_for_indexing(conn):
    """Obtiene todos los IDs de películas que necesitan encolarse."""
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM Peliculas;")
    ids = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return ids
    
def publish_for_indexing(movie_ids):
    """Publica los IDs de las películas en RabbitMQ para que el recomendador procese los embeddings."""
    print("--- ENCOLANDO PELÍCULAS PARA INDEXACIÓN DE EMBEDDINGS ---")
    connection = None
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()


        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        
        for movie_id in movie_ids:
            message = json.dumps({"pelicula_id": movie_id})
            channel.basic_publish(
                exchange='',
                routing_key=QUEUE_NAME,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.DeliveryMode.Persistent  # Hace que el mensaje persista en el broker
                )
            )
            # print(f"  [📤] Encolada película ID: {movie_id}") # Demasiados logs, solo mostramos resumen.

        print(f"✅ Tarea de indexación: {len(movie_ids)} IDs de películas encolados en '{QUEUE_NAME}'.")
        
    except pika.exceptions.AMQPConnectionError:
        print(" Esperando a que RabbitMQ esté disponible... Reintentando en 3 segundos.")
        time.sleep(3)
    except Exception as e:
        print(f" ERROR inesperado en la conexión a RabbitMQ: {e}")
        time.sleep(3)
    finally:
        if connection and not connection.is_closed:
            connection.close()

def verify_population(conn):
    """Realiza un SELECT simple para verificar que la población se realizó con éxito."""
    print("---  CONSULTA DE VERIFICACIÓN DE LA BASE DE DATOS ---\n")
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
            print("  >> EJEMPLOS DE PELÍCULAS ALMACENADAS:")
            for i, (titulo, rating, sinopsis) in enumerate(results):
                print(f"  {i+1}. TÍTULO: {titulo}")
                print(f"     RATING: {rating}/10")
                print(f"     SINOPSIS: {sinopsis[:80]}...") 
            print("-" * 50)
        else:
            print(" ADVERTENCIA: La tabla 'Peliculas' está vacía. Revise la API Key de TMDB.")
            
    except Exception as e:
        print(f" ERROR al intentar consultar la DB: {e}")
    finally:
        cursor.close()

if __name__ == "__main__":
    db_conn = get_db_connection()
    try:
   
        create_tables(db_conn)
        if not TMDB_API_KEY:
            print("🚨 ERROR: TMDB_API_KEY no está configurada. No se cargarán películas.")
        else:
            fetch_and_store_movies(db_conn, TMDB_URL, TMDB_API_KEY, limit=30)

        verify_population(db_conn)
        movie_ids = get_movie_ids_for_indexing(db_conn)
        publish_for_indexing(movie_ids)
        
    except Exception as e:
        print(f"🚨 ERROR FATAL en worker_inicial: {e}")
    finally:
        if db_conn:
            db_conn.close()