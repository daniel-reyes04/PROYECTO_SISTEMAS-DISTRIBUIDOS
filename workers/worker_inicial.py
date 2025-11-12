import pika
import os
import time
import requests
import psycopg2
import json
from psycopg2 import sql
from pika.exceptions import AMQPConnectionError
# Importamos SentenceTransformer
try:
    from sentence_transformers import SentenceTransformer
except ImportError:
    print("🚨 ERROR: No se encontró la librería 'sentence_transformers'. Asegúrate de que esté en requirements.txt.")
    class SentenceTransformer:
        def __init__(self, *args, **kwargs):
            pass 
        def encode(self, *args, **kwargs):
            return [0.0] * 384
        def get_sentence_embedding_dimension(self):
            return 384

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_USER = os.getenv('RABBITMQ_DEFAULT_USER', 'guest') 
RABBITMQ_PASS = os.getenv('RABBITMQ_DEFAULT_PASS', 'guest') 
QUEUE_NAME = 'cola_indexacion_pelicula' 

DB_HOST = os.getenv('DB_HOST', 'db')
DB_NAME = os.getenv('DB_NAME', 'cinesense_ai_db')
DB_USER = os.getenv('DB_USER', 'cinesense_user')
DB_PASS = os.getenv('DB_PASS', 'password')

TMDB_API_KEY = os.getenv('TMDB_API_KEY')
TMDB_URL = "https://api.themoviedb.org/3/movie/popular"

MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
try:
    model = SentenceTransformer(MODEL_NAME)
    EMBEDDING_DIM = model.get_sentence_embedding_dimension()
except Exception as e:
    print(f"⚠️ Advertencia: No se pudo cargar el modelo SBERT. Usando dummy: {e}")
    class DummyModel:
        def encode(self, *args, **kwargs): return [0.0] * 384
        def get_sentence_embedding_dimension(self): return 384
    model = DummyModel()
    EMBEDDING_DIM = model.get_sentence_embedding_dimension()


def get_db_connection():
    max_retries = 15
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASS
            )
            return conn
        except psycopg2.OperationalError as e:
            print(f"⏳ DB no disponible (intento {attempt + 1}/{max_retries}): {e}. Reintentando en 3 segundos...")
            time.sleep(3)
    raise Exception("❌ No se pudo conectar a la base de datos después de varios intentos.")


def create_tables(conn):
    print("🛠️ Creando/Verificando tablas y extensión pgvector...")
    try:
        with conn.cursor() as cursor:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            
            cursor.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS Peliculas (
                    id SERIAL PRIMARY KEY,
                    tmdb_id INTEGER UNIQUE NOT NULL,
                    titulo VARCHAR(255) NOT NULL,
                    resumen TEXT, 
                    rating_imdb NUMERIC(2, 1),
                    embedding VECTOR(%(dim)s)
                );
            """), {'dim': EMBEDDING_DIM})
            
            conn.commit()
        print("✅ Tablas listas.")
    except Exception as e:
        conn.rollback()
        print(f"❌ Error al crear tablas: {e}")
        
        
def fetch_and_store_movies(conn, url, api_key, limit=500):
    print(f"📡 Cargando hasta {limit} películas populares de TMDB...")
    headers = {} 
    page = 1
    total_loaded = 0

    while total_loaded < limit:
        params = {"language": "es-ES", "page": page, "api_key": api_key} 
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status() # Esto lanzará el 401 si la clave sigue siendo inválida
            data = response.json()
            
            movies_to_insert = []
            for movie in data.get('results', []):
                if total_loaded >= limit:
                    break
                    
                movies_to_insert.append((
                    movie.get('id'),
                    movie.get('title'),
                    movie.get('overview'), # overview de TMDB es el resumen/descripción
                    movie.get('vote_average')
                ))
                total_loaded += 1
                
            if not data.get('results') or data.get('page') >= data.get('total_pages', 0):
                break
                
            page += 1
            
            if movies_to_insert:
                print(f"💾 Insertando {len(movies_to_insert)} películas...")
                with conn.cursor() as cursor:
                    insert_query = """
                        INSERT INTO Peliculas (tmdb_id, titulo, resumen, rating_imdb)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (tmdb_id) DO NOTHING;
                    """
                    cursor.executemany(insert_query, movies_to_insert)
                    conn.commit()
            
            if total_loaded >= limit:
                break
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Error al conectar con TMDB: {e}")
            break
        except Exception as e:
            conn.rollback()
            print(f"❌ Error al insertar datos en DB: {e}")
            break

    print(f"✅ Carga de películas finalizada. Total cargadas: {total_loaded}.")


def get_movie_ids_for_embedding(conn):
    """Obtiene IDs y resumen de películas que no tienen embedding."""
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT tmdb_id, resumen FROM Peliculas WHERE embedding IS NULL;")
            return cursor.fetchall()
    except Exception as e:
        print(f"❌ Error al obtener películas para embedding: {e}")
        return []

def generate_embeddings(conn):
    print("🧠 Generando embeddings para películas sin procesar...")
    movies_to_process = get_movie_ids_for_embedding(conn)
    
    if not movies_to_process:
        print("✅ Todas las películas ya tienen embedding.")
        return

    print(f"🎥 Se necesitan procesar {len(movies_to_process)} películas.")
    
    texts = [resumen for _, resumen in movies_to_process if resumen]
    tmdb_ids = [tmdb_id for tmdb_id, resumen in movies_to_process if resumen]
    
    if not texts:
        print("⚠️ No hay resúmenes válidos para generar embeddings.")
        return

    try:
        embeddings = model.encode(texts, convert_to_tensor=False)
        print(f"✅ Embeddings generados para {len(embeddings)} textos.")

        updates = []
        for tmdb_id, embedding in zip(tmdb_ids, embeddings):
            embedding_str = '[' + ','.join(map(str, embedding)) + ']'
            updates.append((embedding_str, tmdb_id))

        with conn.cursor() as cursor:
            update_query = "UPDATE Peliculas SET embedding = %s WHERE tmdb_id = %s;"
            cursor.executemany(update_query, updates)
            conn.commit()
        print("✅ Base de datos actualizada con nuevos embeddings.")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error al generar/guardar embeddings: {e}")


def verify_population(conn):
    print("🔎 Verificando base de datos...")
    with conn.cursor() as cursor:
        cursor.execute("SELECT titulo, resumen, rating_imdb, embedding FROM Peliculas LIMIT 3;") 
        results = cursor.fetchall()
        if results:
            print(f"🎥 Se encontraron {len(results)} películas de prueba:")
            for i, (titulo, resumen, rating, embedding) in enumerate(results):
                embedding_status = "Sí" if embedding else "No"
                print(f"  {i+1}. {titulo} ({rating}/10) | Resumen: {resumen[:50]}... | Embedding: {embedding_status}")
        else:
            print("⚠️ No hay películas cargadas.")


if __name__ == "__main__":
    print("\n--- Iniciando Worker Inicial de DB ---")
    db_conn = get_db_connection()
    try:
        create_tables(db_conn)

        if not TMDB_API_KEY:
            print("🚨 ERROR: TMDB_API_KEY no configurada. No se cargarán películas.")
        else:
            fetch_and_store_movies(db_conn, TMDB_URL, TMDB_API_KEY, limit=500) 
            
            generate_embeddings(db_conn)
            
            verify_population(db_conn)
            
    except Exception as e:
        print(f"🚨 Error en el proceso inicial del worker: {e}")
    finally:
        if db_conn and not db_conn.closed:
            db_conn.close()
            print("🔌 Conexión a DB cerrada.")