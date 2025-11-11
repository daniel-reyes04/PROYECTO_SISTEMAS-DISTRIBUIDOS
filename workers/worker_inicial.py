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
    # Clase simulada para evitar un error fatal si el worker es invocado sin la librería.
    class SentenceTransformer:
        def __init__(self, *args, **kwargs):
            # Usar un modelo pequeño multilingüe
            pass 
        def encode(self, *args, **kwargs):
            # Retorna un vector dummy si no está instalado
            return [0.0] * 384
        def get_sentence_embedding_dimension(self):
            return 384

# ---------------- CONFIGURACIONES ---------------- #
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
# Inicialización del modelo (solo ocurre una vez al inicio)
try:
    model = SentenceTransformer(MODEL_NAME)
    EMBEDDING_DIM = model.get_sentence_embedding_dimension()
except Exception as e:
    print(f"⚠️ Advertencia: No se pudo cargar el modelo SBERT. Usando dummy: {e}")
    # Definición de la clase DummyModel en caso de error
    class DummyModel:
        def encode(self, *args, **kwargs): return [0.0] * 384
        def get_sentence_embedding_dimension(self): return 384
    model = DummyModel()
    EMBEDDING_DIM = model.get_sentence_embedding_dimension()


# ---------------- CONEXIÓN DB ---------------- #
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


# ---------------- TABLAS ---------------- #
def create_tables(conn):
    print("🛠️ Creando/Verificando tablas y extensión pgvector...")
    try:
        with conn.cursor() as cursor:
            # 1. Habilitar pgvector
            cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            
            # 2. Crear tabla Peliculas. CORRECCIÓN: Usar 'resumen' como columna de sinopsis.
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
        # AÑADIDO: Incluimos 'api_key' directamente en los parámetros de la URL
        params = {"language": "es-ES", "page": page, "api_key": api_key} 
        
        try:
            # La solicitud ahora envía la clave en el query string: .../popular?api_key=XXX...
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status() # Esto lanzará el 401 si la clave sigue siendo inválida
            data = response.json()
            
            movies_to_insert = []
            for movie in data.get('results', []):
                # Detener si se alcanza el límite
                if total_loaded >= limit:
                    break
                    
                # Extraer datos. NOTA: El 'overview' de TMDB va a la columna 'resumen'
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
            
            # Insertar en DB
            if movies_to_insert:
                print(f"💾 Insertando {len(movies_to_insert)} películas...")
                with conn.cursor() as cursor:
                    # Usamos ON CONFLICT para ignorar películas duplicadas
                    insert_query = """
                        INSERT INTO Peliculas (tmdb_id, titulo, resumen, rating_imdb)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (tmdb_id) DO NOTHING;
                    """
                    cursor.executemany(insert_query, movies_to_insert)
                    conn.commit()
            
            # Si se alcanzó el límite, salimos del bucle
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


# ---------------- EMBEDDINGS ---------------- #
def get_movie_ids_for_embedding(conn):
    """Obtiene IDs y resumen de películas que no tienen embedding."""
    try:
        with conn.cursor() as cursor:
            # CORRECCIÓN: Obtener el id de TMDB y el resumen
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
    
    # Prepara los textos: se usa solo el resumen para el embedding de similitud
    # Filtramos por resumen vacío, aunque la DB permite NULL, para evitar errores en el encode
    texts = [resumen for _, resumen in movies_to_process if resumen]
    tmdb_ids = [tmdb_id for tmdb_id, resumen in movies_to_process if resumen]
    
    if not texts:
        print("⚠️ No hay resúmenes válidos para generar embeddings.")
        return

    try:
        # Generar embeddings
        embeddings = model.encode(texts, convert_to_tensor=False)
        print(f"✅ Embeddings generados para {len(embeddings)} textos.")

        # Actualizar la base de datos
        updates = []
        for tmdb_id, embedding in zip(tmdb_ids, embeddings):
            # El embedding debe convertirse a una cadena con formato de array de PostgreSQL
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


# ---------------- VERIFICACIÓN ---------------- #
def verify_population(conn):
    print("🔎 Verificando base de datos...")
    with conn.cursor() as cursor:
        # Incluir 'resumen' y estado del 'embedding' para la verificación
        cursor.execute("SELECT titulo, resumen, rating_imdb, embedding FROM Peliculas LIMIT 3;") 
        results = cursor.fetchall()
        if results:
            print(f"🎥 Se encontraron {len(results)} películas de prueba:")
            for i, (titulo, resumen, rating, embedding) in enumerate(results):
                # El embedding es un array/cadena de texto, si tiene contenido, asumimos que existe
                embedding_status = "Sí" if embedding else "No"
                print(f"  {i+1}. {titulo} ({rating}/10) | Resumen: {resumen[:50]}... | Embedding: {embedding_status}")
        else:
            print("⚠️ No hay películas cargadas.")


# ---------------- MAIN ---------------- #
if __name__ == "__main__":
    print("\n--- Iniciando Worker Inicial de DB ---")
    db_conn = get_db_connection()
    try:
        # 1. Crear extensión y tablas (asegurando columna 'resumen')
        create_tables(db_conn)

        if not TMDB_API_KEY:
            print("🚨 ERROR: TMDB_API_KEY no configurada. No se cargarán películas.")
        else:
            # 2. Cargar datos desde TMDB
            fetch_and_store_movies(db_conn, TMDB_URL, TMDB_API_KEY, limit=50) # Aumentado a 50 para una mejor base
            
            # 3. Generar embeddings
            generate_embeddings(db_conn)
            
            # 4. Verificación final
            verify_population(db_conn)
            
    except Exception as e:
        print(f"🚨 Error en el proceso inicial del worker: {e}")
    finally:
        if db_conn and not db_conn.closed:
            db_conn.close()
            print("🔌 Conexión a DB cerrada.")