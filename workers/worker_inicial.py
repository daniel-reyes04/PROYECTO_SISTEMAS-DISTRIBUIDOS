import pika
import os
import time
import requests
import psycopg2
import json
from sentence_transformers import SentenceTransformer

# ---------------- CONFIGURACIONES ---------------- #
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
EMBEDDING_DIM = model.get_sentence_embedding_dimension()


# ---------------- CONEXIÓN DB ---------------- #
def get_db_connection():
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASS
            )
            print("✅ Conexión a la Base de Datos establecida.")
            return conn
        except psycopg2.OperationalError:
            print("⏳ Esperando conexión con la DB...")
            time.sleep(3)


# ---------------- CREACIÓN DE TABLAS ---------------- #
def create_tables(conn):
    with conn.cursor() as cursor:
        print("🧱 Recreando tablas...")
        cursor.execute("DROP TABLE IF EXISTS Recomendaciones;")
        cursor.execute("DROP TABLE IF EXISTS Usuarios;")
        cursor.execute("DROP TABLE IF EXISTS Peliculas;")

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS Peliculas (
                id SERIAL PRIMARY KEY,
                tmdb_id INT UNIQUE NOT NULL,
                titulo VARCHAR(255) NOT NULL,
                genero JSONB,
                rating_imdb DECIMAL(3, 2),
                sinopsis_original TEXT,
                embedding_sinopsis JSONB,
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
        print("✅ Tablas creadas exitosamente.")


# ---------------- OBTENER Y GUARDAR PELÍCULAS ---------------- #
def fetch_and_store_movies(conn, api_url, api_key, limit=20):
    print(f"🎬 Obteniendo {limit} películas desde TMDB...")
    params = {'api_key': api_key, 'language': 'es-ES', 'page': 1}

    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        data = response.json()

        with conn.cursor() as cursor:
            inserted = 0
            for movie in data.get('results', [])[:limit]:
                if movie.get('overview'):
                    cursor.execute("""
                        INSERT INTO Peliculas (tmdb_id, titulo, genero, rating_imdb, sinopsis_original, fecha_lanzamiento)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (tmdb_id) DO NOTHING;
                    """, (
                        movie.get('id'),
                        movie.get('title'),
                        json.dumps(movie.get('genre_ids', [])),
                        movie.get('vote_average'),
                        movie.get('overview'),
                        movie.get('release_date')
                    ))
                    inserted += 1
            conn.commit()
        print(f"✅ {inserted} películas insertadas en la base de datos.")

    except requests.exceptions.RequestException as e:
        print(f"⚠️ Error de red al obtener películas: {e}")
    except Exception as e:
        print(f"⚠️ Error inesperado al insertar películas: {e}")


# ---------------- GENERAR EMBEDDINGS ---------------- #
def generate_embeddings(conn):
    print("🧠 Generando embeddings para las películas sin vectorizar...")
    with conn.cursor() as cursor:
        cursor.execute("SELECT id, sinopsis_original FROM Peliculas WHERE embedding_sinopsis IS NULL;")
        rows = cursor.fetchall()

        for movie_id, sinopsis in rows:
            try:
                embedding = model.encode(sinopsis).tolist()
                cursor.execute(
                    "UPDATE Peliculas SET embedding_sinopsis = %s WHERE id = %s;",
                    (json.dumps(embedding), movie_id)
                )
            except Exception as e:
                print(f"⚠️ Error al generar embedding para película ID {movie_id}: {e}")

        conn.commit()
    print(f"✅ {len(rows)} embeddings generados y almacenados.")


# ---------------- OBTENER IDS Y PUBLICAR A RABBITMQ ---------------- #
def get_movie_ids_for_indexing(conn):
    with conn.cursor() as cursor:
        cursor.execute("SELECT id FROM Peliculas;")
        return [row[0] for row in cursor.fetchall()]


def publish_for_indexing(movie_ids, max_retries=5):
    print("🚀 Encolando películas para indexación en RabbitMQ...")
    for attempt in range(max_retries):
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
            with pika.BlockingConnection(parameters) as connection:
                channel = connection.channel()
                channel.queue_declare(queue=QUEUE_NAME, durable=True)

                for movie_id in movie_ids:
                    message = json.dumps({"pelicula_id": movie_id})
                    channel.basic_publish(
                        exchange='',
                        routing_key=QUEUE_NAME,
                        body=message,
                        properties=pika.BasicProperties(delivery_mode=pika.spec.DeliveryMode.Persistent)
                    )

                print(f"✅ {len(movie_ids)} IDs de películas encoladas correctamente.")
                return
        except pika.exceptions.AMQPConnectionError:
            print(f"⏳ RabbitMQ no disponible (intento {attempt + 1}/{max_retries})...")
            time.sleep(3)
    print("❌ No se pudo conectar con RabbitMQ después de varios intentos.")


# ---------------- VERIFICACIÓN ---------------- #
def verify_population(conn):
    print("🔎 Verificando base de datos...")
    with conn.cursor() as cursor:
        cursor.execute("SELECT titulo, rating_imdb FROM Peliculas LIMIT 3;")
        results = cursor.fetchall()
        if results:
            print(f"🎥 Se encontraron {len(results)} películas de prueba:")
            for i, (titulo, rating) in enumerate(results):
                print(f"  {i+1}. {titulo} ({rating}/10)")
        else:
            print("⚠️ No hay películas cargadas.")


# ---------------- MAIN ---------------- #
if __name__ == "__main__":
    db_conn = get_db_connection()
    try:
        create_tables(db_conn)

        if not TMDB_API_KEY:
            print("🚨 ERROR: TMDB_API_KEY no configurada. No se cargarán películas.")
        else:
            fetch_and_store_movies(db_conn, TMDB_URL, TMDB_API_KEY, limit=5)
            generate_embeddings(db_conn)
            verify_population(db_conn)
            movie_ids = get_movie_ids_for_indexing(db_conn)
            publish_for_indexing(movie_ids)

    except Exception as e:
        print(f"🚨 ERROR FATAL en worker_inicial: {e}")
    finally:
        db_conn.close()
        print("🔚 Conexión cerrada.")
