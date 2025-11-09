import pika
import os
import time
import requests
import psycopg2
import json
from sentence_transformers import SentenceTransformer
from psycopg2 import sql

# ---------------- CONFIGURACIONES ---------------- #
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_USER = os.getenv('RABBITMQ_DEFAULT_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_DEFAULT_PASS', 'guest')
# La cola que usa la WebApp para enviar consultas de recomendación
QUEUE_NAME = 'recommendation_queue' 

DB_HOST = os.getenv('DB_HOST', 'db')
DB_NAME = os.getenv('DB_NAME', 'cinesense_ai_db')
DB_USER = os.getenv('DB_USER', 'cinesense_user')
DB_PASS = os.getenv('DB_PASS', 'cinesense_pass')

MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
model = SentenceTransformer(MODEL_NAME)
EMBEDDING_DIM = model.get_sentence_embedding_dimension()


# ---------------- CONEXIÓN DB ---------------- #
def get_db_connection():
    """Establece una conexión a la base de datos con reintentos."""
    max_retries = 15
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASS,
                port=5432
            )
            return conn
        except psycopg2.OperationalError:
            time.sleep(3)
    raise Exception("❌ No se pudo conectar a la base de datos después de varios intentos.")


# ---------------- LÓGICA DE RECOMENDACIÓN ---------------- #

def find_recommendation(conn, user_query):
    """
    Genera el embedding de la consulta del usuario y encuentra la película más similar 
    usando la distancia del coseno (operador '<=>' de pgvector).
    """
    print(f"🧠 Generando embedding para la consulta: '{user_query}'")
    
    # 1. Generar el embedding de la consulta
    query_embedding = model.encode([user_query])[0]
    query_embedding_list = query_embedding.tolist()
    
    # 2. Convertir el embedding de la consulta a formato string/json para PostgreSQL
    vector_string = json.dumps(query_embedding_list)
    
    # 3. Buscar la película con el embedding más cercano (operador de distancia del coseno '<=>')
    select_query = sql.SQL("""
        SELECT 
            titulo, 
            sinopsis_original, 
            rating_imdb,
            # El operador '<=>' calcula la distancia del coseno.
            embedding_sinopsis <=> %s::vector AS distance
        FROM Peliculas 
        WHERE embedding_sinopsis IS NOT NULL
        ORDER BY distance ASC
        LIMIT 1;
    """)
    
    with conn.cursor() as cursor:
        cursor.execute(select_query, (vector_string,))
        result = cursor.fetchone()
        
        if result:
            titulo, sinopsis, rating, distance = result
            # La distancia del coseno va de 0 a 2. Cercano a 0 es más similar.
            similarity = 1 - distance / 2
            
            return {
                "titulo": titulo,
                "sinopsis": sinopsis,
                "rating": f"{rating:.2f}/10",
                "similitud": f"{similarity:.4f}"
            }
        
    return None


# ---------------- CONSUMIDOR RABBITMQ ---------------- #

def callback(ch, method, properties, body):
    """Procesa el mensaje de la cola: encuentra la recomendación y la imprime."""
    try:
        data = json.loads(body)
        user_query = data.get('query', 'No se especificó consulta')
        
        print(f"⚙️ Recibido nuevo trabajo: Consulta de usuario: '{user_query}'")
        
        # Conectar a la DB e intentar la recomendación
        conn = get_db_connection()
        recommendation = find_recommendation(conn, user_query)
        conn.close()
        
        if recommendation:
            print("✨ RECOMENDACIÓN ENCONTRADA:")
            print(f"   Título: {recommendation['titulo']}")
            print(f"   Similitud (Coseno): {recommendation['similitud']}")
            # Nota: En un sistema real, aquí se enviaría la recomendación de vuelta 
            # a una cola de respuesta que la WebApp estaría escuchando.
        else:
            print("⚠️ No se pudo encontrar una recomendación.")
            
        # Confirma que el mensaje fue procesado correctamente
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        print(f"❌ Error al procesar mensaje: {e}")
        # En caso de error, puedes optar por no hacer ack para que RabbitMQ reencole el mensaje
        # ch.basic_nack(delivery_tag=method.delivery_tag)
        ch.basic_ack(delivery_tag=method.delivery_tag) # Mantener ACK simple por ahora


def start_consumer():
    """Conecta a RabbitMQ y comienza a escuchar la cola de recomendaciones."""
    print(f"🎬 Worker Recomendador iniciado. Escuchando en '{QUEUE_NAME}'...")
    max_retries = 10
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)

    for attempt in range(max_retries):
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST, 
                    port=5672,
                    heartbeat=600,
                    credentials=credentials
                )
            )
            channel = connection.channel()
            # Declaramos la cola (debe ser la misma que usa la WebApp)
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            
            # Aseguramos que solo tome un mensaje a la vez
            channel.basic_qos(prefetch_count=1) 
            
            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
            
            print('✅ Esperando mensajes. Presiona CTRL+C para salir.')
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            print(f"⏳ RabbitMQ no disponible (intento {attempt + 1}/{max_retries}). Reintentando en {5} segundos...")
            time.sleep(5)
            if attempt == max_retries - 1:
                raise Exception("❌ No se pudo conectar a RabbitMQ después de múltiples intentos.")
        except KeyboardInterrupt:
            print("\nShutting down consumer...")
            break
        except Exception as e:
            print(f"Error inesperado en el consumidor: {e}. Reiniciando en 5 segundos...")
            time.sleep(5)
            
    # Si la conexión se estableció y se rompió, cerrar
    if 'connection' in locals() and connection.is_open:
        connection.close()


if __name__ == "__main__":
    # Intentamos la conexión a la DB una vez para asegurar que esté en línea
    try:
        conn = get_db_connection()
        conn.close()
        print("✅ Conexión inicial a DB exitosa. Iniciando consumidor.")
        start_consumer()
    except Exception as e:
        print(f"🚨 Error crítico de inicio: {e}")
        # El contenedor saldrá y Docker Compose lo reiniciará (restart: always)
        exit(1)