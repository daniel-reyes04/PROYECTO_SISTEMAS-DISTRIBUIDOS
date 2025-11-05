import pika
import os
import time
import json
import psycopg2
import numpy as np
import requests # Necesario para la API de Gemini
from sentence_transformers import SentenceTransformer
from decimal import Decimal

# --- CONFIGURACIÓN ---
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'user')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS', 'password')

QUEUE_CONSULTA = 'cola_consulta_usuario' 
QUEUE_RESULTADO = 'cola_resultado_recomendacion'

DB_HOST = os.getenv('DB_HOST', 'db')
DB_NAME = os.getenv('DB_NAME', 'cinesense_ai_db')
DB_USER = os.getenv('DB_USER', 'cinesense_user')
DB_PASS = os.getenv('DB_PASS', 'password')

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY') 
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent"

MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
model = SentenceTransformer(MODEL_NAME)


def get_db_connection():
    """Establece la conexión a la base de datos con reintentos."""
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASS
            )
            return conn
        except psycopg2.OperationalError:
            time.sleep(3)

def cosine_similarity(a, b):
    """Calcula la similitud coseno entre dos vectores NumPy."""
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))

def get_persuasive_sinopsis(sinopsis_original, emocion_detectada, afinidad):
    """
    Llama a la API de Gemini para reescribir la sinopsis.
    Implementa un reintento simple por si falla la red.
    """
    if not GEMINI_API_KEY:
        return (f"🚨 ERROR: GEMINI_API_KEY no configurada. (Original: {sinopsis_original[:100]}...)", 0)
    system_prompt = (
        "Actúa como un crítico de cine persuasivo y un experto en marketing emocional. "
        "Tu tarea es reescribir una sinopsis de película para hacerla irresistible para alguien que busca esa emoción específica. "
        "Sé conciso, usa un tono excitante y enfocado en la emoción. NO añadas títulos ni emojis."
    )

    user_query = (
        f"Reescribe la siguiente sinopsis para persuadir a un usuario que busca películas de '{emocion_detectada}'. "
        f"La película tiene un {afinidad}% de afinidad con su consulta. "
        f"Sinopsis Original: \"{sinopsis_original}\""
    )

    payload = {
        "contents": [{ "parts": [{ "text": user_query }] }],
        "systemInstruction": { "parts": [{ "text": system_prompt }] },
        "config": { "responseMimeType": "text/plain" } 
    }

    for i in range(3):
        try:
            response = requests.post(
                f"{GEMINI_API_URL}?key={GEMINI_API_KEY}",
                headers={'Content-Type': 'application/json'},
                data=json.dumps(payload),
                timeout=15  
            )
            response.raise_for_status() 
            result = response.json()
            generated_text = result['candidates'][0]['content']['parts'][0]['text']
            return generated_text.strip()

        except requests.exceptions.RequestException as e:
            print(f"  [⚠️] Error en la llamada a Gemini (Intento {i+1}/3): {e}")
            time.sleep(2 ** i) 
        except Exception as e:
            print(f"  [⚠️] Error al procesar respuesta de Gemini: {e}")
            break

    return f"¡Perfecta para tu estado de {emocion_detectada}! El sistema de IA tuvo problemas, pero esta película es la más similar. Afinidad: {afinidad}%."


def recomendar_por_semantica(conn, user_query):
    """
    Realiza la búsqueda semántica RAG y genera contenido persuasivo con Gemini.
    """
    cur = conn.cursor()
    cur.execute("SELECT id, titulo, genero, sinopsis_original, embedding_sinopsis, rating_imdb FROM Peliculas WHERE embedding_sinopsis IS NOT NULL;")
    peliculas = cur.fetchall()
    cur.close()

    if not peliculas:
        return []

    user_vec = model.encode(user_query)
    recomendaciones = []

    for id_p, titulo, genero, sinopsis_original, emb_str, rating_imdb in peliculas:
        
        try:
            emb_list = json.loads(emb_str)  
            emb_vec = np.array(emb_list)
        except Exception:
            continue

        sim = cosine_similarity(user_vec, emb_vec)

        afinidad = int(sim * 100) 
        
        # 🚨 ¡INTEGRACIÓN GEMINI AQUÍ! 🚨
        # ----------------------------------------------------------------------
        print(f"    [💬] Llamando a Gemini para reescribir sinopsis de '{titulo}'...")
        sinopsis_persuasiva = get_persuasive_sinopsis(
            sinopsis_original=sinopsis_original,
            emocion_detectada=user_query, # Usamos la consulta del usuario como "emoción"
            afinidad=afinidad
        )
        # ----------------------------------------------------------------------

        recomendaciones.append({
            "id": id_p,
            "titulo": titulo,
            "genero": genero,
            "similitud": float(sim),
            "afinidad": afinidad,
            "sinopsis_original": sinopsis_original,
            "sinopsis_personalizada": sinopsis_persuasiva,
            "rating": float(rating_imdb)
        })

    recomendaciones.sort(key=lambda x: x['similitud'], reverse=True)
    return recomendaciones[:5]


def publish_recommendation_result(channel, result_data):
    """Publica el resultado final en la cola de resultados para que la web lo muestre."""
    message = json.dumps(result_data)
    channel.basic_publish(
        exchange='',
        routing_key=QUEUE_RESULTADO, 
        body=message,
        properties=pika.BasicProperties(delivery_mode=pika.spec.DeliveryMode.Persistent)
    )


def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        user_query = data.get("query", "")
        request_id = data.get("request_id")

        if not user_query:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        conn = get_db_connection()
        try:
            print(f"\n[🧠] Recibida consulta '{user_query}' (ID: {request_id}).")
            recommendations = recomendar_por_semantica(conn, user_query)
            
            result_data = {
                "status": "SUCCESS", 
                "query": user_query,
                "request_id": request_id, 
                "recommendations": recommendations
            }
            
            publish_recommendation_result(channel=ch, result_data=result_data)
            print(f"  [✅] {len(recommendations)} recomendaciones enviadas para ID: {request_id}")

        finally:
            conn.close()

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"  [🚨] ERROR FATAL en el procesamiento de la consulta: {e}")
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)


def start_worker():
    """Inicia la conexión a RabbitMQ y el consumo."""
    while True:
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST, 
                credentials=credentials,
                heartbeat=600, 
                blocked_connection_timeout=300
            )
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            channel.queue_declare(queue=QUEUE_CONSULTA, durable=True)
            channel.queue_declare(queue=QUEUE_RESULTADO, durable=True)
            
            print(f' [*] Agente de Recomendación esperando consultas en {QUEUE_CONSULTA}.')

            channel.basic_consume(queue=QUEUE_CONSULTA, on_message_callback=callback)
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError:
            print(" Esperando conexión con RabbitMQ...")
            time.sleep(5)
        except KeyboardInterrupt:
            print(' Desconectado.')
            break
        except Exception as e:
            print(f" Error inesperado en el Worker: {e}")
            time.sleep(5)

if __name__ == "__main__":
    start_worker()