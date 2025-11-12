import pika
import os
import time
import psycopg2
import json
import requests
from psycopg2 import sql
from pika.exceptions import AMQPConnectionError, ConnectionClosedByBroker

# --- IMPORTACIONES DE IA ---
# Importamos SentenceTransformer
try:
    from sentence_transformers import SentenceTransformer
except ImportError:
    print("🚨 ERROR: No se encontró la librería 'sentence_transformers'. Usando clase dummy.")
    class SentenceTransformer:
        def __init__(self, *args, **kwargs): pass 
        def encode(self, *args, **kwargs): return [0.0] * 384
        def get_sentence_embedding_dimension(self): return 384

# Importamos la librería de Gemini
try:
    from google import genai
    from google.genai.errors import APIError 
except ImportError:
    print("🚨 ERROR: No se encontró la librería 'google-genai'. La personalización de sinopsis estará deshabilitada.")
    # Clases dummy para evitar errores
    class genai:
        class Client:
            def __init__(self, **kwargs): pass
            class models:
                def generate_content(self, **kwargs):
                    raise NotImplementedError("Gemini client not initialized.")
    class APIError(Exception): pass
# -----------------------------

MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
try:
    sbert_model = SentenceTransformer(MODEL_NAME)
    EMBEDDING_DIM = sbert_model.get_sentence_embedding_dimension()
    print(f"✅ Modelo S-BERT cargado. Dimensión: {EMBEDDING_DIM}")
except Exception as e:
    print(f"⚠️ Advertencia: No se pudo cargar el modelo SBERT. Usando dummy: {e}")
    sbert_model = SentenceTransformer()
    EMBEDDING_DIM = sbert_model.get_sentence_embedding_dimension()


# ---------------- CONFIGURACIONES GLOBALES ---------------- #
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_USER = os.getenv('RABBITMQ_DEFAULT_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_DEFAULT_PASS', 'guest')

# Colas
QUEUE_NAME_IN = 'cola_emocion_detectada' 
QUEUE_NAME_OUT = 'cola_resultados_finales'

# Configuración de la base de datos
DB_HOST = os.getenv('DB_HOST', 'db')
DB_NAME = os.getenv('DB_NAME', 'cinesense_ai_db')
DB_USER = os.getenv('DB_USER', 'cinesense_user')
DB_PASS = os.getenv('DB_PASS', 'password')

# Configuración de Gemini
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
GEMINI_MODEL = 'gemini-2.5-flash' # Modelo rápido para esta tarea
GEMINI_CLIENT = None
# --------------------------------------------------------- #


# --- INICIALIZACIÓN DE GEMINI ---
def init_gemini_client():
    """Inicializa el cliente de Gemini si la API key está disponible."""
    global GEMINI_CLIENT
    if GEMINI_API_KEY:
        try:
            # Intentar inicializar el cliente solo si la librería fue importada
            if 'genai' in globals():
                GEMINI_CLIENT = genai.Client(api_key=GEMINI_API_KEY)
                print("✅ Cliente de Gemini inicializado.")
                return True
        except Exception as e:
            print(f"🚨 Error inicializando cliente Gemini: {e}")
            GEMINI_CLIENT = None
    else:
        print("🚨 ERROR: GEMINI_API_KEY no configurada. La reescritura de sinopsis estará deshabilitada.")
    return False

# --- FUNCIÓN DE REESCRITURA CON GEMINI ---
# worker_recomendador.py

def rewrite_synopsis_with_gemini(synopsis: str, emotion: str) -> str:
    """
    Usa la API de Gemini para reescribir la sinopsis, ajustando el tono a la emoción.
    La lógica se amplía para manejar más estados de ánimo con instrucciones específicas.
    """
    if not GEMINI_CLIENT:
        return synopsis # Retorna el original si el cliente no está disponible
    
    # 1. Lógica de mapeo de emoción a instrucción específica
    emotion_lower = emotion.lower()
    tone_instruction = ""

    if any(keyword in emotion_lower for keyword in ["tristeza", "miedo", "melancolía", "duelo", "decepcionado"]):
        # 😢 Emociones Tristes o de Miedo
        tone_instruction = "enfoca el resumen en la superación, la esperanza, el consuelo o la catarsis."
    elif any(keyword in emotion_lower for keyword in ["alegría", "diversión", "felicidad", "entusiasmo", "feliz"]):
        # 😄 Emociones Alegres
        tone_instruction = "resalta el humor, la aventura, la ligereza y la energía positiva."
    elif any(keyword in emotion_lower for keyword in ["enojo", "ira", "asco", "frustración", "rabia", "molesto"]):
        # 😠 Emociones de Enojo o Negativas Intensas
        tone_instruction = "enfoca el resumen en la acción, la justicia, la liberación de tensión o la comedia oscura."
    elif any(keyword in emotion_lower for keyword in ["calma", "relajación", "paz", "tranquilidad", "serenidad", "neutro"]):
        # 😌 Emociones de Calma o Reflexivas
        tone_instruction = "enfoca el resumen en la contemplación, la belleza visual, el drama reflexivo o la intriga intelectual."
    else:
        # 🤷 Por defecto, para emociones no mapeadas (ej. 'sorpresa', 'nostalgia')
        tone_instruction = "mantén un tono atractivo y enfocado en el misterio, la intriga y el entretenimiento general."


    # 2. Prompt para personalizar la sinopsis
    prompt = f"""
    Eres un experto en marketing de películas. Reescribe la siguiente sinopsis para hacerla 
    extremadamente atractiva y relevante para un usuario que se siente **{emotion}**.

    Mantente fiel al argumento central. Ajusta el tono para apelar a la emoción del usuario:
    - {tone_instruction}

    El resultado debe ser **solo la sinopsis reescrita**, sin encabezados, comillas ni explicaciones adicionales. Máximo 3 frases.

    Sinopsis Original: "{synopsis}"
    Emoción del Usuario: "{emotion}"
    """

    try:
        response = GEMINI_CLIENT.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt
        )
        new_synopsis = response.text.strip().replace('"', '')
        return new_synopsis
    except APIError as e:
        print(f"⚠️ Error en la llamada a la API de Gemini. Fallback a sinopsis original: {e}")
        return synopsis
    except Exception as e:
        print(f"⚠️ Error inesperado al usar Gemini. Fallback: {e}")
        return synopsis

# ---------------- CONEXIÓN DB ---------------- #
def get_db_connection():
    """Intenta conectarse a PostgreSQL con reintentos."""
    max_retries = 10
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASS
            )
            return conn
        except psycopg2.OperationalError as e:
            print(f"⏳ DB no disponible (intento {i + 1}/{max_retries}). Reintentando en 5s...")
            time.sleep(5)
    raise Exception("🚨 No se pudo conectar a la base de datos después de múltiples intentos.")

# ---------------- CONEXIÓN RABBITMQ ---------------- #
def get_rabbitmq_connection():
    """Intenta conectarse a RabbitMQ con reintentos."""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST, 
        port=5672, 
        credentials=credentials,
        heartbeat=600
    )
    return pika.BlockingConnection(parameters)

# ---------------- LÓGICA DE RECOMENDACIÓN ---------------- #

def get_recommendations_from_db(conn, emotion, limit=3):
    """
    Busca las 'limit' películas más cercanas a la emoción/sentimiento dado.
    (Tu lógica de S-BERT y pgvector)
    """
    global sbert_model, EMBEDDING_DIM
    
    query_text = f"Una película que me haga sentir {emotion.lower()}"
    
    if sbert_model:
        emotion_embedding = sbert_model.encode(query_text).tolist()
    else:
        emotion_embedding = [0.0] * EMBEDDING_DIM

    embedding_str = '[' + ','.join(map(str, emotion_embedding)) + ']'
    
    # print(f"[🔍] Buscando películas similares a: '{query_text}' (vector dim: {EMBEDDING_DIM})")
    
    try:
        with conn.cursor() as cursor:
            # Consulta de similitud vectorial (coseno)
            query = sql.SQL("""
                SELECT
                    titulo,
                    resumen,
                    rating_imdb,
                    1 - (embedding <=> %s) AS score
                FROM
                    Peliculas
                WHERE 
                    embedding IS NOT NULL
                ORDER BY
                    embedding <=> %s
                LIMIT %s;
            """)
            
            cursor.execute(query, [embedding_str, embedding_str, limit])
            
            results = cursor.fetchall()
            recommendations = []
            
            if not results:
                 return [] 

            for titulo, resumen, rating, score in results:
                recommendations.append({
                    "titulo": titulo,
                    "resumen": resumen,
                    "rating_imdb": float(rating),
                    "score": float(score),
                    "emocion_buscada": emotion
                })
            return recommendations

    except psycopg2.Error as e:
        print(f"🚨 ERROR de DB al obtener recomendaciones: {e}")
        return []
    except Exception as e:
        print(f"🚨 Error inesperado en la lógica de recomendación: {e}")
        return []


# ---------------- CONSUMIDOR DE RABBITMQ ---------------- #
def callback(ch, method, properties, body):
    conn = None
    try:
        data = json.loads(body)
        # CORRECCIÓN: El worker_emotion.py envía 'emotion'
        emotion = data.get("emotion") 
        request_id = data.get("request_id")
        
        print(f"\n[📩] Mensaje recibido de '{QUEUE_NAME_IN}': Emoción '{emotion}' (ID: {request_id})")

        if not emotion or not request_id:
            print(f"⚠️ Mensaje inválido. Faltan datos. Body: {data}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # 1. Lógica de Recomendación (DB)
        conn = get_db_connection()
        recommendations = get_recommendations_from_db(conn, emotion, limit=5)
        
        # 2. PERSONALIZACIÓN DE LA SINOPSIS CON GEMINI (Paso Clave)
        if recommendations:
            for movie in recommendations:
                original_synopsis = movie.get('resumen')
                
                # Eliminamos la clave 'resumen' original de la DB (ya no es necesaria)
                if 'resumen' in movie:
                    del movie['resumen']
                    
                if original_synopsis:
                    # ✅ CORRECCIÓN 1: Usar 'sinopsis' (esperado por el frontend)
                    movie['sinopsis'] = rewrite_synopsis_with_gemini(original_synopsis, emotion)
                    
                    # ✅ CORRECCIÓN 2: Usar 'emocion_usada' (esperado por el frontend)
                    movie['emocion_usada'] = emotion
                else:
                    # Asegurarse de que al menos la clave exista si no hubo sinopsis original
                    movie['sinopsis'] = "Sin sinopsis original disponible para personalizar."
                    movie['emocion_usada'] = emotion
        
        # 3. Envío del resultado final
        final_payload = json.dumps({
            "request_id": request_id,
            "recommendations": recommendations
        })
        
        ch.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME_OUT,
            body=final_payload,
            properties=pika.BasicProperties(
                delivery_mode=2,
            )
        )
        
        print(f"[📤] Recomendaciones (personalizadas) enviadas a '{QUEUE_NAME_OUT}' para ID: {request_id}")
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except json.JSONDecodeError:
        print(f"⚠️ Error de JSON Decode. Cuerpo del mensaje: {body}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        print(f"🚨 ERROR CRÍTICO en callback del Recomendador: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True) 
    finally:
        if conn:
            conn.close()
            
# ---------------- MAIN ---------------- #
if __name__ == "__main__":
    print("\n--- Iniciando Worker Recomendador ---")
    
    # 1. Inicializar cliente Gemini
    init_gemini_client()
    
    # 2. Verificar/Esperar DB
    try:
        conn = get_db_connection()
        conn.close()
        print("✅ Conexión inicial a DB exitosa.")
    except Exception as e:
        print(f"🚨 ERROR FATAL: La DB no está disponible al iniciar. {e}")
        exit(1)

    # 3. Conectar a RabbitMQ e iniciar consumidor
    max_retries = 10
    attempt = 0
    while attempt < max_retries:
        try:
            connection = get_rabbitmq_connection()
            channel = connection.channel()
            
            channel.queue_declare(queue=QUEUE_NAME_IN, durable=True)
            channel.queue_declare(queue=QUEUE_NAME_OUT, durable=True)

            channel.basic_qos(prefetch_count=1) 
            channel.basic_consume(queue=QUEUE_NAME_IN, on_message_callback=callback, auto_ack=False) 
            print(f'✅ Worker Recomendador listo. Esperando mensajes en la cola: {QUEUE_NAME_IN}.')
            channel.start_consuming()

        except (AMQPConnectionError, ConnectionClosedByBroker) as e:
            print(f"❌ Conexión RabbitMQ fallida. Reiniciando conexión en 5 segundos... ({e})")
            time.sleep(5)
            attempt += 1
        except KeyboardInterrupt:
            print("\nShutting down consumer...")
            break
        except Exception as e:
            print(f"Error inesperado en el consumidor: {e}. Reiniciando en 5 segundos...")
            time.sleep(5)
            attempt += 1
            
    if 'connection' in locals() and connection.is_open:
        connection.close()
        print("🔌 Conexión RabbitMQ cerrada.")
    
    if attempt >= max_retries:
        print("🚨 No se pudo conectar a RabbitMQ después de múltiples intentos. Saliendo.")