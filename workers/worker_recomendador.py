import os, json, pika, psycopg2
import numpy as np
from sentence_transformers import SentenceTransformer
import time # Importar para el reintento de conexión a DB

# --- Configuración (Igual que antes) ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "user")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "password")

DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("DB_NAME", "cinesense_ai_db")
DB_USER = os.getenv("DB_USER", "cinesense_user")
DB_PASS = os.getenv("DB_PASS", "cinesense_pass")

model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")

def conectar_db():
    while True:
        try:
            conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
            print("Conexión a la Base de Datos PostgreSQL establecida.")
            return conn
        except psycopg2.OperationalError:
            print("Esperando conexión con la DB...")
            time.sleep(3)

def cosine_similarity(a, b):
    a, b = np.array(a), np.array(b)
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))

def recomendar_por_semantica(emocion_texto):
    conn = conectar_db() 
    cur = conn.cursor()
    cur.execute("SELECT id, titulo, genero, sinopsis_original, embedding_sinopsis FROM Peliculas;")
    peliculas = cur.fetchall()
    conn.close()

    if not peliculas:
        return []

    user_vec = model.encode(emocion_texto)
    recomendaciones = []

    for id_p, titulo, genero, sinopsis, emb_str in peliculas:
        if not emb_str:
            continue
            
        emb_list = json.loads(emb_str) 
        emb_vec = np.array(emb_list)
        
        sim = cosine_similarity(user_vec, emb_vec)
        recomendaciones.append((titulo, genero, sim, sinopsis))

    recomendaciones.sort(key=lambda x: x[2], reverse=True)
    return recomendaciones[:5]

def main():

    while True:
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST, credentials=credentials,
                heartbeat=600, blocked_connection_timeout=300
            )
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            break
        except Exception as e:
            print(f"Error al conectar a RabbitMQ: {e}. Reintentando en 5 segundos...")
            time.sleep(5)
            
    channel.queue_declare(queue="cola_emocion_detectada", durable=True)
    channel.queue_declare(queue="cola_resultados_recomendacion", durable=True)

    print("[*] Esperando emociones para recomendación semántica...")

    def on_message(ch, method, properties, body):
        data = json.loads(body)
        emocion = data.get("emotion", "")
        request_id = data.get("request_id") 

        print(f"Emoción detectada: {emocion}")

        recomendaciones = recomendar_por_semantica(f"Películas para cuando me siento {emocion}")

        result_list = [
            {"titulo": t, "genero": g, "similitud": float(s), "sinopsis": s_desc[:150] + "..."}
            for t, g, s, s_desc in recomendaciones
        ]

        channel.basic_publish(
            exchange='',
            routing_key='cola_resultados_recomendacion',
            body=json.dumps({"emotion": emocion, "recomendaciones": result_list, "request_id": request_id}) 
        )

        print(f"{len(result_list)} recomendaciones enviadas a 'cola_resultados_recomendacion' (ID: {request_id})")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue="cola_emocion_detectada", on_message_callback=on_message)
    channel.start_consuming()

if __name__ == "__main__":
    main()