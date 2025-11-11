import pika, json, os, time
from pysentimiento import create_analyzer
from pika.exceptions import AMQPConnectionError

# --- Configuración de RabbitMQ ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "guest") # Usamos 'guest' como default, consistente con docker-compose
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "guest") # Usamos 'guest' como default
QUEUE_NAME = 'cola_estado_usuario' # Cola de entrada (desde app.py)
NEXT_QUEUE_NAME = 'cola_emocion_detectada' # Cola de salida (hacia worker_recomendador.py)

# --- Inicialización del Modelo Global ---
analyzer = None 

def connect_to_rabbitmq(retries=10, delay=5):
    """Establece una conexión y un canal, declarando la cola si no existe. Retorna (connection, channel)."""
    for i in range(retries):
        try:
            print(f"🔌 Intentando conectar a RabbitMQ ({i+1}/{retries}) en {RABBITMQ_HOST}...")
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            # Declarar las colas de entrada y salida
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            channel.queue_declare(queue=NEXT_QUEUE_NAME, durable=True)
            print("✅ Conectado correctamente a RabbitMQ. Colas declaradas.")
            return connection, channel
        except AMQPConnectionError as e:
            print(f"❌ Error al conectar a RabbitMQ: {e}")
            time.sleep(delay)
    raise Exception("🚨 No se pudo conectar a RabbitMQ después de varios intentos.")


# --- Lógica de Consumo ---
def callback(ch, method, properties, body):
    """Función de callback al recibir un mensaje de la cola_estado_usuario."""
    global analyzer
    if analyzer is None:
        print("⚠️ Modelo no cargado. Reenviando mensaje.")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    try:
        data = json.loads(body)
        texto = data.get("query", "") # Renombrado a 'query' por consistencia con app.py
        request_id = data.get("request_id") # OBTENER el ID DE SOLICITUD
        
        if not texto or not request_id:
            print(f"⚠️ Mensaje inválido recibido: {data}. Descartando.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        print(f"[📩] Texto recibido: {texto} (ID: {request_id})")

        # 1. Análisis de Emoción
        result = analyzer.predict(texto)
        emocion = result.output
        print(f"[💬] Emoción detectada: {emocion}")

        # 2. Publicar a la siguiente cola
        # Se envía: Emoción, Request ID, y la consulta original (texto)
        ch.basic_publish(
            exchange='',
            routing_key=NEXT_QUEUE_NAME,
            body=json.dumps({
                "emotion": emocion, 
                "request_id": request_id,
                "query": texto # <- CRUCIAL: Pasamos el texto/query original
            }), 
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print(f"[📤] Emoción y consulta enviada a '{NEXT_QUEUE_NAME}'")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except json.JSONDecodeError:
        print("⚠️ Error de decodificación JSON. Descartando mensaje.")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"⚠️ Error procesando mensaje: {e}")
        # En caso de error, NACK y re-encolar
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True) 


def main():
    global analyzer
    
    # 1. Inicialización del modelo (Hacer esto una vez)
    print("\n--- Iniciando Worker Analizador de emociones ---")
    print("🧠 Cargando modelo de análisis emocional (esto puede tardar unos segundos)...")
    analyzer = create_analyzer(task="emotion", lang="es")
    print("✅ Modelo cargado.")

    # 2. Conexión y Consumo
    connection = None
    try:
        # Llamamos a la función y obtenemos CONEXIÓN Y CANAL
        connection, channel = connect_to_rabbitmq() 
        
        # Configuramos la política de calidad de servicio (QoS) para 1 mensaje a la vez
        channel.basic_qos(prefetch_count=1) 
        
        # Configuramos el consumidor
        channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=False)
        
        print(f'✅ Worker Emoción listo. Esperando mensajes en la cola: {QUEUE_NAME}.')
        channel.start_consuming()

    except Exception as e:
        print(f"🚨 Error crítico en el worker de emoción: {e}")
    finally:
        if connection and connection.is_open:
            print("🔌 Cerrando conexión RabbitMQ...")
            connection.close()


if __name__ == "__main__":
    main()