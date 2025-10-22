import pika, json, os, time
from pysentimiento import create_analyzer

# --- Configuración de RabbitMQ ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "user")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "password")

def connect_to_rabbitmq(retries=10, delay=5):
    """Intenta reconectarse a RabbitMQ si falla."""
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
            print("✅ Conectado correctamente a RabbitMQ.")
            return connection
        except Exception as e:
            print(f"❌ Error al conectar a RabbitMQ: {e}")
            time.sleep(delay)
    raise Exception("🚨 No se pudo conectar a RabbitMQ después de varios intentos.")

def main():
    # --- Conexión con RabbitMQ ---
    connection = connect_to_rabbitmq()
    channel = connection.channel()

    # --- Declaración de colas ---
    channel.queue_declare(queue='cola_estado_usuario', durable=True)
    channel.queue_declare(queue='cola_emocion_detectada', durable=True)

    # --- Cargamos el analizador de emociones ---
    print("🧠 Cargando modelo de análisis emocional (esto puede tardar unos segundos)...")
    analyzer = create_analyzer(task="emotion", lang="es")
    print("✅ Modelo cargado. Esperando mensajes en 'cola_estado_usuario'...")

    def callback(ch, method, properties, body):
        try:
            data = json.loads(body)
            texto = data.get("text", "")
            print(f"[📩] Texto recibido: {texto}")

            result = analyzer.predict(texto)
            emocion = result.output
            print(f"[💬] Emoción detectada: {emocion}")

            ch.basic_publish(
                exchange='',
                routing_key='cola_emocion_detectada',
                body=json.dumps({"emotion": emocion})
            )
            print(f"[📤] Emoción enviada a 'cola_emocion_detectada'")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"⚠️ Error procesando mensaje: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag)

    # --- Inicia la escucha de la cola ---
    channel.basic_consume(queue='cola_estado_usuario', on_message_callback=callback)
    channel.start_consuming()

if __name__ == "__main__":
    main()

