from flask import Flask, render_template, request, jsonify
import pika, json, os, time

app = Flask(__name__)

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "user")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "password")

def publicar_emocion(texto):
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue="cola_estado_usuario", durable=True)
    channel.basic_publish(exchange="", routing_key="cola_estado_usuario", body=json.dumps({"text": texto}))
    connection.close()

def obtener_recomendaciones(timeout=15):
    """Escucha la cola de resultados por unos segundos."""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue="cola_resultados_recomendacion", durable=True)

    start = time.time()
    while time.time() - start < timeout:
        method, props, body = channel.basic_get(queue="cola_resultados_recomendacion", auto_ack=True)
        if body:
            connection.close()
            return json.loads(body)
        time.sleep(1)
    connection.close()
    return None

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        texto = request.form["texto"]
        publicar_emocion(texto)
        return render_template("resultados.html", emocion=texto)
    return render_template("index.html")

@app.route("/api/recomendaciones")
def api_recomendaciones():
    data = obtener_recomendaciones()
    if data:
        return jsonify(data)
    return jsonify({"status": "pending", "message": "Esperando resultados de IA..."})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
