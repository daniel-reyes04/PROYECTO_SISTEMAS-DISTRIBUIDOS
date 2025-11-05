from flask import Flask, render_template, request, jsonify, session # Añadir 'session'
import pika, json, os, time
import uuid # Nuevo: para generar IDs únicos

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'una_clave_secreta_fuerte_y_aleatoria') 

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "user")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "password")

def publicar_emocion(texto, request_id): 
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue="cola_estado_usuario", durable=True)
    channel.basic_publish(exchange="", routing_key="cola_estado_usuario", body=json.dumps({"text": texto, "request_id": request_id})) 
    connection.close()
def obtener_recomendaciones(request_id, timeout=15): 
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue="cola_resultados_recomendacion", durable=True)

    start = time.time()
    while time.time() - start < timeout:
        # basic_get obtiene el primer mensaje de la cola
        method, props, body = channel.basic_get(queue="cola_resultados_recomendacion", auto_ack=False)
        
        if body:
            data = json.loads(body)
            
            # FILTRO CRÍTICO: Si el mensaje contiene el ID correcto, lo procesamos.
            if data.get('request_id') == request_id:
                channel.basic_ack(method.delivery_tag) # Reconoce el mensaje y lo elimina
                connection.close()
                return data
            else:
                # Si el ID NO coincide (es de otro usuario), lo reencolamos sin reconocerlo (basic_nack)
                # Esto es un workaround. Una solución ideal usaría Redis o correlation_id
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True) 
        
        time.sleep(1)
        
    connection.close()
    return None

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        texto = request.form["texto"]
        request_id = str(uuid.uuid4()) # [CORRECCIÓN 12] Genera el ID único
        session['request_id'] = request_id # Guarda el ID en la sesión
        
        publicar_emocion(texto, request_id) # Pasa el ID a la función
        return render_template("resultados.html", emocion=texto)
    return render_template("index.html")

@app.route("/api/recomendaciones")
def api_recomendaciones():
    # [CORRECCIÓN 13] Recupera el ID de la sesión y filtra la cola
    request_id = session.get('request_id') 
    
    if not request_id:
        return jsonify({"status": "error", "message": "Falta ID de solicitud, intente de nuevo."})
    
    data = obtener_recomendaciones(request_id)
    
    if data:
        return jsonify(data)
        
    return jsonify({"status": "pending", "message": "Aún esperando respuesta de los workers."})

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
