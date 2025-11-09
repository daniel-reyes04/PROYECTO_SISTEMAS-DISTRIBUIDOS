import json
import os
import pika
import time # Importar time
from flask import Flask, render_template_string, request, jsonify, url_for

app = Flask(__name__)

# --- Configuración de RabbitMQ ---
# Las variables de entorno son inyectadas por docker-compose desde el archivo .env
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_USER = os.getenv('RABBITMQ_DEFAULT_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_DEFAULT_PASS', 'guest')
QUEUE_NAME = "recommendation_queue" # Debe coincidir con el worker_recomendador.py

def get_rabbitmq_connection():
    """Intenta conectarse a RabbitMQ con credenciales y reintentos exponenciales."""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST, 
        port=5672, 
        credentials=credentials,
        heartbeat=600 # Aumentar el heartbeat para evitar desconexiones
    )
    max_retries = 10
    for i in range(max_retries):
        try:
            connection = pika.BlockingConnection(parameters)
            print("Web App: Conexión exitosa a RabbitMQ.")
            return connection
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Error de conexión a RabbitMQ en WebApp: {e}. Reintentando en {2**i} segundos...")
            # Usamos min(2 ** i, 30) para limitar el tiempo de espera máximo
            time.sleep(min(2 ** i, 30)) 
    
    # Si fallan todos los reintentos, lanzamos una excepción
    raise Exception("No se pudo conectar a RabbitMQ después de múltiples intentos.")


@app.route('/')
def index():
    """Sirve la página principal de la aplicación web."""
    # HTML simple con Tailwind CSS para una interfaz de usuario moderna
    html_template = """
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <title>CineSense AI</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <script src="https://cdn.tailwindcss.com"></script>
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
            body { font-family: 'Inter', sans-serif; background-color: #f7f7f7; }
        </style>
    </head>
    <body class="bg-gray-50 min-h-screen flex items-center justify-center p-4">
        <div class="w-full max-w-xl bg-white shadow-xl rounded-2xl p-8 space-y-6">
            <h1 class="text-3xl font-bold text-center text-indigo-700">
                🎬 CineSense AI
            </h1>
            <p class="text-center text-gray-600">
                ¿Qué tipo de película te apetece ver hoy? Describe tus preferencias y te enviaremos una recomendación.
            </p>

            <!-- Formulario de Consulta -->
            <div id="query-form" class="space-y-4">
                <textarea 
                    id="user-query" 
                    rows="4" 
                    placeholder="Ej: 'Quiero una película de ciencia ficción con viajes en el tiempo y un final sorprendente, como Interstellar.'"
                    class="w-full p-4 border border-gray-300 rounded-lg shadow-sm focus:ring-indigo-500 focus:border-indigo-500 transition duration-150 ease-in-out resize-none"
                ></textarea>
                <button 
                    id="submit-button" 
                    onclick="sendQuery()" 
                    class="w-full bg-indigo-600 hover:bg-indigo-700 text-white font-semibold py-3 px-6 rounded-lg shadow-md transition duration-200 ease-in-out transform hover:scale-[1.01] focus:outline-none focus:ring-4 focus:ring-indigo-500 focus:ring-opacity-50"
                >
                    Enviar Consulta
                </button>
            </div>

            <!-- Área de Mensajes -->
            <div id="message-area" class="mt-6">
                <div id="loading-indicator" class="hidden text-center text-indigo-600 font-medium">
                    Procesando tu solicitud... <span class="animate-spin inline-block">🌀</span>
                </div>
                <div id="status-message" class="text-center text-sm font-medium p-3 rounded-lg hidden">
                    <!-- Los mensajes de estado aparecerán aquí -->
                </div>
            </div>

        </div>

        <script>
            // Función auxiliar para mostrar mensajes
            function showMessage(text, type = 'info') {
                const msgElement = document.getElementById('status-message');
                msgElement.textContent = text;
                msgElement.classList.remove('hidden', 'bg-red-100', 'text-red-800', 'bg-green-100', 'text-green-800', 'bg-blue-100', 'text-blue-800');
                
                if (type === 'success') {
                    msgElement.classList.add('bg-green-100', 'text-green-800');
                } else if (type === 'error') {
                    msgElement.classList.add('bg-red-100', 'text-red-800');
                } else { // info/default
                    msgElement.classList.add('bg-blue-100', 'text-blue-800');
                }
            }

            // Función principal para enviar la consulta
            async function sendQuery() {
                const query = document.getElementById('user-query').value.trim();
                const submitButton = document.getElementById('submit-button');
                const loadingIndicator = document.getElementById('loading-indicator');
                
                if (!query) {
                    showMessage('Por favor, ingresa tu consulta antes de enviar.', 'error');
                    return;
                }

                submitButton.disabled = true;
                submitButton.textContent = 'Enviando...';
                loadingIndicator.classList.remove('hidden');
                document.getElementById('status-message').classList.add('hidden'); // Ocultar mensaje anterior

                try {
                    const response = await fetch('/send_query', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify({ query: query })
                    });

                    const data = await response.json();

                    loadingIndicator.classList.add('hidden');
                    submitButton.disabled = false;
                    submitButton.textContent = 'Enviar Consulta';

                    if (response.ok) {
                        showMessage('✅ ' + data.message + '. El worker está procesando tu recomendación.', 'success');
                        document.getElementById('user-query').value = ''; // Limpiar campo
                    } else {
                        showMessage('❌ Error al enviar consulta: ' + data.message, 'error');
                    }

                } catch (error) {
                    console.error('Fetch error:', error);
                    loadingIndicator.classList.add('hidden');
                    submitButton.disabled = false;
                    submitButton.textContent = 'Enviar Consulta';
                    showMessage('❌ Error de conexión con el servidor. Por favor, revisa la consola.', 'error');
                }
            }
        </script>
    </body>
    </html>
    """
    return render_template_string(html_template)

@app.route('/send_query', methods=['POST'])
def send_query():
    """Recibe la consulta y la envía a RabbitMQ."""
    data = request.get_json()
    user_query = data.get('query')
    
    if not user_query:
        return jsonify({"status": "error", "message": "No se proporcionó consulta"}), 400

    connection = None
    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        # Aseguramos que la cola exista antes de enviar
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        payload = json.dumps({"query": user_query})
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME,
            body=payload,
            properties=pika.BasicProperties(
                delivery_mode=2, # Hace el mensaje persistente
            )
        )
        print(f"Web App: Mensaje enviado a RabbitMQ: '{user_query}'")
        return jsonify({"status": "success", "message": "Consulta enviada"}), 200

    except Exception as e:
        print(f"Error en la webapp al enviar a RabbitMQ: {e}")
        return jsonify({"status": "error", "message": f"Error interno: {e}"}), 500
    finally:
        # Aseguramos que la conexión se cierre después de enviar el mensaje
        if connection and connection.is_open:
            connection.close()
            print("Web App: Conexión de RabbitMQ cerrada.")

if __name__ == '__main__':
    # La webapp debe esperar a que RabbitMQ esté disponible antes de iniciar
    # Intentamos obtener la conexión en el inicio para verificar la disponibilidad
    try:
        connection = get_rabbitmq_connection()
        connection.close() # Solo verificamos la conexión, la cerramos inmediatamente
        app.run(host='0.0.0.0', port=5000)
    except Exception as e:
        print(f"La aplicación web no pudo iniciarse debido a un error de conexión con RabbitMQ: {e}")
        exit(1) # Salir si no se puede conectar al inicio