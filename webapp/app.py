import json
import os
import pika
import time 
from flask import Flask, render_template_string, request, jsonify, url_for
import uuid 
import threading 
from pika.exceptions import AMQPConnectionError

app = Flask(__name__)

# --- Configuración de Colas ---
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_USER = os.getenv('RABBITMQ_DEFAULT_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_DEFAULT_PASS', 'guest')

# Colas
QUEUE_NAME_EMOTION = "cola_estado_usuario" # <-- Worker Emocion (Entrada)
QUEUE_NAME_RESULTS = "cola_resultados_finales" # <-- Para que el Worker Recomendador devuelva el resultado (Salida)

# Cache en memoria para almacenar las respuestas finales.
# {request_id: recomendaciones_json}
RESULTS_CACHE = {} 

# Variables globales para la conexión persistente de publicación
RABBITMQ_CONNECTION_PUBLISH = None
RABBITMQ_CHANNEL_PUBLISH = None
# -----------------------------

def get_rabbitmq_connection_and_channel(queue_name):
    """
    Establece una conexión y un canal, declarando la cola si no existe.
    Retorna (connection, channel).
    """
    max_retries = 10
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST, 
        port=5672, 
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300
    )

    for i in range(max_retries):
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            # Declarar la cola con durabilidad
            channel.queue_declare(queue=queue_name, durable=True) 
            print(f"Web App: Conexión exitosa a RabbitMQ. Cola '{queue_name}' declarada.")
            return connection, channel
        except AMQPConnectionError as e:
            print(f"Error de conexión a RabbitMQ en WebApp: {e}. Reintentando en {min(2 ** i, 30)} segundos...")
            time.sleep(min(2 ** i, 30)) 
    
    raise Exception("No se pudo conectar a RabbitMQ después de varios intentos.")


# --- CONSUMIDOR ASÍNCRONO DE RESULTADOS ---
def start_result_consumer():
    """Se ejecuta en un hilo separado para escuchar los resultados finales."""
    connection = None
    try:
        # Usamos la cola de resultados para la conexión del consumidor
        connection, channel = get_rabbitmq_connection_and_channel(QUEUE_NAME_RESULTS)
        
        # Configurar el consumidor
        channel.basic_qos(prefetch_count=1) # Procesa un mensaje a la vez

        def result_callback(ch, method, properties, body):
            """Función que se llama al recibir el resultado final."""
            try:
                data = json.loads(body)
                request_id = data.get("request_id")
                recommendations = data.get("recommendations") # Obtener la lista de recomendaciones
                
                if request_id and recommendations is not None:
                    # Almacenar el resultado en la cache
                    RESULTS_CACHE[request_id] = recommendations
                    print(f"[⬅️ CONSUMER] Resultado final para ID: {request_id} almacenado en cache.")
                else:
                    print(f"⚠️ [CONSUMER] Mensaje de resultado inválido: {data}")

                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                print(f"🚨 [CONSUMER] Error procesando mensaje de resultado: {e}")
                # En caso de error, NACK y re-encolar
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True) 

        channel.basic_consume(queue=QUEUE_NAME_RESULTS, on_message_callback=result_callback, auto_ack=False)
        print(f"✅ Hilo Consumidor de Resultados listo. Esperando mensajes en: {QUEUE_NAME_RESULTS}")
        channel.start_consuming()

    except Exception as e:
        print(f"🚨 Error crítico en el Hilo Consumidor de Resultados: {e}")
    finally:
        if connection and connection.is_open:
            print("🔌 Hilo Consumidor: Conexión de RabbitMQ cerrada.")
            connection.close()
        
# ---------------------------------------------


@app.route('/')
def index():
    """Ruta principal para la interfaz de usuario."""
    # He incluido el código HTML con el botón de voz y la lógica JS
    html_content = """
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>CineSense AI</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
            body { font-family: 'Inter', sans-serif; background-color: #f7f9fb; }
            .card { background-color: white; border: 1px solid #e0e7ff; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -2px rgba(0, 0, 0, 0.06); }
        </style>
    </head>
    <body class="flex items-center justify-center min-h-screen p-4">
        <div class="card w-full max-w-lg p-6 rounded-xl">
            <h1 class="text-3xl font-bold text-center text-indigo-700 mb-6">🎬 CineSense AI</h1>
            <p class="text-center text-gray-600 mb-8">Dime cómo te sientes (ej. "Hoy estoy muy feliz y quiero ver algo divertido") y te recomendaré una película.</p>

            <div class="mb-6">
                <textarea id="queryInput" rows="3" class="w-full p-3 border border-gray-300 rounded-lg focus:ring-indigo-500 focus:border-indigo-500 transition duration-150 ease-in-out resize-none" placeholder="Escribe tu estado de ánimo o lo que buscas..."></textarea>
            </div>
            
            <button id="sendButton" class="w-full bg-indigo-600 text-white font-semibold py-3 rounded-lg hover:bg-indigo-700 transition duration-200 ease-in-out shadow-md">
                Buscar Recomendación (Texto)
            </button>

            <button id="voiceButton" class="w-full bg-green-600 text-white font-semibold py-3 rounded-lg hover:bg-green-700 transition duration-200 ease-in-out shadow-md mt-3">
                🎙️ Iniciar Búsqueda por Voz
            </button>

            <div id="loading" class="mt-6 hidden text-center">
                <div class="flex items-center justify-center space-x-2">
                    <svg class="animate-spin h-5 w-5 text-indigo-600" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                        <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                        <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                    </svg>
                    <span class="text-indigo-600 font-medium">Analizando tu emoción y buscando...</span>
                </div>
            </div>

            <div id="resultsContainer" class="mt-8">
                <h2 class="text-xl font-semibold text-gray-700 mb-4 border-b pb-2 hidden">Recomendaciones</h2>
            </div>
            
            <div id="statusMessage" class="mt-4 text-center text-sm text-red-500 hidden"></div>

        </div>

        <script>
    const sendButton = document.getElementById('sendButton');
    const voiceButton = document.getElementById('voiceButton'); 
    const queryInput = document.getElementById('queryInput');
    const loadingIndicator = document.getElementById('loading');
    const resultsContainer = document.getElementById('resultsContainer');
    const statusMessage = document.getElementById('statusMessage');
    let pollingInterval = null;

    // --- CONSTANTES DE TIMEOUT (Aseguran que no se quede cargando infinitamente) ---
    const POLLING_INTERVAL = 3000; // 3 segundos
    const MAX_POLLING_TIME = 60000; // 60 segundos de espera máxima
    const MAX_ATTEMPTS = Math.floor(MAX_POLLING_TIME / POLLING_INTERVAL);
    let pollingAttempts = 0;
    // ---------------------------------------------------------------------------------

    // --- Lógica Reutilizable de Envío y Polling ---
    async function sendRecommendation(query) {
        if (!query) {
            displayStatus('Por favor, ingresa o dicta tu estado de ánimo.');
            resetState();
            return;
        }
        
        // 1. Iniciar el proceso y mostrar indicador de carga
        displayStatus('');
        resultsContainer.innerHTML = '<h2 class="text-xl font-semibold text-gray-700 mb-4 border-b pb-2 hidden">Recomendaciones</h2>';
        loadingIndicator.classList.remove('hidden');
        sendButton.disabled = true;
        voiceButton.disabled = true;
        sendButton.textContent = 'Enviando...';
        voiceButton.textContent = 'Enviando...';
        clearInterval(pollingInterval); 

        try {
            // 2. Enviar la consulta a la webapp (que a su vez envía a RabbitMQ)
            const response = await fetch('/api/recommend', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ query: query })
            });

            const data = await response.json();

            if (response.ok && data.status === 'success') {
                const requestId = data.request_id;
                sendButton.textContent = 'Buscando...';
                voiceButton.textContent = 'Buscando...';
                console.log('Consulta enviada. Request ID:', requestId);
                
                // 3. Iniciar Polling
                pollingAttempts = 0; // REINICIAR CONTADOR DE INTENTOS
                pollingInterval = setInterval(() => pollForResult(requestId), POLLING_INTERVAL); 

            } else {
                displayStatus(`Error al enviar la consulta: ${data.message || 'Error desconocido'}`);
                resetState();
            }
        } catch (error) {
            displayStatus('Error de conexión con el servidor.');
            resetState();
        }
    }

    // --- Listeners de Botones (Sin cambios) ---
    
    // Listener para el botón de Texto
    sendButton.addEventListener('click', () => {
        const query = queryInput.value.trim();
        sendRecommendation(query);
    });

    // Listener para el botón de Voz (Lógica de reconocimiento de voz)
    const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;

    if (SpeechRecognition) {
        const recognition = new SpeechRecognition();
        recognition.lang = 'es-ES'; 
        recognition.interimResults = false; 

        voiceButton.addEventListener('click', () => {
            resultsContainer.innerHTML = '<h2 class="text-xl font-semibold text-gray-700 mb-4 border-b pb-2 hidden">Recomendaciones</h2>';
            loadingIndicator.classList.remove('hidden');
            displayStatus('🎙️ Escuchando... Di tu estado de ánimo ahora.');
            sendButton.disabled = true;
            voiceButton.disabled = true;
            voiceButton.textContent = '...Grabando...';
            recognition.start();
        });

        recognition.addEventListener('result', (event) => {
            const last = event.results.length - 1;
            const query = event.results[last][0].transcript;
            queryInput.value = query; 
            displayStatus(`Texto reconocido: "${query}"`);
            
            recognition.stop(); 
            sendRecommendation(query); 
        });

        recognition.addEventListener('end', () => {
            if (sendButton.textContent.includes('Recomendación') && voiceButton.textContent.includes('Grabando')) {
                resetState(); 
            }
        });

        recognition.addEventListener('error', (event) => {
            displayStatus(`🚨 Error de reconocimiento de voz: ${event.error}`);
            resetState();
        });

    } else {
        voiceButton.disabled = true;
        voiceButton.textContent = 'Voz no soportada en este navegador';
        displayStatus('Tu navegador no soporta el reconocimiento de voz web.');
    }
    
    // --- Funciones Auxiliares ---

    async function pollForResult(requestId) {
        
        // LÓGICA DE TIMEOUT: Detener el polling si excede el número máximo de intentos
        pollingAttempts++;
        if (pollingAttempts >= MAX_ATTEMPTS) {
            clearInterval(pollingInterval);
            displayStatus('⚠️ El proceso tardó demasiado y la conexión se agotó. Intente de nuevo.');
            resetState();
            return;
        }

        try {
            const response = await fetch(`/get_result/${requestId}`);
            const data = await response.json();

            if (data.status === 'ready') {
                clearInterval(pollingInterval); // Detener la consulta

                // NUEVO BLOQUE TRY/CATCH para manejar fallas de renderizado
                try {
                    displayResults(data.recommendations); 
                } catch (renderError) {
                    // Si falla el renderizado, lo reportamos y reseteamos el estado de carga
                    console.error('🚨 Error al renderizar resultados:', renderError);
                    displayStatus('🚨 El resultado llegó, pero hubo un error al mostrarlo. Verifique la consola (F12).');
                }
                
                resetState(); // Resetear el estado (ocultar spinner, habilitar botones)

            } else if (data.status === 'pending') {
                // Ahora usamos el contador para mostrar el estado.
                const timeElapsed = pollingAttempts * POLLING_INTERVAL / 1000;
                loadingIndicator.querySelector('span').textContent = `Analizando tu emoción y buscando... (${timeElapsed}s)`;
                console.log(`Resultado aún pendiente... Intento ${pollingAttempts}/${MAX_ATTEMPTS}`);
            } else {
                console.error('Estado de polling inesperado:', data);
            }
        } catch (error) {
            // Este catch maneja errores de red o JSON inválido
            console.error('Error durante el polling:', error);
            if (error instanceof TypeError && error.message.includes('Failed to fetch')) {
                clearInterval(pollingInterval);
                displayStatus('Error de red al intentar obtener el resultado. (Fallo de conexión)');
                resetState();
            }
        }
    }
    
    function displayResults(recommendations) {
        const titleElement = resultsContainer.querySelector('h2');
        titleElement.classList.remove('hidden');
        titleElement.textContent = "Recomendaciones"; // Asegurar que se muestre

        if (!Array.isArray(recommendations) || recommendations.length === 0) {
            // Limpiar el contenido previo, si existe
            resultsContainer.innerHTML = '<h2 class="text-xl font-semibold text-gray-700 mb-4 border-b pb-2">Recomendaciones</h2>';
            resultsContainer.innerHTML += '<p class="text-gray-500">No se encontraron recomendaciones de películas.</p>';
            return;
        }

        // Limpiar el contenedor antes de añadir nuevos resultados
        resultsContainer.innerHTML = '<h2 class="text-xl font-semibold text-gray-700 mb-4 border-b pb-2">Recomendaciones</h2>';
        
        const listHtml = recommendations.map(movie => `
            <div class="p-4 border-b border-gray-100 last:border-b-0">
                <h3 class="text-lg font-semibold text-indigo-600">${movie.titulo || 'Película sin título'} (${movie.ano_lanzamiento || 'N/A'})</h3>
                <p class="text-gray-700 mt-1">${(movie.sinopsis || 'Sin sinopsis.').substring(0, 300)}...</p>
                <p class="text-sm text-gray-500 mt-2">Rating IMDB: ${movie.rating_imdb || 'N/A'} | Emoción para la recomendación: ${movie.emocion_usada || 'N/A'}</p>
            </div>
        `).join('');

        resultsContainer.innerHTML += `
            <div class="mt-4 border border-gray-200 rounded-lg divide-y divide-gray-100">
                ${listHtml}
            </div>
        `;
    }

    function displayStatus(message) {
        statusMessage.textContent = message;
        statusMessage.classList.toggle('hidden', message === '');
    }

    function resetState() {
        loadingIndicator.classList.add('hidden');
        loadingIndicator.querySelector('span').textContent = 'Analizando tu emoción y buscando...'; // Resetear texto de carga
        sendButton.disabled = false;
        voiceButton.disabled = false;
        sendButton.textContent = 'Buscar Recomendación (Texto)';
        voiceButton.textContent = '🎙️ Iniciar Búsqueda por Voz';
        clearInterval(pollingInterval);
    }
</script>
    </body>
    </html>
    """
    return render_template_string(html_content)

@app.route('/api/recommend', methods=['POST'])
def recommend():
    """Ruta para enviar la consulta a RabbitMQ e iniciar el proceso."""
    global RABBITMQ_CHANNEL_PUBLISH

    if RABBITMQ_CHANNEL_PUBLISH is None:
        return jsonify({"status": "error", "message": "RabbitMQ no está conectado."}), 503

    data = request.get_json()
    user_query = data.get('query')
    
    if not user_query:
        return jsonify({"status": "error", "message": "No se proporcionó consulta"}), 400
    
    # --- Generar Request ID ---
    request_id = str(uuid.uuid4())

    try:
        # El payload ahora contiene la consulta original y el request_id
        payload = json.dumps({"query": user_query, "request_id": request_id})
        
        RABBITMQ_CHANNEL_PUBLISH.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME_EMOTION, # Envía al worker de emoción
            body=payload,
            properties=pika.BasicProperties(
                delivery_mode=2, # Hace el mensaje persistente
            )
        )
        print(f"Web App: Mensaje enviado a RabbitMQ (Emotion Queue): '{user_query}' con ID: {request_id}")
        
        # Devolver el request_id para que el cliente pueda hacer polling
        return jsonify({"status": "success", "message": "Consulta enviada", "request_id": request_id}), 200

    except Exception as e:
        print(f"Error en la webapp al enviar a RabbitMQ: {e}")
        # Intentar reconectar si la publicación falla (solo para el canal de publicación)
        global RABBITMQ_CONNECTION_PUBLISH
        try:
            RABBITMQ_CONNECTION_PUBLISH, RABBITMQ_CHANNEL_PUBLISH = get_rabbitmq_connection_and_channel(QUEUE_NAME_EMOTION)
            print("Web App: Conexión de publicación RabbitMQ re-establecida.")
        except Exception as re_e:
            print(f"Web App: Fallo en la reconexión de RabbitMQ: {re_e}")
            RABBITMQ_CHANNEL_PUBLISH = None # Marcar como fallido

        return jsonify({"status": "error", "message": f"Error interno: {e}. Intente de nuevo."}), 500


@app.route('/get_result/<request_id>', methods=['GET'])
def get_result(request_id):
    """Permite al cliente hacer polling para obtener el resultado final."""
    if request_id in RESULTS_CACHE:
        # Resultado listo, devolverlo y limpiarlo de la cache
        recommendations = RESULTS_CACHE.pop(request_id)
        
        # El worker_recomendador envia un JSON que es una lista. 
        if not isinstance(recommendations, list):
             recommendations = [recommendations]

        return jsonify({"status": "ready", "recommendations": recommendations}), 200
    else:
        return jsonify({"status": "pending"}), 202

# --- INICIALIZACIÓN ---\r
if __name__ == '__main__':
    # 1. Conexión de Publicación Global (Intenta la conexión ANTES de iniciar Flask)
    try:
        # Usamos la cola de EMOCIÓN para el canal de publicación
        RABBITMQ_CONNECTION_PUBLISH, RABBITMQ_CHANNEL_PUBLISH = get_rabbitmq_connection_and_channel(QUEUE_NAME_EMOTION)
    except Exception as e:
        print("🚨 Fallo al conectar RabbitMQ en el arranque. La aplicación Flask no podrá publicar.")
        RABBITMQ_CHANNEL_PUBLISH = None
        
    # 2. Hilo Consumidor de Resultados
    consumer_thread = threading.Thread(target=start_result_consumer, daemon=True)
    consumer_thread.start()

    # 3. Iniciar Flask
    app.run(host='0.0.0.0', port=5000)
    
    # 4. Limpiar conexiones al salir (si es posible)
    if RABBITMQ_CONNECTION_PUBLISH and RABBITMQ_CONNECTION_PUBLISH.is_open:
        RABBITMQ_CONNECTION_PUBLISH.close()