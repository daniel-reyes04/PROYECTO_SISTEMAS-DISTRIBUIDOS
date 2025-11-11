FROM python:3.11-buster

WORKDIR /app

COPY requirements.txt .

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# INICIO DE LA INSTALACIÓN DE LIBRERÍAS (VERSION 20251110) <-- ¡AÑADE ESTO!
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "workers/worker_recomendador.py"]