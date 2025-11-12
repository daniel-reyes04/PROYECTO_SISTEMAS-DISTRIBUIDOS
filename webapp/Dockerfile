
FROM python:3.11-slim

ENV TZ=America/Bogota

RUN apt-get update && apt-get install -y \
    ffmpeg \
    --no-install-recommends && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt . 
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "app.py"]