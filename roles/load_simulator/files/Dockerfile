# Schlankes Python-Image als Basis
FROM python:3.11-slim

# Arbeitsverzeichnis im Container festlegen
WORKDIR /app

# System-Abhängigkeiten (minimal gehalten)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Zuerst nur requirements kopieren (Docker Layer Caching nutzen)
COPY requirements.txt .

# Python-Bibliotheken installieren
RUN pip install --no-cache-dir -r requirements.txt

# Den eigentlichen Simulator-Code kopieren
COPY simulator.py .

# Der Port auf dem die Steuer-API lauscht
EXPOSE 8001

# Startbefehl für die FastAPI-App via Uvicorn
CMD ["uvicorn", "simulator:app", "--host", "0.0.0.0", "--port", "9011"]