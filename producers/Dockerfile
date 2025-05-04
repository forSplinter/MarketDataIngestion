FROM python:3.10-slim

# Installer les dépendances système
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Définir le dossier de travail
WORKDIR /app

# Copier le fichier requirements.txt depuis la racine du projet
COPY ../requirements.txt ./requirements.txt

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code source du producer
COPY . .

# Commande à exécuter
CMD ["python", "alpaca_ingestion.py"]
