# --- Stage 1: Builder ---
# Cette étape installe les dépendances dans un environnement virtuel
FROM python:3.12-alpine AS builder

# Définir le répertoire de travail
WORKDIR /app

# Créer un environnement virtuel pour garder les dépendances isolées
RUN python -m venv /opt/venv
# Activer l'environnement virtuel pour les commandes suivantes
ENV PATH="/opt/venv/bin:$PATH"

# Copier et installer les dépendances en premier pour profiter du cache Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code source de l'application
COPY bank2mqtt/ ./bank2mqtt/


# --- Stage 2: Final Image ---
# Cette étape crée l'image finale, légère, pour l'exécution
FROM python:3.12-alpine

# Définir le répertoire de travail
WORKDIR /app

# Copier l'environnement virtuel avec les dépendances depuis l'étape "builder"
COPY --from=builder /opt/venv /opt/venv

# Copier le code source de l'application depuis l'étape "builder"
COPY --from=builder /app/bank2mqtt ./bank2mqtt/

# Créer un utilisateur non-root pour des raisons de sécurité
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

# Ajouter l'environnement virtuel au PATH
ENV PATH="/opt/venv/bin:$PATH"

# Commande pour lancer l'application
# Utilise "-m" pour lancer le module bank2mqtt, ce qui exécute __main__.py
    # Copier le fichier de configuration
    # Assurez-vous d'avoir un fichier config.yaml à la racine lors du build
    COPY config.yaml .

    # Commande pour lancer l'application en mode streaming continu
    CMD ["python", "-m", "bank2mqtt", "--config", "config.yaml", "--loop"]
