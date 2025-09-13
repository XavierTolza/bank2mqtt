# --- Stage 1: Builder ---
# Cette étape installe les dépendances dans un environnement virtuel
FROM python:3.12-alpine AS builder

# Arguments pour l'UID/GID de l'utilisateur (par défaut 1000 pour correspondre aux utilisateurs Linux standards)
ARG USER_UID=1000
ARG USER_GID=1000

# Définir le répertoire de travail
WORKDIR /app

# Créer un environnement virtuel pour garder les dépendances isolées
RUN python -m venv /opt/venv
# Activer l'environnement virtuel pour les commandes suivantes
ENV PATH="/opt/venv/bin:$PATH"

# Copier et installer les dépendances en premier pour profiter du cache Docker
COPY . /tmp/app
RUN pip install --no-cache-dir /tmp/app/ && rm -rf /tmp/app

# Créer un utilisateur non-root pour des raisons de sécurité avec l'UID/GID spécifié
RUN addgroup -g ${USER_GID} appgroup && adduser -u ${USER_UID} -G appgroup -S appuser

USER appuser

# Commande pour lancer l'application en mode streaming continu
ENTRYPOINT ["python", "-m", "bank2mqtt", "run"]
