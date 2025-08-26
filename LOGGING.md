# Système de Logging - bank2mqtt

Ce document décrit le système de logging intégré dans l'application bank2mqtt utilisant la bibliothèque Loguru.

## Configuration du Logging

Le système de logging peut être configuré via des variables d'environnement :

### Variables d'environnement disponibles

| Variable | Description | Valeur par défaut | Exemples |
|----------|-------------|-------------------|----------|
| `BANK2MQTT_LOG_LEVEL` | Niveau de log minimum | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `BANK2MQTT_LOG_FILE` | Nom du fichier de log | `bank2mqtt.log` | `app.log`, `/tmp/bank2mqtt.log` |
| `BANK2MQTT_LOG_DIR` | Répertoire des logs | Répertoire système par défaut | `/var/log/bank2mqtt` |
| `BANK2MQTT_LOG_ROTATION` | Rotation des fichiers de log | `10 MB` | `100 MB`, `1 GB`, `daily` |
| `BANK2MQTT_LOG_RETENTION` | Rétention des logs | `1 month` | `7 days`, `30 days`, `1 year` |
| `BANK2MQTT_LOG_FORMAT` | Format des logs | `default` | `simple`, `detailed`, `json` |
| `BANK2MQTT_LOG_COLORIZE` | Coloration des logs console | `true` | `false` |
| `BANK2MQTT_LOG_SERIALIZE` | Sérialisation JSON | `false` | `true` |

### Formats de logging disponibles

#### `default`
```
2025-08-26 15:30:45.123 | INFO     | bank2mqtt.client:authenticate:45 - Authentication successful
```

#### `simple`
```
15:30:45 | INFO     | Authentication successful
```

#### `detailed`
```
2025-08-26 15:30:45.123 | INFO     | bank2mqtt.client:authenticate:45 | PID:12345 | Thread:MainThread - Authentication successful
```

#### `json`
```json
{"time":"2025-08-26 15:30:45.123","level":"INFO","name":"bank2mqtt.client","function":"authenticate","line":45,"message":"Authentication successful"}
```

## Exemples d'utilisation

### Configuration simple dans un script bash

```bash
#!/bin/bash
export BANK2MQTT_LOG_LEVEL=DEBUG
export BANK2MQTT_LOG_FILE=debug.log
export BANK2MQTT_LOG_FORMAT=detailed

python -m bank2mqtt list-accounts
```

### Configuration pour la production

```bash
export BANK2MQTT_LOG_LEVEL=INFO
export BANK2MQTT_LOG_DIR=/var/log/bank2mqtt
export BANK2MQTT_LOG_ROTATION="100 MB"
export BANK2MQTT_LOG_RETENTION="30 days"
export BANK2MQTT_LOG_FORMAT=json
export BANK2MQTT_LOG_SERIALIZE=true
```

### Configuration pour le développement

```bash
export BANK2MQTT_LOG_LEVEL=DEBUG
export BANK2MQTT_LOG_FORMAT=detailed
export BANK2MQTT_LOG_COLORIZE=true
```

## Niveaux de logging utilisés

- **TRACE** : Informations très détaillées pour le débogage (ex: chaque transaction processée)
- **DEBUG** : Informations de débogage (ex: paramètres de requête, réponses API)
- **INFO** : Informations générales sur le fonctionnement (ex: début/fin d'opérations)
- **SUCCESS** : Opérations réussies importantes
- **WARNING** : Situations anormales mais gérées (ex: réinitialisation du cache)
- **ERROR** : Erreurs récupérables (ex: échec de requête API)
- **CRITICAL** : Erreurs critiques non récupérables

## Localisation des logs

### Répertoires par défaut par système

- **Linux** : `~/.cache/bank2mqtt/`
- **macOS** : `~/Library/Caches/bank2mqtt/`
- **Windows** : `%LOCALAPPDATA%\\bank2mqtt\\Cache\\`

### Fichiers de log

- **Console** : Logs colorés avec format configurable
- **Fichier** : Format simple ou JSON selon configuration
- **Rotation** : Automatique selon la taille ou la date
- **Rétention** : Nettoyage automatique des anciens logs

## Exemples de logs par commande

### `authenticate`
```
2025-08-26 15:30:45.123 | INFO     | Starting authentication process
2025-08-26 15:30:45.124 | DEBUG    | Using cached authentication data
2025-08-26 15:30:45.125 | SUCCESS  | Authentication completed successfully
```

### `list-accounts`
```
2025-08-26 15:30:46.001 | INFO     | Listing accounts (include_disabled=False)
2025-08-26 15:30:46.002 | DEBUG    | Client created from environment variables
2025-08-26 15:30:46.100 | SUCCESS  | Retrieved 3 accounts
2025-08-26 15:30:46.101 | DEBUG    | Account 12345: Mon Compte Courant (active)
```

### `stream-new-transactions`
```
2025-08-26 15:30:47.001 | INFO     | Starting transaction streaming (limit=1000, count=None)
2025-08-26 15:30:47.002 | INFO     | Last processed transaction ID: 98765
2025-08-26 15:30:47.200 | DEBUG    | Found 5 new transactions in this batch
2025-08-26 15:30:47.201 | DEBUG    | Yielding transaction 1/5: ID 98766 (2025-08-26)
2025-08-26 15:30:47.250 | SUCCESS  | Transaction streaming completed. Yielded 5 transactions
```

## Intégration avec des outils externes

### Avec systemd (Linux)

Créer un service avec redirection des logs :

```ini
[Unit]
Description=Bank2MQTT Service
After=network.target

[Service]
Type=simple
User=bank2mqtt
Environment=BANK2MQTT_LOG_LEVEL=INFO
Environment=BANK2MQTT_LOG_FILE=/var/log/bank2mqtt/bank2mqtt.log
ExecStart=/usr/bin/python3 -m bank2mqtt stream-new-transactions
Restart=always
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

### Avec logrotate

```
/var/log/bank2mqtt/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    create 0644 bank2mqtt bank2mqtt
    postrotate
        systemctl reload bank2mqtt || true
    endscript
}
```

### Avec des outils de monitoring

Les logs au format JSON peuvent être facilement intégrés avec :
- **ELK Stack** (Elasticsearch, Logstash, Kibana)
- **Grafana Loki**
- **Fluentd**
- **Vector**

## Conseils de performance

1. **Production** : Utilisez `INFO` ou `WARNING` comme niveau minimum
2. **Développement** : Utilisez `DEBUG` pour plus de détails
3. **Débogage** : Utilisez `TRACE` temporairement pour des problèmes spécifiques
4. **Fichiers volumineux** : Configurez la rotation et la rétention appropriées
5. **JSON** : Utilisez la sérialisation JSON pour l'intégration avec des outils d'analyse
