import json
import os
import paho.mqtt.client as mqtt
from typing import Any, Dict, List, Optional
from loguru import logger


class MqttHandler:
    """
    A handler to publish transaction data to an MQTT topic.
    """

    default_port = 1883

    def __init__(
        self,
        host: str,
        topic: str,
        port: int = default_port,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        """
        Initializes the MQTT handler.

        Args:
            host (str): MQTT broker host.
            topic (str): MQTT topic to publish to.
            port (int, optional): MQTT broker port. Defaults to 1883.
            username (str, optional): MQTT username. Defaults to None.
            password (str, optional): MQTT password. Defaults to None.
        """
        if not host or not topic:
            raise ValueError("MQTT host and topic cannot be empty.")
        self.broker_config = {
            "host": host,
            "port": port,
            "username": username,
            "password": password,
        }
        self.topic = topic
        self.client: Optional[mqtt.Client] = None

    def __enter__(self):
        host = self.broker_config["host"]
        port = self.broker_config["port"]
        logger.info(f"Initialisation de la connexion MQTT vers {host}:{port}")
        self.client = mqtt.Client()

        # Configurer les credentials si fournis
        if self.broker_config["username"]:
            logger.debug("Configuration de l'authentification MQTT")
            self.client.username_pw_set(
                self.broker_config["username"], self.broker_config["password"]
            )

        # Tenter la connexion
        try:
            logger.debug("Tentative de connexion au broker MQTT...")
            self.client.connect(
                self.broker_config["host"], self.broker_config["port"], 60
            )
            self.client.loop_start()
            logger.success("Connexion MQTT initialisée")

        except Exception as e:
            # Nettoyer en cas d'erreur
            logger.error(f"Erreur lors de la connexion MQTT: {e}")
            if self.client:
                self.client.loop_stop()
                self.client = None
            raise ConnectionError(f"Impossible de se connecter au broker MQTT: {e}")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.client is None:
            raise ValueError("MQTT client is not initialized.")
        logger.info("Fermeture de la connexion MQTT")
        client: mqtt.Client = self.client
        client.loop_stop()
        client.disconnect()
        self.client = None
        logger.debug("Connexion MQTT fermée avec succès")

    def process_transaction(self, data: List[Dict[str, Any]]) -> None:
        """
        Publishes the transaction data to the configured MQTT topic.
        """
        if not self.client:
            raise ValueError("MQTT client is not initialized.")
        client = self.client

        payload = json.dumps(data, ensure_ascii=False)
        result = client.publish(self.topic, payload)
        result.wait_for_publish()

    @classmethod
    def from_env(cls) -> "MqttHandler":
        """
        Creates an instance of MqttHandler from environment variables.
        """
        logger.debug("Création du MqttHandler à partir des variables d'environnement")
        host = os.getenv("MQTT_HOST")
        topic = os.getenv("MQTT_TOPIC")
        port = int(os.getenv("MQTT_PORT", MqttHandler.default_port))
        username = os.getenv("MQTT_USERNAME")
        password = os.getenv("MQTT_PASSWORD")

        if not host or not topic:
            logger.error("MQTT host et topic ne peuvent pas être vides")
            raise ValueError("MQTT host and topic cannot be empty.")

        logger.info(f"Configuration MQTT: {host}:{port}, topic: {topic}")
        if username:
            logger.debug("Authentification MQTT configurée")

        return cls(
            host=host,
            topic=topic,
            port=port,
            username=username,
            password=password,
        )
