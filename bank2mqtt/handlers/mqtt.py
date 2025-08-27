import json
import os
import paho.mqtt.client as mqtt
from typing import Optional


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
        self.client = mqtt.Client()
        if self.broker_config["username"]:
            self.client.username_pw_set(
                self.broker_config["username"], self.broker_config["password"]
            )
        self.client.connect(self.broker_config["host"], self.broker_config["port"], 60)
        self.client.loop_start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.client is None:
            raise ValueError("MQTT client is not initialized.")
        client: mqtt.Client = self.client
        client.loop_stop()
        client.disconnect()
        self.client = None

    def process_transaction(self, data: dict) -> None:
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
        host = os.getenv("MQTT_HOST")
        topic = os.getenv("MQTT_TOPIC")
        port = int(os.getenv("MQTT_PORT", MqttHandler.default_port))
        username = os.getenv("MQTT_USERNAME")
        password = os.getenv("MQTT_PASSWORD")

        if not host or not topic:
            raise ValueError("MQTT host and topic cannot be empty.")

        return cls(
            host=host,
            topic=topic,
            port=port,
            username=username,
            password=password,
        )
