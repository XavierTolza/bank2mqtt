
import json
import paho.mqtt.client as mqtt
from .handler import Handler

class MqttHandler(Handler):
    """
    A handler to publish transaction data to an MQTT topic.
    """
    def __init__(self, host: str, topic: str, port: int = 1883, username: str = None, password: str = None):
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
            "password": password
        }
        self.topic = topic

    def process_transaction(self, data: dict) -> None:
        """
        Publishes the transaction data to the configured MQTT topic.
        """
        client = mqtt.Client()
        if self.broker_config["username"]:
            client.username_pw_set(self.broker_config["username"], self.broker_config["password"])
        
        try:
            client.connect(self.broker_config["host"], self.broker_config["port"], 60)
            client.loop_start()
            payload = json.dumps(data, ensure_ascii=False)
            result = client.publish(self.topic, payload)
            result.wait_for_publish()
            client.loop_stop()
            client.disconnect()
            print(f"Successfully published transaction to MQTT topic: {self.topic}")
        except Exception as e:
            print(f"Error publishing to MQTT: {e}")

