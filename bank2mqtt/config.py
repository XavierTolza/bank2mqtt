from functools import cached_property
import os
from dotenv import load_dotenv
import yaml

from bank2mqtt.db import Bank2MQTTDatabase
from bank2mqtt.client import PowensClient as Client


class Config(dict):
    @classmethod
    def from_env(cls):
        load_dotenv()
        return cls(
            {
                "db": {
                    "url": os.getenv("BANK2MQTT_DB_URL") or os.getenv("DATABASE_URL")
                },
                "mqtt": {
                    "broker": os.getenv("MQTT_BROKER"),
                    "port": os.getenv("MQTT_PORT"),
                    "user": os.getenv("MQTT_USER"),
                    "password": os.getenv("MQTT_PASSWORD"),
                },
                "powens": {
                    "client_id": os.getenv("POWENS_CLIENT_ID"),
                    "client_secret": os.getenv("POWENS_CLIENT_SECRET"),
                    "redirect_uri": os.getenv("POWENS_REDIRECT_URI"),
                    "auth_token": os.getenv("POWENS_AUTH_TOKEN"),
                },
            }
        )

    @classmethod
    def from_yaml(cls, path):
        with open(path, "r") as f:
            return cls(yaml.safe_load(f))

    @cached_property
    def db(self):
        return Bank2MQTTDatabase(**self["db"])

    @cached_property
    def client(self):
        return Client(**self["powens"])
