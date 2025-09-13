from functools import cached_property
import os
from dotenv import load_dotenv
from bank2mqtt.constants import default_sleep_interval, default_mqtt_port
from bank2mqtt.db import Bank2MQTTDatabase
from bank2mqtt.client import PowensClient as Client
import jsonschema

from bank2mqtt.handlers.mqtt import MqttHandler


class Config(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        SCHEMA = {
            "type": "object",
            "properties": {
                "db": {
                    "type": "object",
                    "properties": {"url": {"type": "string"}},
                    "required": ["url"],
                    "additionalProperties": False,
                },
                "mqtt": {
                    "type": "object",
                    "properties": {
                        "host": {"type": "string"},
                        "port": {"type": ["string", "integer"]},
                        "username": {"type": ["string", "null"]},
                        "password": {"type": ["string", "null"]},
                    },
                    "required": ["host"],
                    "additionalProperties": False,
                },
                "powens": {
                    "type": "object",
                    "properties": {
                        "domain": {"type": "string"},
                        "client_id": {"type": "string"},
                        "client_secret": {"type": ["string", "null"]},
                        "redirect_uri": {"type": ["string", "null"]},
                        "auth_token": {"type": ["string", "null"]},
                    },
                    "required": ["client_id", "client_secret", "domain"],
                    "additionalProperties": False,
                },
                "settings": {
                    "type": "object",
                    "properties": {
                        "sleep_interval": {"type": "integer", "minimum": 60},
                    },
                    "required": ["sleep_interval"],
                    "additionalProperties": False,
                },
            },
            "required": ["db", "mqtt", "powens"],
            "additionalProperties": False,
        }

        jsonschema.validate(self, SCHEMA)

    @classmethod
    def from_env(cls):
        load_dotenv()
        auth_token = os.getenv("POWENS_AUTH_TOKEN")
        if auth_token is None:
            auth_token_b64 = os.getenv("POWENS_AUTH_TOKEN_B64")
            if auth_token_b64:
                import base64

                auth_token = base64.b64decode(auth_token_b64).decode("utf-8")

        sleep_interval = int(os.getenv("SLEEP_INTERVAL", default_sleep_interval))
        return cls(
            {
                "db": {
                    "url": os.getenv("BANK2MQTT_DB_URL") or os.getenv("DATABASE_URL")
                },
                "mqtt": {
                    "host": os.getenv("MQTT_BROKER", os.getenv("MQTT_HOST")),
                    "port": os.getenv("MQTT_PORT", default_mqtt_port),
                    "username": os.getenv("MQTT_USER", os.getenv("MQTT_USERNAME")),
                    "password": os.getenv("MQTT_PASSWORD"),
                },
                "powens": {
                    "domain": os.getenv("POWENS_DOMAIN"),
                    "client_id": os.getenv("POWENS_CLIENT_ID"),
                    "client_secret": os.getenv("POWENS_CLIENT_SECRET"),
                    "redirect_uri": os.getenv("POWENS_REDIRECT_URI"),
                    "auth_token": auth_token,
                },
                "settings": {"sleep_interval": sleep_interval},
            }
        )

    @cached_property
    def db(self):
        return Bank2MQTTDatabase(**self["db"])

    @cached_property
    def client(self) -> Client:
        db = self.db
        client = Client(**self["powens"])
        _, db_auth = db.get_domain_and_latest_auth(client.domain, client.client_id)
        match (db_auth, client.auth_token):
            case (db_auth_obj, auth_token) if (
                db_auth is None or (db_auth_obj or {}).get("auth_token") != auth_token
            ):
                # Save the new auth token to the database
                auth_data = dict(
                    client_id=client.client_id,
                    client_secret=client.client_secret,
                    auth_token=auth_token,
                )
                domain_data = dict(
                    domain=client.domain,
                    redirect_uri=client.redirect_uri,
                )
                db.register_domain_and_auth(
                    domain_data=domain_data,
                    auth_data=auth_data,
                )
            case (db_auth_obj, None) if db_auth_obj is not None:
                client.auth_token = db_auth_obj["auth_token"]
        return client

    @cached_property
    def mqtt_handler(self):
        return MqttHandler(**self["mqtt"])

    @property
    def sleep_interval(self):
        return self.get("settings", {}).get("sleep_interval", default_sleep_interval)
