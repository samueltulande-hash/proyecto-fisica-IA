
import os
import logging
from contextlib import asynccontextmanager

import psycopg
import paho.mqtt.client as mqtt
from fastapi import FastAPI, Query
from pydantic import ValidationError
from schemas import SensorMeasurement
# --------------------------------------------------
# CONFIGURACION GENERAL
# --------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = "aiot/sensores/#"

DATABASE_URL = os.getenv("DATABASE_URL")


# --------------------------------------------------
# CONEXION Y PREPARACION DE POSTGRESQL
# --------------------------------------------------

def initialize_database():
    if not DATABASE_URL:
        raise RuntimeError("No se encontro DATABASE_URL")

    with psycopg.connect(DATABASE_URL) as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS mqtt_messages (
                    id BIGSERIAL PRIMARY KEY,
                    topic VARCHAR(255) NOT NULL,
                    payload TEXT NOT NULL,
                    received_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                )
            """)

    logger.info("Tabla mqtt_messages preparada correctamente")


# --------------------------------------------------
# GUARDAR MENSAJES EN POSTGRESQL
# --------------------------------------------------

def save_message(topic, payload):
    with psycopg.connect(DATABASE_URL) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO mqtt_messages (topic, payload)
                VALUES (%s, %s)
                """,
                (topic, payload)
            )

    logger.info("Mensaje guardado en PostgreSQL")


# --------------------------------------------------
# EVENTOS MQTT
# --------------------------------------------------

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8")

        logger.info(
            "Mensaje MQTT recibido | Topico: %s | Mensaje: %s",
            msg.topic,
            payload
        )

        # Validar el mensaje JSON con Pydantic
        medicion = SensorMeasurement.model_validate_json(payload)

        logger.info(
            "Medicion valida | Dispositivo: %s | Temperatura: %s | Humedad: %s",
            medicion.device_id,
            medicion.temperature,
            medicion.humidity
        )

        # Guardar el mensaje original en PostgreSQL
        save_message(msg.topic, payload)

    except UnicodeDecodeError:
        logger.error("El mensaje recibido no tiene formato UTF-8 valido")

    except ValidationError:
        logger.exception("Mensaje rechazado: los datos no cumplen el esquema")

    except Exception:
        logger.exception("Error al procesar el mensaje MQTT")


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        logger.info("Conectado correctamente al broker MQTT")
        client.subscribe(MQTT_TOPIC)
        logger.info("Suscrito al topico: %s", MQTT_TOPIC)
    else:
        logger.error("Error de conexion MQTT: %s", reason_code)


# --------------------------------------------------
# CICLO DE VIDA DE FASTAPI
# --------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):

    # Primero preparamos la base de datos
    initialize_database()

    # Creamos el cliente MQTT
    mqtt_client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="aiot-backend"
    )

    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    try:
        logger.info("Conectando con el broker MQTT...")

        mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
        mqtt_client.loop_start()

        app.state.mqtt_client = mqtt_client

        yield

    finally:
        logger.info("Cerrando conexion MQTT...")
        mqtt_client.disconnect()
        mqtt_client.loop_stop()


# --------------------------------------------------
# API FASTAPI
# --------------------------------------------------

app = FastAPI(
    title="AIoT-CORE API",
    lifespan=lifespan
)


@app.get("/")
def root():
    return {
        "project": "AIoT-CORE",
        "status": "running"
    }


@app.get("/health")
def health():
    return {
        "api": "ok"
    }
@app.get("/messages")
def get_messages(limit: int = Query(default=10, ge=1, le=100)):
    with psycopg.connect(DATABASE_URL) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, topic, payload, received_at
                FROM mqtt_messages
                ORDER BY id DESC
                LIMIT %s
                """,
                (limit,)
            )
            rows = cursor.fetchall()

    return [
        {
            "id": row[0],
            "topic": row[1],
            "payload": row[2],
            "received_at": row[3]
        }
        for row in rows
    ]