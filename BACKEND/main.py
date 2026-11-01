
import os
import sqlite3
import logging

from contextlib import asynccontextmanager, contextmanager

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

DATABASE_PATH = os.getenv("DATABASE_PATH", "data/aiot.db")


# --------------------------------------------------
# CONEXION CON SQLITE
# --------------------------------------------------

@contextmanager
def database_connection():

    connection = sqlite3.connect(DATABASE_PATH, timeout=10)

    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")

    try:
        yield connection
        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


# --------------------------------------------------
# INICIALIZACION DE LA BASE DE DATOS
# --------------------------------------------------

def initialize_database():

    database_directory = os.path.dirname(
        os.path.abspath(DATABASE_PATH)
    )

    os.makedirs(database_directory, exist_ok=True)

    with database_connection() as connection:

        connection.execute("PRAGMA journal_mode = WAL")

        # Tabla de dispositivos
        connection.execute("""
            CREATE TABLE IF NOT EXISTS devices (
                device_id TEXT PRIMARY KEY,
                first_seen TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_seen TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Tabla de mediciones
        connection.execute("""
            CREATE TABLE IF NOT EXISTS measurements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                topic TEXT NOT NULL,
                temperature REAL NOT NULL,
                humidity REAL NOT NULL,
                measured_at TEXT NOT NULL,
                received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (device_id)
                    REFERENCES devices(device_id)
            )
        """)

        # Tabla de mensajes MQTT originales
        connection.execute("""
            CREATE TABLE IF NOT EXISTS mqtt_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT NOT NULL,
                payload TEXT NOT NULL,
                received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)

    logger.info("Base de datos SQLite preparada correctamente")


# --------------------------------------------------
# GUARDAR MEDICIONES Y MENSAJES
# --------------------------------------------------

def save_message(topic, payload, medicion):

    with database_connection() as connection:

        # Registrar el dispositivo si no existe
        connection.execute(
            """
            INSERT INTO devices (device_id)
            VALUES (?)
            ON CONFLICT(device_id)
            DO UPDATE SET last_seen = CURRENT_TIMESTAMP
            """,
            (medicion.device_id,)
        )

        # Guardar la medicion estructurada
        connection.execute(
            """
            INSERT INTO measurements (
                device_id,
                topic,
                temperature,
                humidity,
                measured_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                medicion.device_id,
                topic,
                medicion.temperature,
                medicion.humidity,
                medicion.timestamp.isoformat()
            )
        )

        # Conservar el mensaje MQTT original
        connection.execute(
            """
            INSERT INTO mqtt_messages (topic, payload)
            VALUES (?, ?)
            """,
            (topic, payload)
        )

    logger.info("Medicion y mensaje guardados correctamente en SQLite")


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

        # Guardar los datos en SQLite
        save_message(msg.topic, payload, medicion)

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

    # Preparar la base de datos
    initialize_database()

    # Crear cliente MQTT
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


# --------------------------------------------------
# CONSULTAR MENSAJES MQTT
# --------------------------------------------------

@app.get("/messages")
def get_messages(
    limit: int = Query(default=10, ge=1, le=100)
):

    with database_connection() as connection:

        cursor = connection.execute(
            """
            SELECT id, topic, payload, received_at
            FROM mqtt_messages
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,)
        )

        rows = cursor.fetchall()

    return [dict(row) for row in rows]


# --------------------------------------------------
# CONSULTAR MEDICIONES
# --------------------------------------------------

@app.get("/measurements")
def get_measurements(
    device_id: str | None = None,
    limit: int = Query(default=10, ge=1, le=100)
):

    with database_connection() as connection:

        if device_id:

            cursor = connection.execute(
                """
                SELECT id, device_id, topic, temperature,
                       humidity, measured_at, received_at
                FROM measurements
                WHERE device_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (device_id, limit)
            )

        else:

            cursor = connection.execute(
                """
                SELECT id, device_id, topic, temperature,
                       humidity, measured_at, received_at
                FROM measurements
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,)
            )

        rows = cursor.fetchall()

    return [dict(row) for row in rows]
@app.get("/devices")

def get_devices():

    with database_connection() as connection:

        cursor = connection.execute(
            """
            SELECT device_id, first_seen, last_seen
            FROM devices
            ORDER BY device_id
            """
        )

        rows = cursor.fetchall()

    return [dict(row) for row in rows]
    
    
    