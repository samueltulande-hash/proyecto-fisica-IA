import os
import logging

from contextlib import asynccontextmanager

import paho.mqtt.client as mqtt

from fastapi import FastAPI, Query, HTTPException
from pydantic import ValidationError

from database import database_connection, initialize_database
from schemas import SensorMeasurement, DeviceCreate, CapabilityCreate, SensorCreate


# --------------------------------------------------
# CONFIGURACION GENERAL
# --------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = "aiot/sensores/#"

def save_message(topic, payload, medicion):

    with database_connection() as connection:

        # 1. Buscar el dispositivo
        device = connection.execute(
            """
            SELECT id
            FROM devices
            WHERE device_id = ?
            """,
            (medicion.device_id,)
        ).fetchone()

        if device is None:
            logger.error(
                "Dispositivo no registrado: %s",
                medicion.device_id
            )
            return

        device_id = device["id"]

        # 2. Buscar los sensores del dispositivo
        sensors = connection.execute(
            """
            SELECT id, sensor_type
            FROM sensors
            WHERE device_id = ?
            """,
            (device_id,)
        ).fetchall()

        sensor_ids = {
            sensor["sensor_type"]: sensor["id"]
            for sensor in sensors
        }

        # 3. Verificar que existan ambos sensores
        if (
            "temperature" not in sensor_ids
            or "humidity" not in sensor_ids
        ):
            logger.error(
                "El dispositivo %s no tiene los sensores necesarios",
                medicion.device_id
            )
            return

        # 4. Guardar temperatura
        connection.execute(
            """
            INSERT INTO measurements (
                sensor_id,
                value,
                measured_at,
                source_topic
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                sensor_ids["temperature"],
                medicion.temperature,
                medicion.timestamp.isoformat(),
                topic
            )
        )

        # 5. Guardar humedad
        connection.execute(
            """
            INSERT INTO measurements (
                sensor_id,
                value,
                measured_at,
                source_topic
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                sensor_ids["humidity"],
                medicion.humidity,
                medicion.timestamp.isoformat(),
                topic
            )
        )

        # 6. Conservar el mensaje MQTT original
        connection.execute(
            """
            INSERT INTO mqtt_messages (topic, payload)
            VALUES (?, ?)
            """,
            (topic, payload)
        )

    logger.info(
        "Mediciones guardadas correctamente para %s",
        medicion.device_id
    )


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
    sensor_type: str | None = None,
    limit: int = Query(default=10, ge=1, le=100)
):

    with database_connection() as connection:

        query = """
            SELECT
                m.id,
                d.device_id,
                s.name AS sensor_name,
                s.sensor_type,
                s.unit,
                m.value,
                m.measured_at,
                m.received_at,
                m.source_topic
            FROM measurements AS m
            INNER JOIN sensors AS s
                ON m.sensor_id = s.id
            INNER JOIN devices AS d
                ON s.device_id = d.id
            WHERE 1 = 1
        """

        parameters = []

        # Filtrar por dispositivo
        if device_id:
            query += " AND d.device_id = ?"
            parameters.append(device_id)

        # Filtrar por tipo de sensor
        if sensor_type:
            query += " AND s.sensor_type = ?"
            parameters.append(sensor_type)

        query += " ORDER BY m.id DESC LIMIT ?"
        parameters.append(limit)

        cursor = connection.execute(
            query,
            parameters
        )

        rows = cursor.fetchall()

    return [dict(row) for row in rows]


@app.get("/devices")
def get_devices():
    with database_connection() as connection:
        cursor = connection.execute("""
            SELECT
                id,
                device_id,
                name,
                device_type,
                status,
                location,
                created_at,
                updated_at
            FROM devices
            ORDER BY device_id
        """)
        rows = cursor.fetchall()

    return [dict(row) for row in rows]

@app.get("/devices/{device_id}")
def get_device(device_id: int):

    with database_connection() as connection:
        cursor = connection.execute(
            """
            SELECT
                id,
                device_id,
                name,
                device_type,
                status,
                location,
                created_at,
                updated_at
            FROM devices
            WHERE id = ?
            """,
            (device_id,)
        )

        row = cursor.fetchone()

    if row is None:
        raise HTTPException(
            status_code=404,
            detail="Dispositivo no encontrado"
        )

    return dict(row)

@app.get("/devices/{device_id}/status")
def get_device_status(device_id: int):

    with database_connection() as connection:
        cursor = connection.execute(
            """
            SELECT
                id,
                device_id,
                status
            FROM devices
            WHERE id = ?
            """,
            (device_id,)
        )

        row = cursor.fetchone()

    if row is None:
        raise HTTPException(
            status_code=404,
            detail="Dispositivo no encontrado"
        )

    return dict(row)

@app.get("/devices/{device_id}/capabilities")
def get_device_capabilities(device_id: int):

    with database_connection() as connection:

        # Comprobar que el dispositivo existe
        device = connection.execute(
            """
            SELECT id, device_id
            FROM devices
            WHERE id = ?
            """,
            (device_id,)
        ).fetchone()

        if device is None:
            raise HTTPException(
                status_code=404,
                detail="Dispositivo no encontrado"
            )

        # Consultar las capacidades del dispositivo
        cursor = connection.execute(
            """
            SELECT
                id,
                name,
                capability_type,
                description
            FROM capabilities
            WHERE device_id = ?
            ORDER BY id
            """,
            (device_id,)
        )

        capabilities = cursor.fetchall()

    return {
        "device_id": device["device_id"],
        "capabilities": [dict(row) for row in capabilities]
    }

@app.post("/devices/{device_id}/capabilities", status_code=201)
def create_device_capability(
    device_id: int,
    capability: CapabilityCreate
):

    with database_connection() as connection:

        # Comprobar que el dispositivo existe
        device = connection.execute(
            """
            SELECT id, device_id
            FROM devices
            WHERE id = ?
            """,
            (device_id,)
        ).fetchone()

        if device is None:
            raise HTTPException(
                status_code=404,
                detail="Dispositivo no encontrado"
            )

        # Registrar la capacidad
        cursor = connection.execute(
            """
            INSERT INTO capabilities (
                device_id,
                name,
                capability_type,
                description
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                device_id,
                capability.name,
                capability.capability_type,
                capability.description
            )
        )

        capability_id = cursor.lastrowid

        # Recuperar la capacidad registrada
        new_capability = connection.execute(
            """
            SELECT
                id,
                device_id,
                name,
                capability_type,
                description
            FROM capabilities
            WHERE id = ?
            """,
            (capability_id,)
        ).fetchone()

    return dict(new_capability)

@app.post("/devices/{device_id}/sensors", status_code=201)
def create_sensor(device_id: int, sensor: SensorCreate):
    with database_connection() as connection:
        # 1. Comprobar que el dispositivo existe
        device = connection.execute(
            "SELECT id FROM devices WHERE id = ?",
            (device_id,)
        ).fetchone()

        if device is None:
            raise HTTPException(
                status_code=404,
                detail="Dispositivo no encontrado"
            )

        # 2. Registrar el sensor asociado al dispositivo
        cursor = connection.execute(
            """
            INSERT INTO sensors (
                device_id,
                name,
                sensor_type,
                unit,
                status
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                device_id,
                sensor.name,
                sensor.sensor_type,
                sensor.unit,
                sensor.status
            )
        )

        # 3. Recuperar el sensor recién creado
        created_sensor = connection.execute(
            """
            SELECT id, device_id, name, sensor_type, unit, status
            FROM sensors
            WHERE id = ?
            """,
            (cursor.lastrowid,)
        ).fetchone()

        return dict(created_sensor)
@app.get("/sensors/{sensor_id}")
def get_sensor(sensor_id: int):
    with database_connection() as connection:
        sensor = connection.execute(
            """
            SELECT
                s.id,
                s.device_id,
                d.device_id AS external_device_id,
                s.name,
                s.sensor_type,
                s.unit,
                s.status
            FROM sensors s
            JOIN devices d ON s.device_id = d.id
            WHERE s.id = ?
            """,
            (sensor_id,)
        ).fetchone()

        if sensor is None:
            raise HTTPException(
                status_code=404,
                detail="Sensor no encontrado"
            )

        return dict(sensor)

@app.post("/devices", status_code=201)
def create_device(device: DeviceCreate):

    with database_connection() as connection:

        # Comprobar si el identificador ya existe
        existing_device = connection.execute(
            """
            SELECT id
            FROM devices
            WHERE device_id = ?
            """,
            (device.device_id,)
        ).fetchone()

        if existing_device:
            raise HTTPException(
                status_code=409,
                detail="Ya existe un dispositivo con ese device_id"
            )

        # Insertar el nuevo dispositivo
        cursor = connection.execute(
            """
            INSERT INTO devices (
                device_id,
                name,
                device_type,
                status,
                location
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                device.device_id,
                device.name,
                device.device_type,
                device.status,
                device.location
            )
        )

        device_id = cursor.lastrowid

        # Recuperar el dispositivo registrado
        new_device = connection.execute(
            """
            SELECT
                id,
                device_id,
                name,
                device_type,
                status,
                location,
                created_at,
                updated_at
            FROM devices
            WHERE id = ?
            """,
            (device_id,)
        ).fetchone()

    return dict(new_device)
    