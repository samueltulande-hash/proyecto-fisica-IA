import os
import logging
import sqlite3
import json
import uuid
import asyncio
import secrets
from datetime import timedelta

from contextlib import asynccontextmanager
from pydantic import BaseModel, ValidationError

import paho.mqtt.client as mqtt
from fastapi import (
    FastAPI,
    Query,
    HTTPException,
    Request,
    Depends,
    status
)

from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pwdlib import PasswordHash
import jwt
from jwt.exceptions import InvalidTokenError

from datetime import datetime, timezone
from database import database_connection, initialize_database
from schemas import SensorMeasurement, DeviceCreate, CapabilityCreate, SensorCreate
from schemas import (
    SensorMeasurement,
    DeviceCreate,
    CapabilityCreate,
    SensorCreate,
    DeviceRegister,
    DeviceStatus,
    CommandCreate,
    CommandResponse,
    CommandError,
    AssistantChatRequest,
    AssistantChatResponse,
    UserCreate,
    Token,
    UserResponse
)


# --------------------------------------------------
# CONFIGURACION GENERAL
# --------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
"OLLAMA_URL",
http://host.docker.internal:11434"


OLLAMA_MODEL = os.getenv(
    "OLLAMA_MODEL",
    "qwen2.5:7b"
)

# ==========================================
# CONFIGURACION JWT - SPRINT 6
# ==========================================

JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY")

JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")

JWT_ACCESS_TOKEN_EXPIRE_MINUTES = int(
    os.getenv("JWT_ACCESS_TOKEN_EXPIRE_MINUTES", "30")
)

if not JWT_SECRET_KEY or len(JWT_SECRET_KEY) < 32:
    raise RuntimeError("JWT_SECRET_KEY no configurada o demasiado corta")

password_hash = PasswordHash.recommended()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")
# ==========================================
# FUNCIONES DE AUTENTICACION JWT
# ==========================================

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return password_hash.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    return password_hash.hash(password)

def verify_device_secret(device_secret: str, secret_hash: str) -> bool:
    try:
        return password_hash.verify(device_secret, secret_hash)
    except Exception:
        return False
def authenticate_mqtt_device(device_id: str, device_secret: str) -> bool:

    with database_connection() as connection:
        device = connection.execute(
            """
            SELECT secret_hash
            FROM devices
            WHERE device_id = ?
            """,
            (device_id,)
        ).fetchone()

    if device is None or not device["secret_hash"]:
        return False

    return verify_device_secret(
        device_secret,
        device["secret_hash"]
    )
def create_access_token(data: dict) -> str:
    to_encode = data.copy()

    expire = datetime.now(timezone.utc) + timedelta(
        minutes=JWT_ACCESS_TOKEN_EXPIRE_MINUTES
    )

    to_encode.update({"exp": expire})

    return jwt.encode(
        to_encode,
        JWT_SECRET_KEY,
        algorithm=JWT_ALGORITHM
    )

def get_current_user(token: str = Depends(oauth2_scheme)):

    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="No se pudieron validar las credenciales",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(
            token,
            JWT_SECRET_KEY,
            algorithms=[JWT_ALGORITHM]
        )

        username = payload.get("sub")

        if username is None:
            raise credentials_exception

    except InvalidTokenError:
        raise credentials_exception

    with database_connection() as connection:

        user = connection.execute(
            """
            SELECT id, username, email, role, is_active
            FROM users
            WHERE username = ?
            """,
            (username,)
        ).fetchone()

    if user is None or not user["is_active"]:
        raise credentials_exception

    return dict(user)


MQTT_TOPIC = "aiot/sensores/#"
MQTT_REGISTER_TOPIC = "aiot/devices/register"
MQTT_STATUS_TOPIC = "aiot/devices/+/status"
MQTT_RESPONSE_TOPIC = "aiot/devices/+/response"
MQTT_ERROR_TOPIC = "aiot/devices/+/error"

COMMAND_TIMEOUT_SECONDS = int(
    os.getenv("COMMAND_TIMEOUT_SECONDS", "30")
)

TIMEOUT_CHECK_INTERVAL_SECONDS = int(
    os.getenv("TIMEOUT_CHECK_INTERVAL_SECONDS", "5")
)

MAX_COMMAND_RETRIES = int(
    os.getenv("MAX_COMMAND_RETRIES", "3")
)

COMMAND_RETRY_INTERVAL_SECONDS = int(
    os.getenv("COMMAND_RETRY_INTERVAL_SECONDS", "10")
)
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
            "Mensaje MQTT recibido | Topico: %s",
            msg.topic
        )



        # ======================================
        # 1. REGISTRO AUTOMATICO DE DISPOSITIVOS
        # ======================================

        if msg.topic == MQTT_REGISTER_TOPIC:

            device = DeviceRegister.model_validate_json(payload)

            save_device_registration(
                msg.topic,
                payload,
                device
            )

            return

        # ======================================
        # 2. ACTUALIZACION DE ESTADO
        # ======================================

        if msg.topic.startswith("aiot/devices/") and msg.topic.endswith("/status"):

            device_status = DeviceStatus.model_validate_json(payload)

            topic_device_id = msg.topic.split("/")[2]

            if topic_device_id != device_status.device_id:

                logger.error(
                    "El device_id no coincide con el topico MQTT"
                )

                return
                safe_payload = device_status.model_dump(
                exclude={"device_secret"}
            )

            update_device_status(
                device_status.device_id,
                device_status.status,
                json.dumps(safe_payload)
            )
            return

        # ======================================
        # 3. RESPUESTAS DEL COMMAND GATEWAY
        # ======================================

        if msg.topic.startswith("aiot/devices/") and msg.topic.endswith("/response"):

            command_response = CommandResponse.model_validate_json(payload)

            topic_device_id = msg.topic.split("/")[2]

            if topic_device_id != command_response.device_id:
                logger.error(
                    "El device_id no coincide con el topico MQTT"
                )
                return

            if not authenticate_mqtt_device(
                command_response.device_id,
                command_response.device_secret
            ):
                logger.warning(
                    "Respuesta MQTT rechazada: credencial invalida"
                )
                return

            process_command_response(command_response)

            return



        #====================================
        # 4. ERRORES DEL COMMAND GATEWAY
        # ======================================

                # ======================================
        # 4. ERRORES DEL COMMAND GATEWAY
        # ======================================

        if msg.topic.startswith("aiot/devices/") and msg.topic.endswith("/error"):

            command_error = CommandError.model_validate_json(payload)

            topic_device_id = msg.topic.split("/")[2]

            if topic_device_id != command_error.device_id:
                logger.error(
                    "El device_id no coincide con el topico MQTT"
                )
                return

            if not authenticate_mqtt_device(
                command_error.device_id,
                command_error.device_secret
            ):
                logger.warning(
                    "Error MQTT rechazado: credencial invalida"
                )
                return

            process_command_error(command_error)

            return


        # ======================================
        # 5. RECEPCION DE MEDICIONES
        # ======================================

        if msg.topic.startswith("aiot/sensores/"):

            medicion = SensorMeasurement.model_validate_json(payload)

            topic_device_id = msg.topic.split("/")[2]

            if topic_device_id != medicion.device_id:
                logger.error(
                    "El device_id no coincide con el topico MQTT"
                )
                return

            if not authenticate_mqtt_device(
                medicion.device_id,
                medicion.device_secret
            ):
                logger.warning(
                    "Medicion MQTT rechazada: credencial invalida"
                )
                return

            logger.info(
                "Medicion valida | Dispositivo: %s | Temperatura: %s | Humedad: %s",
                medicion.device_id,
                medicion.temperature,
                medicion.humidity
            )

            safe_payload = medicion.model_dump(
                exclude={"device_secret"}
            )

            save_message(
                msg.topic,
                json.dumps(safe_payload),
                medicion
            )

            return
            logger.warning(
            "Mensaje recibido en un topico no reconocido: %s",
            msg.topic
        )

    except UnicodeDecodeError:
        logger.error(
            "El mensaje recibido no tiene formato UTF-8 valido"
        )

    except ValidationError:
        logger.error(
            "Mensaje rechazado: los datos no cumplen el esquema"
        )

    except Exception:
        logger.exception(
            "Error al procesar el mensaje MQTT"
        )



# --------------------------------------------------
# PROCESAMIENTO DE RESPUESTAS DEL COMMAND GATEWAY
# --------------------------------------------------
def process_command_response(command_response):

    current_time = datetime.now(timezone.utc).isoformat()

    with database_connection() as connection:

        command = connection.execute(
            """
            SELECT
                c.id,
                c.status,
                c.error_message
            FROM commands AS c
            INNER JOIN devices AS d
                ON c.device_id = d.id
            WHERE c.command_uuid = ?
              AND d.device_id = ?
            """,
            (
                command_response.command_uuid,
                command_response.device_id
            )
        ).fetchone()

        if command is None:
            logger.warning(
                "Respuesta recibida para comando inexistente: %s",
                command_response.command_uuid
            )
            return

        status = command["status"]

        # ACK: solamente aceptar en comandos enviados
        if command_response.status == "received":

            if status == "sent":
                connection.execute(
                    """
                    UPDATE commands
                    SET status = 'acknowledged',
                        acknowledged_at = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        current_time,
                        current_time,
                        command["id"]
                    )
                )

            else:
                logger.info(
                    "ACK ignorado para comando en estado: %s",
                    status
                )
                return

        # ConfirmaciÃ³n de ejecuciÃ³n
        elif command_response.status == "completed":

            # Permitir resolver fallos causados por timeout/retry
            late_completion = (
                status in ("timeout", "failed")
                and command["error_message"] in (
                    "Tiempo de espera agotado",
                    "Reintentos mÃ¡ximos agotados"
                )
            )

            if status in ("sent", "acknowledged") or late_completion:

                connection.execute(
                    """
                    UPDATE commands
                    SET status = 'completed',
                        acknowledged_at = COALESCE(
                            acknowledged_at, ?
                        ),
                        completed_at = ?,
                        error_message = NULL,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        current_time,
                        current_time,
                        current_time,
                        command["id"]
                    )
                )

            else:
                logger.warning(
                    "FinalizaciÃ³n tardÃ­a ignorada: estado=%s",
                    status
                )
                return

        # EjecuciÃ³n fallida
        elif command_response.status == "failed":

            if status not in ("sent", "acknowledged"):
                logger.warning(
                    "Fallo ignorado para comando terminal: %s",
                    status
                )
                return

            connection.execute(
                """
                UPDATE commands
                SET status = 'failed',
                    error_message = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    command_response.message
                    or "El dispositivo reportÃ³ un error",
                    current_time,
                    command["id"]
                )
            )

    logger.info(
        "Respuesta procesada | Comando: %s | Estado: %s",
        command_response.command_uuid,
        command_response.status
    )

def process_command_error(command_error):

    current_time = datetime.now(timezone.utc).isoformat()

    with database_connection() as connection:

        command = connection.execute(
            """
            SELECT
                c.id,
                c.status
            FROM commands AS c
            INNER JOIN devices AS d
                ON c.device_id = d.id
            WHERE c.command_uuid = ?
              AND d.device_id = ?
            """,
            (
                command_error.command_uuid,
                command_error.device_id
            )
        ).fetchone()

        if command is None:
            logger.warning(
                "Error recibido para un comando inexistente: %s",
                command_error.command_uuid
            )
            return

        if command["status"] in ("completed", "failed", "timeout"):
            logger.warning(
                "Error ignorado: el comando ya es terminal (%s)",
                command["status"]
            )
            return

        connection.execute(
            """
            UPDATE commands
            SET
                status = 'failed',
                error_message = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                command_error.error,
                current_time,
                command["id"]
            )
        )

    logger.error(
        "Error del dispositivo | Comando: %s | Error: %s",
        command_error.command_uuid,
        command_error.error
     )


def publish_command(mqtt_client, device_id, command_uuid, command_type, payload):
    current_time = datetime.now(timezone.utc).isoformat()

    message = {
        "command_uuid": command_uuid,
        "device_id": device_id,
        "command_type": command_type,
        "payload": payload,
        "issued_at": current_time
    }

    topic = f"aiot/devices/{device_id}/command"

    result = mqtt_client.publish(
        topic,
        json.dumps(message),
        qos=1
    )

    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        return False

    logger.info("Comando %s publicado para %s", command_uuid, device_id)
    return True

def on_connect(client, userdata, flags, reason_code, properties):

    if reason_code == 0:

        logger.info("Conexion MQTT establecida correctamente")

        client.subscribe(MQTT_TOPIC, qos=1)
        client.subscribe(MQTT_REGISTER_TOPIC, qos=1)
        client.subscribe(MQTT_STATUS_TOPIC, qos=1)
        client.subscribe(MQTT_RESPONSE_TOPIC, qos=1)
        client.subscribe(MQTT_ERROR_TOPIC, qos=1)

        logger.info("Suscrito a: %s", MQTT_TOPIC)
        logger.info("Suscrito a: %s", MQTT_REGISTER_TOPIC)
        logger.info("Suscrito a: %s", MQTT_STATUS_TOPIC)
        logger.info("Suscrito a: %s", MQTT_RESPONSE_TOPIC)
        logger.info("Suscrito a: %s", MQTT_ERROR_TOPIC)

    else:
        logger.error(
            "Error al conectar con MQTT. Codigo: %s",
            reason_code
        )



def save_device_registration(topic, payload, device):

    current_time = datetime.now(timezone.utc).isoformat()

    with database_connection() as connection:

        # 1. Buscar el dispositivo previamente autorizado
        registered_device = connection.execute(
            """
            SELECT id, secret_hash
            FROM devices
            WHERE device_id = ?
            """,
            (device.device_id,)
        ).fetchone()

        # 2. Rechazar dispositivos desconocidos
        if registered_device is None:
            logger.warning(
                "Registro rechazado: dispositivo desconocido"
            )
            return

        # 3. Comprobar que tenga una credencial configurada
        if not registered_device["secret_hash"]:
            logger.warning(
                "Registro rechazado: dispositivo sin credencial"
            )
            return

        # 4. Validar la credencial del dispositivo
        if not verify_device_secret(
            device.device_secret,
            registered_device["secret_hash"]
        ):
            logger.warning(
                "Registro rechazado: credencial incorrecta"
            )
            return

        internal_id = registered_device["id"]

        # 5. Actualizar Ãºnicamente el dispositivo autorizado
        connection.execute(
            """
            UPDATE devices
            SET
                name = ?,
                device_type = ?,
                status = 'online',
                firmware_version = ?,
                ip = ?,
                last_seen = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                device.device_id,
                device.device_type,
                device.firmware_version,
                device.ip,
                current_time,
                internal_id
            )
        )

        # 6. Sincronizar capacidades
        connection.execute(
            """
            DELETE FROM capabilities
            WHERE device_id = ?
            """,
            (internal_id,)
        )

        for capability in device.capabilities:

            connection.execute(
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
                    internal_id,
                    capability.name,
                    capability.capability_type,
                    capability.description
                )
            )

        # 7. Guardar el mensaje sin almacenar el secreto
        safe_payload = device.model_dump(
            exclude={"device_secret"}
        )

        connection.execute(
            """
            INSERT INTO mqtt_messages (topic, payload)
            VALUES (?, ?)
            """,
            (topic, json.dumps(safe_payload))
        )

    logger.info(
        "Dispositivo registrado correctamente: %s",
        device.device_id
    )
    # --------------------------------------------------
# ACTUALIZAR ESTADO DE DISPOSITIVOS
# --------------------------------------------------

def update_device_status(device_id, status, payload):

    current_time = datetime.now(timezone.utc).isoformat()

    with database_connection() as connection:

        # 1. Comprobar que el dispositivo existe
        device = connection.execute(
            """
            SELECT id
            FROM devices
            WHERE device_id = ?
            """,
            (device_id,)
        ).fetchone()

        if device is None:
            logger.warning(
                "No se puede actualizar el estado. Dispositivo no registrado: %s",
                device_id
            )
            return

        # 2. Actualizar estado y ultima comunicacion
        connection.execute(
            """
            UPDATE devices
            SET
                status = ?,
                last_seen = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE device_id = ?
            """,
            (
                status,
                current_time,
                device_id
            )
        )

        # 3. Guardar mensaje MQTT original
        connection.execute(
            """
            INSERT INTO mqtt_messages (topic, payload)
            VALUES (?, ?)
            """,
            (
                f"aiot/devices/{device_id}/status",
                payload
            )
        )

    logger.info(
        "Estado actualizado | Dispositivo: %s | Estado: %s",
        device_id,
        status
    )

# --------------------------------------------------
# EJECUTOR SEGURO DE HERRAMIENTAS
# --------------------------------------------------

def execute_aiot_tool(tool_name: str, arguments: dict):

    if tool_name == "list_devices":
        return get_devices()

    if tool_name == "get_device_status":
        device_id = arguments.get("device_id")

        if not isinstance(device_id, str) or not device_id.strip():
            raise ValueError("device_id invÃ¡lido")

        return get_device_status(device_id.strip())

    if tool_name == "get_device_capabilities":
        device_id = arguments.get("device_id")

        if not isinstance(device_id, str) or not device_id.strip():
            raise ValueError("device_id invÃ¡lido")

        return get_device_capabilities(device_id.strip())

    if tool_name == "get_measurements":
        device_id = arguments.get("device_id")
        sensor_type = arguments.get("sensor_type")
        limit = arguments.get("limit", 10)

        if device_id is not None and not isinstance(device_id, str):
            raise ValueError("device_id invÃ¡lido")

        if sensor_type is not None and not isinstance(sensor_type, str):
            raise ValueError("sensor_type invÃ¡lido")

        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
            raise ValueError("limit debe estar entre 1 y 100")

        return get_measurements(
            device_id=device_id,
            sensor_type=sensor_type,
            limit=limit
        )

    raise ValueError("Herramienta no permitida")
# --------------------------------------------------
# HERRAMIENTAS DEL ASISTENTE AIOT
# --------------------------------------------------

AIOT_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "list_devices",
            "description": "Lista los dispositivos IoT registrados y su estado.",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_device_status",
            "description": "Consulta el estado actual de un dispositivo IoT.",
            "parameters": {
                "type": "object",
                "properties": {
                    "device_id": {
                        "type": "string",
                        "description": "Identificador del dispositivo"
                    }
                },
                "required": ["device_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_device_capabilities",
            "description": "Consulta los sensores y capacidades de un dispositivo.",
            "parameters": {
                "type": "object",
                "properties": {
                    "device_id": {
                        "type": "string",
                        "description": "Identificador del dispositivo"
                    }
                },
                "required": ["device_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_measurements",
            "description": "Consulta las Ãºltimas mediciones de los sensores.",
            "parameters": {
                "type": "object",
                "properties": {
                    "device_id": {
                        "type": "string",
                        "description": "Identificador del dispositivo (opcional)"
                    },
                    "sensor_type": {
                        "type": "string",
                        "description": "Tipo de sensor (opcional)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Cantidad de mediciones, entre 1 y 100"
                    }
                },
                "required": []
            }
        }
    }
]
def ask_ollama(message: str) -> str:
    import urllib.request

    url = f"{OLLAMA_URL}/api/chat"

    messages = [
        {
            "role": "system",
            "content": (
                "Eres un asistente inteligente para un sistema IoT. "
                "Responde en espaÃ±ol. "
                "Cuando necesites informaciÃ³n real de dispositivos, "
                "sensores o mediciones, utiliza las herramientas disponibles. "
                "No inventes datos. "
                "Solo puedes consultar informaciÃ³n; no afirmes haber "
                "ejecutado acciones fÃ­sicas."
            )
        },
        {
            "role": "user",
            "content": message
        }
    ]

    def call_ollama():
        data = {
            "model": OLLAMA_MODEL,
            "messages": messages,
            "tools": AIOT_TOOLS,
            "stream": False
        }

        request = urllib.request.Request(
            url,
            data=json.dumps(data).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST"
        )

        with urllib.request.urlopen(request, timeout=120) as response:
            return json.loads(response.read().decode("utf-8"))

    try:
        result = call_ollama()

        tool_calls = result.get("message", {}).get("tool_calls", [])

        if tool_calls:
            messages.append(result["message"])

            for tool_call in tool_calls:
                function = tool_call.get("function", {})
                tool_name = function.get("name")
                arguments = function.get("arguments", {})

                try:
                    tool_result = execute_aiot_tool(
                        tool_name,
                        arguments
                    )

                    tool_content = json.dumps(
                        tool_result,
                        ensure_ascii=False,
                        default=str
                    )

                except Exception as error:
                    logger.exception("Error ejecutando herramienta IoT")
                    tool_content = json.dumps({
                        "error": str(error)
                    }, ensure_ascii=False)

                messages.append({
                    "role": "tool",
                    "content": tool_content
                })

            result = call_ollama()

        return result["message"]["content"]

    except Exception:
        logger.exception("Error al comunicarse con Ollama")
        raise HTTPException(
            status_code=503,
            detail="El servicio de IA no estÃ¡ disponible"
        )
# --------------------------------------------------
# CICLO DE VIDA DE FASTAPI
# --------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):

    initialize_database()

    mqtt_client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="aiot-backend"
    )
    if not MQTT_USERNAME or not MQTT_PASSWORD:
     raise RuntimeError(
        "Faltan las credenciales MQTT del backend"
    )

    mqtt_client.username_pw_set(
    MQTT_USERNAME,
    MQTT_PASSWORD
)

    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    timeout_task = None

    try:

        logger.info("Conectando con el broker MQTT...")

        mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
        mqtt_client.loop_start()

        app.state.mqtt_client = mqtt_client

        timeout_task = asyncio.create_task(
            monitor_command_timeouts()
        )

        yield

    finally:

        logger.info("Cerrando servicios del backend...")

        if timeout_task is not None:
            timeout_task.cancel()

            try:
                await timeout_task
            except asyncio.CancelledError:
                pass

        mqtt_client.disconnect()
        mqtt_client.loop_stop()

# --------------------------------------------------
# API FASTAPI
# --------------------------------------------------

app = FastAPI(
    title="AIoT-CORE API",
    lifespan=lifespan
)

# ==========================================
# AUTENTICACION - REGISTRO DE USUARIOS
# ==========================================

@app.post("/auth/register", response_model=UserResponse)
def register_user(data: UserCreate):

    with database_connection() as connection:

        total_users = connection.execute(
            "SELECT COUNT(*) AS total FROM users"
        ).fetchone()["total"]

        if total_users > 0:
            raise HTTPException(
                status_code=403,
                detail="El registro inicial ya fue realizado"
            )

        try:
            cursor = connection.execute(
                """
                INSERT INTO users (
                    username,
                    email,
                    password_hash,
                    role
                )
                VALUES (?, ?, ?, ?)
                """,
                (
                    data.username,
                    data.email,
                    get_password_hash(data.password),
                    "admin"
                )
            )

            user_id = cursor.lastrowid

        except sqlite3.IntegrityError:
            raise HTTPException(
                status_code=409,
                detail="El usuario o correo ya existe"
            )

        return UserResponse(
            id=user_id,
            username=data.username,
            email=data.email,
            role="admin",
            is_active=True
        )


# ==========================================
# AUTENTICACION - INICIO DE SESION
# ==========================================

@app.post("/auth/token", response_model=Token)
def login_user(
    form_data: OAuth2PasswordRequestForm = Depends()
):

    with database_connection() as connection:

        user = connection.execute(
            """
            SELECT *
            FROM users
            WHERE username = ?
            """,
            (form_data.username,)
        ).fetchone()

    if user is None or not verify_password(
        form_data.password,
        user["password_hash"]
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario o contraseÃ±a incorrectos",
            headers={"WWW-Authenticate": "Bearer"}
        )

    if not user["is_active"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="La cuenta estÃ¡ desactivada"
        )

    access_token = create_access_token(
        data={"sub": user["username"]}
    )

    return Token(access_token=access_token)

# ==========================================
# AUTENTICACION - USUARIO ACTUAL
# ==========================================

@app.get("/auth/me", response_model=UserResponse)
def read_current_user(
    current_user: dict = Depends(get_current_user)
):

    return UserResponse(
        id=current_user["id"],
        username=current_user["username"],
        email=current_user["email"],
        role=current_user["role"],
        is_active=bool(current_user["is_active"])
    )

# ==========================================
# CONTROL DE ACCESO RBAC - SPRINT 6
# ==========================================

def require_roles(*allowed_roles):

    def role_checker(
        current_user: dict = Depends(get_current_user)
    ):
        if current_user["role"] not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="No tienes permisos para realizar esta acciÃ³n"
            )

        return current_user

    return role_checker
# ==========================================
# ADMINISTRACION DE USUARIOS - RBAC
# ==========================================

@app.post("/auth/users", response_model=UserResponse)
def create_user(
    data: UserCreate,
    current_user: dict = Depends(
     require_roles("admin")
    )
):

    with database_connection() as connection:

        try:
            cursor = connection.execute(
                """
                INSERT INTO users (
                    username,
                    email,
                    password_hash,
                    role
                )
                VALUES (?, ?, ?, ?)
                """,
                (
                    data.username,
                    data.email,
                    get_password_hash(data.password),
                    "user"
                )
            )

            user_id = cursor.lastrowid

        except sqlite3.IntegrityError:
            raise HTTPException(
                status_code=409,
                detail="El usuario o correo ya existe"
            )

    return UserResponse(
        id=user_id,
        username=data.username,
        email=data.email,
        role="user",
        is_active=True
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

@app.post("/assistant/chat", response_model=AssistantChatResponse)
def assistant_chat(data: AssistantChatRequest):

    if not data.message.strip():
        raise HTTPException(
            status_code=400,
            detail="El mensaje no puede estar vacÃ­o"
        )

    response = ask_ollama(data.message)

    return AssistantChatResponse(
        response=response,
        status="completed"
    )


# --------------------------------------------------
# CONSULTAR MENSAJES MQTT
# --------------------------------------------------

@app.get("/messages")
def get_messages(
    limit: int = Query(default=10, ge=1, le=100),
    current_user: dict = Depends(
        require_roles("admin", "operator", "user")
    )
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
    limit: int = Query(default=10, ge=1, le=100),
    current_user: dict = Depends(
        require_roles("admin", "operator", "user")
    )
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
def get_devices(
    current_user: dict = Depends(
        require_roles("admin", "operator", "user")
    )
):
    with database_connection() as connection:
        cursor = connection.execute("""
           SELECT
            id,
         device_id,
         name,
    device_type,
    status,
    location,
    firmware_version,
    ip,
    last_seen,
    created_at,
    updated_at
FROM devices
            ORDER BY device_id
        """)
        rows = cursor.fetchall()

    return [dict(row) for row in rows]


@app.get("/devices/{device_id}")
def get_device(
    device_id: str,
    current_user: dict = Depends(
        require_roles("admin", "operator", "user")
    )
):

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
                firmware_version,
                ip,
                last_seen,
                created_at,
                updated_at
            FROM devices
            WHERE device_id = ?
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
def get_device_status(
    device_id: str,
    current_user: dict = Depends(
        require_roles("admin", "operator", "user")
    )
):

    with database_connection() as connection:
        cursor = connection.execute(
            """
            SELECT
                id,
                device_id,
                status,
                last_seen,
                updated_at
            FROM devices
            WHERE device_id = ?
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
def get_device_capabilities(
    device_id: str,
    current_user: dict = Depends(
        require_roles("admin", "operator", "user")
    )
):
    with database_connection() as connection:

        # Comprobar que el dispositivo existe
        device = connection.execute(
            """
            SELECT id, device_id
            FROM devices
            WHERE device_id = ?
            """,
            (device_id,)
        ).fetchone()

        if device is None:
            raise HTTPException(
                status_code=404,
                detail="Dispositivo no encontrado"
            )

        # Consultar las capacidades usando el ID interno
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
            (device["id"],)
        )

        capabilities = cursor.fetchall()

    return {
        "device_id": device["device_id"],
        "capabilities": [dict(row) for row in capabilities]
    }

@app.post("/devices/{device_id}/capabilities", status_code=201)
def create_device_capability(
    device_id: str,
    capability: CapabilityCreate,
    current_user: dict = Depends(
        require_roles("admin", "operator")
    )
):

    with database_connection() as connection:

        # Comprobar que el dispositivo existe
        device = connection.execute(
            """
            SELECT id, device_id
            FROM devices
            WHERE device_id = ?
            """,
            (device_id,)
        ).fetchone()

        if device is None:
            raise HTTPException(
                status_code=404,
                detail="Dispositivo no encontrado"
            )

        # Registrar la capacidad
        try:
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
                    device["id"],
                    capability.name,
                    capability.capability_type,
                    capability.description
                )
            )

        except sqlite3.IntegrityError:
            raise HTTPException(
                status_code=409,
                detail="El dispositivo ya tiene registrada esta capacidad"
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
def create_sensor(
    device_id: str,
    sensor: SensorCreate,
    current_user: dict = Depends(
        require_roles("admin", "operator")
    )
):

        # 1. Comprobar que el dispositivo existe
        device = connection.execute(
            """
            SELECT id, device_id
            FROM devices
            WHERE device_id = ?
            """,
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
                device["id"],
                sensor.name,
                sensor.sensor_type,
                sensor.unit,
                sensor.status
            )
        )

        # 3. Recuperar el sensor reciÃ©n creado
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
def get_sensor(
    sensor_id: int,
    current_user: dict = Depends(
        require_roles("admin", "operator", "user")
    )
):
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
def create_device(
    device: DeviceCreate,
    current_user: dict = Depends(
        require_roles("admin")
    )
):
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

        # Generar credencial Ãºnica del dispositivo
        device_secret = secrets.token_urlsafe(32)
        secret_hash = get_password_hash(device_secret)

        # Insertar el nuevo dispositivo
        cursor = connection.execute(
            """
            INSERT INTO devices (
                device_id,
                name,
                device_type,
                status,
                location,
                secret_hash
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                device.device_id,
                device.name,
                device.device_type,
                device.status,
                device.location,
                secret_hash
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

    # Entregar el secreto solamente al crear el dispositivo
    response = dict(new_device)
    response["device_secret"] = device_secret

    return response


@app.post("/devices/{device_id}/credentials")
def rotate_device_credentials(
    device_id: str,
    current_user: dict = Depends(
        require_roles("admin")
    )
):
    # Generar una nueva credencial
    device_secret = secrets.token_urlsafe(32)
    secret_hash = get_password_hash(device_secret)

    with database_connection() as connection:
        device = connection.execute(
            """
            SELECT id
            FROM devices
            WHERE device_id = ?
            """,
            (device_id,)
        ).fetchone()

        if device is None:
            raise HTTPException(
                status_code=404,
                detail="Dispositivo no encontrado"
            )

        connection.execute(
            """
            UPDATE devices
            SET secret_hash = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE device_id = ?
            """,
            (secret_hash, device_id)
        )

    return {
        "device_id": device_id,
        "device_secret": device_secret,
        "message": "Credencial generada correctamente. GuÃ¡rdala de forma segura."
    }


@app.post("/devices/{device_id}/commands", status_code=202)
def create_device_command(
    device_id: str,
    command: CommandCreate,
    request: Request,
    current_user: dict = Depends(
        require_roles("admin", "operator")
    )
):
    current_time = datetime.now(timezone.utc).isoformat()
    command_uuid = str(uuid.uuid4())

    # 1. Verificar que el dispositivo exista y estÃ© conectado
    with database_connection() as connection:
        device = connection.execute(
            """
            SELECT id, device_id, status
            FROM devices
            WHERE device_id = ?
            """,
            (device_id,)
        ).fetchone()

        if device is None:
            raise HTTPException(
                status_code=404,
                detail="Dispositivo no encontrado"
            )

        if device["status"] != "online":
            raise HTTPException(
                status_code=409,
                detail="El dispositivo no estÃ¡ conectado"
            )

        # 2. Registrar el comando en SQLite
        connection.execute(
            """
            INSERT INTO commands (
                device_id,
                command_type,
                payload,
                status,
                created_at,
                command_uuid,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                device["id"],
                command.command_type,
                json.dumps(command.payload),
                "pending",
                current_time,
                command_uuid,
                current_time
            )
        )

    # 3. Obtener el cliente MQTT
    mqtt_client = getattr(
        request.app.state,
        "mqtt_client",
        None
    )

    if mqtt_client is None or not mqtt_client.is_connected():
        with database_connection() as connection:
            connection.execute(
                """
                UPDATE commands
                SET status = 'failed',
                    error_message = ?,
                    updated_at = ?
                WHERE command_uuid = ?
                """,
                (
                    "Cliente MQTT no disponible",
                    current_time,
                    command_uuid
                )
            )

        raise HTTPException(
            status_code=503,
            detail="El servicio MQTT no estÃ¡ disponible"
         )

     # 4. Publicar el comando
    published = publish_command(
        mqtt_client,
        device_id,
        command_uuid,
        command.command_type,
        command.payload
     )

    if not published:
        with database_connection() as connection:
            connection.execute(
                """
                UPDATE commands
                SET status = 'failed',
                    error_message = ?,
                    updated_at = ?
                WHERE command_uuid = ?
                """,
                (
                    "No se pudo publicar el comando MQTT",
                    datetime.now(timezone.utc).isoformat(),
                    command_uuid
                )
            )

        raise HTTPException(
            status_code=503,
            detail="No se pudo publicar el comando"
        )

    # 5. Actualizar el estado del comando
    sent_time = datetime.now(timezone.utc).isoformat()

    with database_connection() as connection:
        connection.execute(
            """
            UPDATE commands
            SET status = 'sent',
                sent_at = ?,
                updated_at = ?
            WHERE command_uuid = ?
            """,
            (sent_time, sent_time, command_uuid)
        )

    return {
        "message": "Comando enviado correctamente",
        "command_uuid": command_uuid,
        "device_id": device_id,
        "command_type": command.command_type,
        "status": "sent"
    }

async def monitor_command_timeouts():

    logger.info("Monitor de timeout y retry iniciado")

    while True:

        current_time = datetime.now(timezone.utc)
        current_time_iso = current_time.isoformat()

        cutoff_time = (
            current_time - timedelta(
                seconds=COMMAND_TIMEOUT_SECONDS
            )
        ).isoformat()

        mqtt_client = getattr(
            app.state, "mqtt_client", None
        )

        with database_connection() as connection:

            expired_commands = connection.execute(
                """
                SELECT
                    c.id,
                    c.command_uuid,
                    c.command_type,
                    c.payload,
                    c.retry_count,
                    d.device_id
                FROM commands AS c
                INNER JOIN devices AS d
                    ON c.device_id = d.id
                WHERE c.status IN ('sent', 'acknowledged')
                  AND COALESCE(c.acknowledged_at, c.sent_at) IS NOT NULL
                  AND julianday(
                      COALESCE(c.acknowledged_at, c.sent_at)
                  ) <= julianday(?)
                """,
                (cutoff_time,)
            ).fetchall()

            for command in expired_commands:

                if command["retry_count"] >= MAX_COMMAND_RETRIES:

                    connection.execute(
                        """
                        UPDATE commands
                        SET status = 'failed',
                            timeout_at = ?,
                            error_message = ?,
                            updated_at = ?
                        WHERE id = ?
                        """,
                        (
                            current_time_iso,
                            "Reintentos mÃ¡ximos agotados",
                            current_time_iso,
                            command["id"]
                        )
                    )

                    continue

                if mqtt_client is None or not mqtt_client.is_connected():
                    logger.warning(
                        "No se puede reintentar: MQTT desconectado"
                    )
                    continue

                payload = json.loads(command["payload"])

                published = publish_command(
                    mqtt_client,
                    command["device_id"],
                    command["command_uuid"],
                    command["command_type"],
                    payload
                )

                if published:

                    connection.execute(
                        """
                        UPDATE commands
                        SET status = 'sent',
                            retry_count = retry_count + 1,
                            sent_at = ?,
                            acknowledged_at = NULL,
                            timeout_at = NULL,
                            error_message = NULL,
                            updated_at = ?
                        WHERE id = ?
                        """,
                        (
                            current_time_iso,
                            current_time_iso,
                            command["id"]
                        )
                    )

                    logger.info(
                        "Reintento %s/%s | Comando: %s",
                        command["retry_count"] + 1,
                        MAX_COMMAND_RETRIES,
                        command["command_uuid"]
                    )

        await asyncio.sleep(
            TIMEOUT_CHECK_INTERVAL_SECONDS
        )

@app.get("/devices/{device_id}/commands")
def get_device_commands(
    device_id: str,
    current_user: dict = Depends(
        require_roles("admin", "operator", "user")
    )
):

    with database_connection() as connection:

        device = connection.execute(
            """
            SELECT id
            FROM devices
            WHERE device_id = ?
            """,
            (device_id,)
        ).fetchone()

        if device is None:
            raise HTTPException(
                status_code=404,
                detail="Dispositivo no encontrado"
            )

        commands = connection.execute(
            """
            SELECT
                command_uuid,
                command_type,
                payload,
                status,
                created_at,
                sent_at,
                acknowledged_at,
                completed_at,
                timeout_at,
                retry_count,
                error_message
            FROM commands
            WHERE device_id = ?
            ORDER BY id DESC
            """,
            (device["id"],)
        ).fetchall()

        return [
            {
                **dict(command),
                "payload": json.loads(command["payload"])
                if command["payload"] else {}
            }
            for command in commands
        ]

@app.get("/commands/{command_uuid}")
def get_command(
    command_uuid: str,
    current_user: dict = Depends(
        require_roles("admin", "operator", "user")
    )
):
    with database_connection() as connection:
        command = connection.execute(
            """
            SELECT
                c.command_uuid,
                d.device_id,
                c.command_type,
                c.payload,
                c.status,
                c.created_at,
                c.sent_at,
                c.acknowledged_at,
                c.completed_at,
                c.timeout_at,
                c.retry_count,
                c.error_message,
                c.updated_at
            FROM commands AS c
            INNER JOIN devices AS d
                ON c.device_id = d.id
            WHERE c.command_uuid = ?
            """,
            (command_uuid,)
        ).fetchone()

        if command is None:
            raise HTTPException(
                status_code=404,
                detail="Comando no encontrado"
            )

        result = dict(command)

        result["payload"] = (
            json.loads(result["payload"])
            if result["payload"]
            else {}
        )

        return result
