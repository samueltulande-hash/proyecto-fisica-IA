import os
import sqlite3
import logging

from contextlib import contextmanager


# ==========================================
# CONFIGURACION
# ==========================================

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

DATABASE_PATH = os.getenv(
    "DATABASE_PATH",
    "data/aiot.db"
)


# ==========================================
# CONEXION CON SQLITE
# ==========================================

@contextmanager
def database_connection():

    connection = sqlite3.connect(
        DATABASE_PATH,
        timeout=10
    )

    connection.row_factory = sqlite3.Row

    connection.execute(
        "PRAGMA foreign_keys = ON"
    )

    try:

        yield connection

        connection.commit()

    except Exception:

        connection.rollback()

        raise

    finally:

        connection.close()


# ==========================================
# INICIALIZACION
# ==========================================

def initialize_database():

    database_directory = os.path.dirname(
        os.path.abspath(DATABASE_PATH)
    )

    os.makedirs(
        database_directory,
        exist_ok=True
    )

    with database_connection() as connection:

        connection.execute("PRAGMA journal_mode = WAL")
        prepare_legacy_tables(connection)
        create_tables(connection)
        migrate_legacy_data(connection)
        migrate_device_registry_schema(connection)
        migrate_command_gateway_schema(connection)
        migrate_device_auth_schema(connection)


        
       

    logger.info(
        "Base de datos AIoT-CORE inicializada correctamente"
    )

def prepare_legacy_tables(connection):

    existing_tables = {
        row["name"]
        for row in connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
            """
        ).fetchall()
    }

    # Renombrar primero las mediciones antiguas
    if (
        "measurements" in existing_tables
        and "measurements_legacy" not in existing_tables
    ):
        connection.execute(
            "ALTER TABLE measurements RENAME TO measurements_legacy"
        )

    # Renombrar los dispositivos antiguos
    if (
        "devices" in existing_tables
        and "devices_legacy" not in existing_tables
    ):
        connection.execute(
            "ALTER TABLE devices RENAME TO devices_legacy"
        )

def create_tables(connection):


    connection.execute("""
    CREATE TABLE IF NOT EXISTS mqtt_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        topic TEXT NOT NULL,
        payload TEXT NOT NULL,
        received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
""")

    # 1. DISPOSITIVOS
    connection.execute("""
        CREATE TABLE IF NOT EXISTS devices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            device_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'unknown',
            location TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)

    

    # 2. CAPACIDADES
    connection.execute("""
        CREATE TABLE IF NOT EXISTS capabilities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            capability_type TEXT NOT NULL,
            description TEXT,

            FOREIGN KEY (device_id)
                REFERENCES devices(id)
                ON DELETE CASCADE
        )
    """)

    # 3. SENSORES
    connection.execute("""
        CREATE TABLE IF NOT EXISTS sensors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            sensor_type TEXT NOT NULL,
            unit TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'unknown',

            FOREIGN KEY (device_id)
                REFERENCES devices(id)
                ON DELETE CASCADE
        )
    """)

    # 4. MEDICIONES
    connection.execute("""
    CREATE TABLE IF NOT EXISTS measurements (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sensor_id INTEGER NOT NULL,
        value REAL NOT NULL,
        measured_at TEXT NOT NULL,
        received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        source_topic TEXT,

        FOREIGN KEY (sensor_id)
            REFERENCES sensors(id)
            ON DELETE CASCADE
    )
""")

    # 5. ACTUADORES
    connection.execute("""
        CREATE TABLE IF NOT EXISTS actuators (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            actuator_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'unknown',

            FOREIGN KEY (device_id)
                REFERENCES devices(id)
                ON DELETE CASCADE
        )
    """)

    # 6. CAMARAS
    connection.execute("""
        CREATE TABLE IF NOT EXISTS cameras (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            camera_type TEXT NOT NULL,
            stream_url TEXT,
            status TEXT NOT NULL DEFAULT 'unknown',

            FOREIGN KEY (device_id)
                REFERENCES devices(id)
                ON DELETE CASCADE
        )
    """)

    # 7. USUARIOS
    connection.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'user',
            is_active INTEGER NOT NULL DEFAULT 1
                CHECK (is_active IN (0, 1)),
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 8. COMANDOS
    connection.execute("""
        CREATE TABLE IF NOT EXISTS commands (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id INTEGER NOT NULL,
            actuator_id INTEGER,
            user_id INTEGER,
            command_type TEXT NOT NULL,
            payload TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            executed_at TEXT,

            FOREIGN KEY (device_id)
                REFERENCES devices(id),

            FOREIGN KEY (actuator_id)
                REFERENCES actuators(id)
                ON DELETE SET NULL,

            FOREIGN KEY (user_id)
                REFERENCES users(id)
                ON DELETE SET NULL
        )
    """)

    # 9. EVENTOS
    connection.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id INTEGER,
            event_type TEXT NOT NULL,
            description TEXT NOT NULL,
            severity TEXT NOT NULL DEFAULT 'info',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

            FOREIGN KEY (device_id)
                REFERENCES devices(id)
                ON DELETE SET NULL
        )
    """)

    # INDICES PARA MEJORAR CONSULTAS
    connection.execute("""
        CREATE INDEX IF NOT EXISTS idx_sensors_device
        ON sensors(device_id)
    """)

    connection.execute("""
        CREATE INDEX IF NOT EXISTS idx_measurements_sensor
        ON measurements(sensor_id)
    """)

    connection.execute("""
        CREATE INDEX IF NOT EXISTS idx_commands_device
        ON commands(device_id)
    """)

    connection.execute("""
        CREATE INDEX IF NOT EXISTS idx_events_device
        ON events(device_id)
    """)

    # CONTROL DE VERSIONES DEL ESQUEMA
    connection.execute("""
    CREATE TABLE IF NOT EXISTS schema_migrations (
        version INTEGER PRIMARY KEY,
        description TEXT NOT NULL,
        applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """)

    logger.info("Tablas e indices creados correctamente")

    connection.execute(
    """
    CREATE UNIQUE INDEX IF NOT EXISTS
    idx_capabilities_device_type
    ON capabilities (device_id, capability_type)
    """
)

def migrate_legacy_data(connection):

    # Verificar si la migración ya fue ejecutada
    migration = connection.execute(
        """
        SELECT version
        FROM schema_migrations
        WHERE version = 1
        """
    ).fetchone()

    if migration:
        logger.info("Migracion inicial ya aplicada")
        return

    # Consultar dispositivos antiguos
    devices = connection.execute(
        "SELECT * FROM devices_legacy"
    ).fetchall()

    for old_device in devices:

        old_device_id = old_device["device_id"]

        # Crear dispositivo nuevo
        connection.execute(
            """
            INSERT INTO devices (
                device_id,
                name,
                device_type,
                status,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                old_device_id,
                old_device_id,
                "sensor_node",
                "unknown",
                old_device["first_seen"],
                old_device["last_seen"]
            )
        )

        # Obtener identificador interno
        device = connection.execute(
            """
            SELECT id
            FROM devices
            WHERE device_id = ?
            """,
            (old_device_id,)
        ).fetchone()

        new_device_id = device["id"]

        # Crear sensores
        sensor_definitions = [
            ("Sensor de temperatura", "temperature", "°C"),
            ("Sensor de humedad", "humidity", "%")
        ]

        sensor_ids = {}

        for name, sensor_type, unit in sensor_definitions:

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
                    new_device_id,
                    name,
                    sensor_type,
                    unit,
                    "unknown"
                )
            )

            sensor_ids[sensor_type] = cursor.lastrowid

        # Consultar mediciones antiguas
        old_measurements = connection.execute(
            """
            SELECT *
            FROM measurements_legacy
            WHERE device_id = ?
            """,
            (old_device_id,)
        ).fetchall()

        for old_measurement in old_measurements:

            # Guardar temperatura
            connection.execute(
                """
                INSERT INTO measurements (
                    sensor_id,
                    value,
                    measured_at,
                    received_at,
                    source_topic
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    sensor_ids["temperature"],
                    old_measurement["temperature"],
                    old_measurement["measured_at"],
                    old_measurement["received_at"],
                    old_measurement["topic"]
                )
            )

            # Guardar humedad
            connection.execute(
                """
                INSERT INTO measurements (
                    sensor_id,
                    value,
                    measured_at,
                    received_at,
                    source_topic
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    sensor_ids["humidity"],
                    old_measurement["humidity"],
                    old_measurement["measured_at"],
                    old_measurement["received_at"],
                    old_measurement["topic"]
                )
            )

    # Registrar migración completada
    connection.execute(
        """
        INSERT INTO schema_migrations (
            version,
            description
        )
        VALUES (?, ?)
        """,
        (
            1,
            "Migracion inicial al modelo relacional SQLite"
        )
    )

    logger.info(
        "Migracion de dispositivos y mediciones completada"
    )

    
def migrate_device_registry_schema(connection):

    # Consultar las columnas actuales de devices
    existing_columns = {
        row["name"]
        for row in connection.execute(
            "PRAGMA table_info(devices)"
        ).fetchall()
    }

    # Agregar version del firmware
    if "firmware_version" not in existing_columns:
        connection.execute("""
            ALTER TABLE devices
            ADD COLUMN firmware_version TEXT
        """)

    # Agregar direccion IP
    if "ip" not in existing_columns:
        connection.execute("""
            ALTER TABLE devices
            ADD COLUMN ip TEXT
        """)

    # Agregar ultima comunicacion
    if "last_seen" not in existing_columns:
        connection.execute("""
            ALTER TABLE devices
            ADD COLUMN last_seen TEXT
        """)

    # Registrar migracion version 2
    connection.execute("""
        INSERT OR IGNORE INTO schema_migrations (
            version,
            description
        )
        VALUES (?, ?)
    """, (
        2,
        "Agregar campos para Device Registry"
    ))

    logger.info(
        "Migracion Device Registry aplicada correctamente"
    )
    # ==========================================
# MIGRACION 3 - COMMAND GATEWAY
# ==========================================

def migrate_command_gateway_schema(connection):

    # Consultar columnas actuales de commands
    existing_columns = {
        row["name"]
        for row in connection.execute(
            "PRAGMA table_info(commands)"
        ).fetchall()
    }

    # Identificador unico para correlacionar comandos MQTT
    if "command_uuid" not in existing_columns:
        connection.execute("""
            ALTER TABLE commands
            ADD COLUMN command_uuid TEXT
        """)

    # Fecha de envio del comando
    if "sent_at" not in existing_columns:
        connection.execute("""
            ALTER TABLE commands
            ADD COLUMN sent_at TEXT
        """)

    # Fecha de confirmacion del dispositivo
    if "acknowledged_at" not in existing_columns:
        connection.execute("""
            ALTER TABLE commands
            ADD COLUMN acknowledged_at TEXT
        """)

    # Fecha de finalizacion
    if "completed_at" not in existing_columns:
        connection.execute("""
            ALTER TABLE commands
            ADD COLUMN completed_at TEXT
        """)

    # Fecha limite de espera
    if "timeout_at" not in existing_columns:
        connection.execute("""
            ALTER TABLE commands
            ADD COLUMN timeout_at TEXT
        """)

    # Numero de reintentos realizados
    if "retry_count" not in existing_columns:
        connection.execute("""
            ALTER TABLE commands
            ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0
        """)

    # Descripcion del error, si ocurre
    if "error_message" not in existing_columns:
        connection.execute("""
            ALTER TABLE commands
            ADD COLUMN error_message TEXT
        """)

    # Ultima actualizacion del comando
    if "updated_at" not in existing_columns:
        connection.execute("""
            ALTER TABLE commands
            ADD COLUMN updated_at TEXT
        """)

    # Asignar identificadores a comandos antiguos
    import uuid

    old_commands = connection.execute("""
        SELECT id
        FROM commands
        WHERE command_uuid IS NULL
    """).fetchall()

    for command in old_commands:
        connection.execute(
            """
            UPDATE commands
            SET command_uuid = ?
            WHERE id = ?
            """,
            (
                str(uuid.uuid4()),
                command["id"]
            )
        )

    # Garantizar que no existan identificadores duplicados
    connection.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS
        idx_commands_uuid
        ON commands(command_uuid)
    """)

    # Registrar migracion version 3
    connection.execute("""
        INSERT OR IGNORE INTO schema_migrations (
            version,
            description
        )
        VALUES (?, ?)
    """, (
        3,
        "Agregar campos para Command Gateway"
    ))

    logger.info(
        "Migracion Command Gateway aplicada correctamente"
    )

# ==========================================
# MIGRACION 4 - AUTENTICACION DE DISPOSITIVOS
# ==========================================

def migrate_device_auth_schema(connection):

    existing_columns = {
        row["name"]
        for row in connection.execute(
            "PRAGMA table_info(devices)"
        ).fetchall()
    }

    if "secret_hash" not in existing_columns:
        connection.execute("""
            ALTER TABLE devices
            ADD COLUMN secret_hash TEXT
        """)

    connection.execute("""
        INSERT OR IGNORE INTO schema_migrations (
            version,
            description
        )
        VALUES (?, ?)
    """, (
        4,
        "Agregar autenticacion individual de dispositivos"
    ))

    logger.info(
        "Migracion de autenticacion de dispositivos aplicada"
    )
