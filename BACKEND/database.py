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

        # 1. Preparar las tablas anteriores
        prepare_legacy_tables(connection)

        # 2. Crear las nuevas tablas
        create_tables(connection)

        # 3. Migrar los datos anteriores
        migrate_legacy_data(connection)

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