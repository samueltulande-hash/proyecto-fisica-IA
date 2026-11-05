
import pytest
from BACKEND.schemas import SensorMeasurement, UserCreate


def test_mensaje_valido():
    mensaje = {
        "device_id": "sensor_001",
        "device_secret": "A" * 43,
        "temperature": 25.4,
        "humidity": 62.3,
        "timestamp": "2026-11-01T18:45:08Z"
    }

    medicion = SensorMeasurement.model_validate(mensaje)

    assert medicion.temperature == 25.4
    assert medicion.humidity == 62.3


def test_humedad_fuera_de_rango():
    mensaje = {
        "device_id": "sensor_001",
        "temperature": 25.4,
        "humidity": 150,
        "timestamp": "2026-11-01T18:45:08Z"
    }

    with pytest.raises(ValueError):
        SensorMeasurement.model_validate(mensaje)


def test_temperatura_fuera_de_rango():
    mensaje = {
        "device_id": "sensor_001",
        "temperature": 100,
        "humidity": 62.3,
        "timestamp": "2026-11-01T18:45:08Z"
    }

    with pytest.raises(ValueError):
        SensorMeasurement.model_validate(mensaje)


def test_fecha_sin_zona_horaria():
    mensaje = {
        "device_id": "sensor_001",
        "device_secret": "A" * 43,
        "temperature": 25.4,
        "humidity": 62.3,
        "timestamp": "2026-11-01T18:45:08"
    }

    with pytest.raises(ValueError):
        SensorMeasurement.model_validate(mensaje)

    with pytest.raises(ValueError):
        SensorMeasurement.model_validate(mensaje)
# ==========================================
# PRUEBAS RBAC - SPRINT 6
# ==========================================

@pytest.mark.parametrize(
    "role",
    ["viewer", "operator", "engineer", "admin"]
)
def test_roles_permitidos(role):
    usuario = UserCreate(
        username="usuario_test",
        email="test@example.com",
        password="Password123",
        role=role
    )

    assert usuario.role == role


def test_rol_invalido_rechazado():
    with pytest.raises(ValueError):
        UserCreate(
            username="usuario_test",
            email="test@example.com",
            password="Password123",
            role="superadmin"
        )


def test_rol_por_defecto_viewer():
    usuario = UserCreate(
        username="usuario_test",
        email="test@example.com",
        password="Password123"
    )

    assert usuario.role == "viewer"
