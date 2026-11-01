
import pytest

from BACKEND.schemas import SensorMeasurement


def test_mensaje_valido():
    mensaje = {
        "device_id": "sensor_001",
        "temperature": 25.4,
        "humidity": 62.3,
        "timestamp": "2026-11-01T18:45:08Z"
    }

    medicion = SensorMeasurement.model_validate(mensaje)

    assert medicion.device_id == "sensor_001"
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
        "temperature": 25.4,
        "humidity": 62.3,
        "timestamp": "2026-11-01T18:45:08"
    }

    with pytest.raises(ValueError):
        SensorMeasurement.model_validate(mensaje)
