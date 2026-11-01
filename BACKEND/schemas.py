from datetime import datetime

from pydantic import BaseModel, Field, ConfigDict, field_validator


class SensorMeasurement(BaseModel):

    model_config = ConfigDict(extra="forbid")

    device_id: str = Field(min_length=1, max_length=100)

    temperature: float = Field(ge=-40, le=85)

    humidity: float = Field(ge=0, le=100)

    timestamp: datetime

    @field_validator("device_id")
    @classmethod
    def validate_device_id(cls, value: str) -> str:
        value = value.strip()

        if not value:
            raise ValueError("device_id no puede estar vacío")

        return value

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, value: datetime) -> datetime:

        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp debe incluir zona horaria")

        return value

class DeviceCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    device_id: str = Field(min_length=1, max_length=100)
    name: str = Field(min_length=1, max_length=150)
    device_type: str = Field(min_length=1, max_length=50)
    status: str = Field(default="unknown", max_length=30)
    location: str | None = Field(default=None, max_length=200)

    @field_validator("device_id", "name", "device_type", "status")
    @classmethod
    def validate_text_fields(cls, value: str) -> str:
        value = value.strip()

        if not value:
            raise ValueError("Este campo no puede estar vacío")

        return value

    @field_validator("location")
    @classmethod
    def validate_location(cls, value: str | None) -> str | None:
        if value is not None:
            value = value.strip()

            if not value:
                return None

        return value

class CapabilityCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=100)
    capability_type: str = Field(min_length=1, max_length=50)
    description: str | None = Field(default=None, max_length=250)

    @field_validator("name", "capability_type")
    @classmethod
    def validate_text_fields(cls, value: str) -> str:
        value = value.strip()

        if not value:
            raise ValueError("Este campo no puede estar vacío")

        return value

    @field_validator("description")
    @classmethod
    def validate_description(cls, value: str | None) -> str | None:
        if value is not None:
            value = value.strip()

            if not value:
                return None

        return value

class SensorCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=100)
    sensor_type: str = Field(min_length=1, max_length=50)
    unit: str = Field(min_length=1, max_length=20)
    status: str = Field(default="unknown", max_length=30)

    @field_validator("name", "sensor_type", "unit", "status")
    @classmethod
    def validate_text_fields(cls, value: str) -> str:
        value = value.strip()

        if not value:
            raise ValueError("Este campo no puede estar vacío")

        return value