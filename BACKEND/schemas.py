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