from datetime import datetime
from ipaddress import ip_address
from typing import Literal
from pydantic import BaseModel, Field, ConfigDict, field_validator

class SensorMeasurement(BaseModel):

    model_config = ConfigDict(extra="forbid")

    device_id: str = Field(min_length=1, max_length=100)

    device_secret: str = Field(min_length=32, max_length=128)

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

    # ==========================================
# DEVICE REGISTRY
# ==========================================

class DeviceRegister(BaseModel):

    model_config = ConfigDict(extra="forbid")

    device_id: str = Field(min_length=1, max_length=100)

    device_secret: str = Field(min_length=32, max_length=128)

    device_type: str = Field(min_length=1, max_length=50)

    firmware_version: str = Field(min_length=1, max_length=50)

    device_secret: str = Field(min_length=32, max_length=128)


    ip: str

    capabilities: list[CapabilityCreate] = Field(
        default_factory=list,
        max_length=30
    )

    @field_validator(
        "device_id",
        "device_type",
        "firmware_version"
    )
    @classmethod
    def validate_text_fields(cls, value: str) -> str:

        value = value.strip()

        if not value:
            raise ValueError("Este campo no puede estar vacio")

        return value

    @field_validator("ip")
    @classmethod
    def validate_ip(cls, value: str) -> str:

        value = value.strip()

        try:
            return str(ip_address(value))
        except ValueError:
            raise ValueError("La direccion IP no es valida")

        
# ==========================================
# DEVICE REGISTRY
# ==========================================


class DeviceStatus(BaseModel):

    model_config = ConfigDict(extra="forbid")

    device_id: str = Field(min_length=1, max_length=100)

    device_secret: str = Field(min_length=32, max_length=128)

    status: Literal["online", "offline"]

    @field_validator("device_id")
    @classmethod
    def validate_device_id(cls, value: str) -> str:

        value = value.strip()

        if not value:
            raise ValueError("device_id no puede estar vacio")

        return value
    # ==========================================
# COMMAND GATEWAY - SPRINT 5
# ==========================================

class CommandCreate(BaseModel):

    model_config = ConfigDict(extra="forbid")

    command_type: Literal[
        "get_status",
        "read_sensor",
        "ping"
    ]

    payload: dict = Field(default_factory=dict, max_length=10)


class CommandResponse(BaseModel):

    model_config = ConfigDict(extra="forbid")

    command_uuid: str = Field(min_length=1, max_length=36)

    device_id: str = Field(min_length=1, max_length=100)

    device_secret: str = Field(min_length=32, max_length=128)

    status: Literal[
        "received",
        "completed",
        "failed"
    ]

    message: str | None = Field(default=None, max_length=250)


class CommandError(BaseModel):

    model_config = ConfigDict(extra="forbid")

    command_uuid: str = Field(min_length=1, max_length=36)

    device_id: str = Field(min_length=1, max_length=100)

    device_secret: str = Field(min_length=32, max_length=128)

    error: str = Field(min_length=1, max_length=250)
    # ==========================================
# AI ASSISTANT - OLLAMA
# ==========================================

class AssistantChatRequest(BaseModel):

    model_config = ConfigDict(extra="forbid")

    message: str = Field(min_length=1, max_length=2000)

    device_id: str | None = Field(
        default=None,
        max_length=100
    )


class AssistantChatResponse(BaseModel):

    model_config = ConfigDict(extra="forbid")

    response: str

    device_id: str | None = None

    command_uuid: str | None = None

    status: str = "completed"

    # ==========================================
# AUTENTICACION JWT - SPRINT 6
# ==========================================

class UserCreate(BaseModel):

    model_config = ConfigDict(extra="forbid")

    username: str = Field(min_length=3, max_length=50)
    email: str = Field(min_length=5, max_length=150)
    password: str = Field(min_length=8, max_length=128)


class Token(BaseModel):

    model_config = ConfigDict(extra="forbid")

    access_token: str
    token_type: str = "bearer"


class UserResponse(BaseModel):

    model_config = ConfigDict(extra="forbid")

    id: int
    username: str
    email: str
    role: str
    is_active: bool