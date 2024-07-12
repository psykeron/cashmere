from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Protocol
from uuid import uuid4

from pydantic import BaseModel
from typing_extensions import Self


@dataclass
class CashmereMessageMetadata:
    id: str
    name: str
    created_at: str

    @classmethod
    def build_metadata(cls, event_name: str) -> Self:
        return cls(
            id=str(uuid4()),
            name=event_name,
            created_at=datetime.now(timezone.utc).isoformat(),
        )

    def dict(self) -> dict:
        return {"id": self.id, "name": self.name, "created_at": self.created_at}


@dataclass
class CashmereMessage:
    meta: CashmereMessageMetadata
    data: dict

    def to_dict(self) -> dict:
        return {"meta": self.meta.dict(), "data": self.data}

    @staticmethod
    def from_dict(data: dict) -> "CashmereMessage":
        return CashmereMessage(
            meta=CashmereMessageMetadata(**data["meta"]), data=data["data"]
        )


def deserialize_cashmere_event(event: dict) -> CashmereMessage:
    return CashmereMessage(
        meta=CashmereMessageMetadata(**event["meta"]), data=event["data"]
    )


class CashmereSerializableEvent(Protocol):
    @staticmethod
    def get_event_name() -> str: ...

    @classmethod
    def from_event(cls, event: CashmereMessage) -> Self: ...

    def get_message(self) -> CashmereMessage: ...


class BaseSerializableEvent(BaseModel):
    _cashmere_message: Optional[CashmereMessage] = None

    def __init__(
        self, cashmere_message: CashmereMessage | None = None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._cashmere_message = cashmere_message

    @staticmethod
    def get_event_name() -> str:
        raise NotImplementedError()

    def get_message(self) -> CashmereMessage:
        if self._cashmere_message:
            return self._cashmere_message

        return CashmereMessage(
            meta=CashmereMessageMetadata.build_metadata(self.get_event_name()),
            data=self.model_dump(),
        )

    @classmethod
    def from_event(cls, cashmere_message: CashmereMessage) -> "BaseSerializableEvent":
        raise NotImplementedError()


class SampleEventV1(BaseSerializableEvent):
    item_one: str
    item_two: int
    item_three: Optional[str] = None

    @staticmethod
    def get_event_name() -> str:
        return "SampleEventV1"

    @classmethod
    def from_event(cls, cashmere_message: CashmereMessage) -> "SampleEventV1":
        return SampleEventV1(
            cashmere_message=cashmere_message, **cashmere_message.data  # type: ignore
        )
