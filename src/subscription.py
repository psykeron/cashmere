import base64
import hashlib
from typing import Any, Callable, Type

from .event import BaseSerializableEvent
from .exception import CashmereException

DEFAULT_SUBSCRIPTION_TIMEOUT_SECONDS = 60 * 10


class CashmereSubscriptionException(CashmereException): ...


class CashmereSubscription:
    def __init__(
        self,
        domain: str,
        event: Type[BaseSerializableEvent],
        handler: Callable,
        timeout_seconds: int = 60 * 10,
    ):
        if not domain:
            raise CashmereSubscriptionException("Domain cannot be empty")

        if not event:
            raise CashmereSubscriptionException("Event cannot be empty")

        if handler is None:
            raise CashmereSubscriptionException("Handler cannot be empty")

        self.domain = domain
        self.event = event
        self.handler = handler
        self.timeout_seconds = timeout_seconds

    def __hash__(self) -> int:
        domain = self.domain
        event_name = self.event.get_event_name()
        handler_qual_name = self.handler.__name__
        return hash(f"{domain}:{event_name}:{handler_qual_name}")

    def __eq__(self, other: Any) -> bool:
        if not other or not isinstance(other, CashmereSubscription):
            return False

        return (
            self.domain == other.domain
            and self.event.get_event_name() == other.event.get_event_name()
            and self.handler.__name__ == other.handler.__name__
        )

    def get_qualified_short_name(self) -> str:
        # We do all this to create a short enough name that doesn't
        # exceed the 80 character limit for SQS queue names.

        prefix_md5_bytes = hashlib.md5(
            f"{self.domain}{self.event.get_event_name()}".encode(),
            usedforsecurity=False,
        ).digest()

        base64_hash = base64.b64encode(prefix_md5_bytes).decode("utf-8")

        trimmed_hash = base64_hash.rstrip("=")

        return f"{trimmed_hash}.{self.handler.__name__}"

    def get_qualified_long_name(self) -> str:
        return f"{self.domain}.{self.event.get_event_name()}.{self.handler.__name__}"

    def get_event_name(self) -> str:
        return self.event.get_event_name()
