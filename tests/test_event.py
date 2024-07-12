import json
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from src.event import CashmereMessage, CashmereMessageMetadata, SampleEventV1


class TestValidCashmereMessageToSerializableEvent:
    def get_test_event(self) -> SampleEventV1:
        cashmere_message = CashmereMessage(
            meta=CashmereMessageMetadata(
                id="1234", name="SampleEventV1", created_at="2021-01-01T00:00:00.000Z"
            ),
            data={"item_one": "hello", "item_two": 1234},
        )

        return SampleEventV1.from_event(cashmere_message)

    def test_id_is_correct(self):
        event = self.get_test_event()
        assert event.get_message().meta.id == "1234"

    def test_name_is_correct(self):
        event = self.get_test_event()
        assert event.get_message().meta.name == "SampleEventV1"

    def test_created_at_is_correct(self):
        event = self.get_test_event()
        assert event.get_message().meta.created_at == "2021-01-01T00:00:00.000Z"

    def test_required_data_is_correct(self):
        event = self.get_test_event()
        assert event.item_one == "hello"
        assert event.item_two == 1234

    def test_optional_data_is_correct(self):
        event = self.get_test_event()
        assert event.item_three is None


class TestInvalidDataCashmereMessageToSerializableEvent:
    def test_conversion_raises_exception(self):
        cashmere_message = CashmereMessage(
            meta=CashmereMessageMetadata(
                id="1234", name="SampleEventV1", created_at="2021-01-01T00:00:00.000Z"
            ),
            data={"item_one": "hello"},
        )

        with pytest.raises(
            ValidationError,
            match="1 validation error for SampleEventV1\nitem_two\n  Field required",
        ):
            SampleEventV1.from_event(cashmere_message)


class TestSerializableEventToCashmereMessage:
    def get_cashmere_message(self) -> CashmereMessage:
        sample_event = SampleEventV1(
            item_one="hello",
            item_two=1234,
        )

        return sample_event.get_message()

    def test_id_is_correct(self):
        event = self.get_cashmere_message()
        assert event.meta.id

    def test_name_is_correct(self):
        event = self.get_cashmere_message()
        assert event.meta.name == "SampleEventV1"

    def test_created_at_is_correct(self):
        event = self.get_cashmere_message()
        assert event.meta.created_at
        assert (
            datetime.now(timezone.utc).timestamp()
            - datetime.fromisoformat(event.meta.created_at).timestamp()
        ) < 1  # seconds

    def test_required_data_is_correct(self):
        event = self.get_cashmere_message()
        assert event.data["item_one"] == "hello"
        assert event.data["item_two"] == 1234

    def test_optional_data_is_correct(self):
        event = self.get_cashmere_message()
        assert event.data["item_three"] is None


class TestSerializingCashmereMessage:
    def test_serializes_correctly(self):
        cashmere_message = CashmereMessage(
            meta=CashmereMessageMetadata(
                id="1234", name="SampleEventV1", created_at="2021-01-01T00:00:00.000Z"
            ),
            data={"item_one": "hello", "item_two": 1234},
        )

        serialized_response = json.dumps(cashmere_message.to_dict())
        unserialized_response = json.loads(serialized_response)
        assert unserialized_response == {
            "meta": {
                "id": "1234",
                "name": "SampleEventV1",
                "created_at": "2021-01-01T00:00:00.000Z",
            },
            "data": {"item_one": "hello", "item_two": 1234},
        }
