import pytest

from src.utils_aws import (
    translate_event_name_to_sns_topic_name,
    translate_queue_name_to_sqs_queue_name,
)


class TestTranslateEventName:
    def test_periods_are_replaced_with_underscores(self):
        assert translate_event_name_to_sns_topic_name("event.name") == "event_name"

    def test_exception_raised_when_name_too_short(self):
        event_name = ""
        with pytest.raises(
            ValueError,
            match=f"Topic name {event_name} is too short. It must be 1 to 80 characters.",
        ):
            translate_event_name_to_sns_topic_name(event_name)

    def test_exception_raised_when_name_too_long(self):
        event_name = "a" * 81
        with pytest.raises(
            ValueError,
            match=f"Topic name {event_name} is too long. It must be 1 to 80 characters.",
        ):
            translate_event_name_to_sns_topic_name(event_name)

    def test_fifo_is_appended_to_name(self):
        event_name = "event.name"
        assert (
            translate_event_name_to_sns_topic_name(event_name, is_fifo=True)
            == "event_name.fifo"
        )


class TestTranslateQueueName:
    def test_periods_are_replaced_with_underscores(self):
        assert translate_queue_name_to_sqs_queue_name("queue.name") == "queue_name"

    def test_exception_raised_when_name_too_short(self):
        queue_name = ""
        with pytest.raises(
            ValueError,
            match=f"Queue name {queue_name} is too short. It must be 1 to 80 characters.",
        ):
            translate_queue_name_to_sqs_queue_name(queue_name)

    def test_exception_raised_when_name_too_long(self):
        queue_name = "a" * 81
        with pytest.raises(
            ValueError,
            match=f"Queue name {queue_name} is too long. It must be 1 to 80 characters.",
        ):
            translate_queue_name_to_sqs_queue_name(queue_name)

    def test_fifo_is_appended_to_name(self):
        queue_name = "queue.name"
        assert (
            translate_queue_name_to_sqs_queue_name(queue_name, is_fifo=True)
            == "queue_name.fifo"
        )

    def test_error_is_appended_to_name(self):
        queue_name = "queue.name"
        assert (
            translate_queue_name_to_sqs_queue_name(queue_name, is_deadletter=True)
            == "queue_name-error"
        )

    def test_fifo_and_error_is_appended_to_name(self):
        queue_name = "queue.name"
        assert (
            translate_queue_name_to_sqs_queue_name(
                queue_name, is_fifo=True, is_deadletter=True
            )
            == "queue_name-error.fifo"
        )
