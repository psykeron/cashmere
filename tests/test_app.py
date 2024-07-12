import json
from typing import Dict

import pytest

from src.app import (
    Cashmere,
    CashmereException,
    CashmereSubscription,
    CashmereSubscriptionException,
)
from src.dependencies import AsIsDependency
from src.event import (
    BaseSerializableEvent,
    CashmereMessage,
    CashmereMessageMetadata,
    SampleEventV1,
)
from src.threader import QueueResourceHolder, ThreadingResult


class TestValidCashmereSubscription:
    def _test_handler(self, event):
        pass

    def _get_test_subscription(self) -> CashmereSubscription:
        return CashmereSubscription(
            domain="demo", event=SampleEventV1, handler=self._test_handler
        )

    def test_get_qualified_short_name(self):
        subscription = self._get_test_subscription()
        assert (
            subscription.get_qualified_short_name()
            == "kNsLGAXzNa1E3P01LDDqyw._test_handler"
        )

    def test_get_qualified_long_name(self):
        subscription = self._get_test_subscription()
        assert (
            subscription.get_qualified_long_name() == "demo.SampleEventV1._test_handler"
        )

    def test_get_event_name(self):
        subscription = self._get_test_subscription()
        assert subscription.get_event_name() == "SampleEventV1"


class TestInvalidCashmereSubscriptions:
    def test_should_throw_exception_if_domain_is_empty(self):
        with pytest.raises(
            CashmereSubscriptionException, match="Domain cannot be empty"
        ):
            CashmereSubscription(domain="", event=SampleEventV1, handler=lambda x: x)

    def test_should_throw_exception_if_event_is_empty(self):
        with pytest.raises(
            CashmereSubscriptionException, match="Event cannot be empty"
        ):
            CashmereSubscription(domain="demo", event=None, handler=lambda x: x)

    def test_should_throw_exception_if_handler_is_empty(self):
        with pytest.raises(
            CashmereSubscriptionException, match="Handler cannot be empty"
        ):
            CashmereSubscription(domain="demo", event=SampleEventV1, handler=None)


class TestMinimalCashmereAppInitialization:
    def _get_minimal_test_app(self) -> Cashmere:
        return Cashmere(
            domain="demo",
            namespace_prefix=None,
            cashmere_emitter=None,
            cashmere_consumer=None,
            cashmere_threader=None,
            exported_events=None,
        )

    def test_app_domain_is_filled(self):
        app = self._get_minimal_test_app()
        assert app.domain == "demo"

    def test_app_queue_prefix_is_none(self):
        app = self._get_minimal_test_app()
        assert app.namespace_prefix is None

    def test_app_cashmere_emit_client_is_none(self):
        app = self._get_minimal_test_app()
        assert app.cashmere_emitter is None

    def test_app_cashmere_consume_client_is_none(self):
        app = self._get_minimal_test_app()
        assert app.cashmere_consumer is None

    def test_app_dependencies_is_empty(self):
        app = self._get_minimal_test_app()
        assert app.dependencies == []


class TestFullCashmereAppInitialization:
    class StubEmitClient:
        async def emit(
            self,
            message: str,
            target_resource_name: str,
            deduplication_id: str | None = None,
            group_id: str | None = None,
        ) -> None: ...

    class StubConsumeClient:
        async def consume(
            self,
            subscriptions: Dict[QueueResourceHolder, CashmereSubscription],
        ) -> None: ...

    class StubCashmereThreader:
        async def thread_the_loom(
            self,
            queue_names: list[str],
            event_topic_names: list[str],
            queue_names_to_event_topics_names: dict[str, str],
        ) -> ThreadingResult:
            return ThreadingResult(
                topic_name_to_arn={},
                queue_name_to_resource_holder={},
            )

    class StubEvent(BaseSerializableEvent):
        @staticmethod
        def get_event_name() -> str:
            return "StubEvent"

    def _get_app(self) -> Cashmere:
        return Cashmere(
            domain="demo",
            namespace_prefix="test",
            cashmere_emitter=self.StubEmitClient(),
            cashmere_consumer=self.StubConsumeClient(),
            cashmere_threader=self.StubCashmereThreader(),
            exported_events=[self.StubEvent],
            dependencies=[AsIsDependency(name="test", dep="test")],
        )

    def test_app_domain_is_filled(self):
        app = self._get_app()
        assert app.domain == "demo"

    def test_app_queue_prefix_is_filled(self):
        app = self._get_app()
        assert app.namespace_prefix == "test"

    def test_app_cashmere_emit_client_is_filled(self):
        app = self._get_app()
        assert isinstance(app.cashmere_emitter, self.StubEmitClient)

    def test_app_cashmere_consume_client_is_filled(self):
        app = self._get_app()
        assert isinstance(app.cashmere_consumer, self.StubConsumeClient)

    def test_app_cashmere_thread_is_filled(self):
        app = self._get_app()
        assert isinstance(app.cashmere_threader, self.StubCashmereThreader)

    def test_app_event_types_is_filled(self):
        app = self._get_app()
        assert app.exported_events == [self.StubEvent]

    def test_app_dependencies_is_filled(self):
        app = self._get_app()
        assert app.dependencies == [AsIsDependency(name="test", dep="test")]


class TestCashmereAppSubscribeDecorator:
    def _get_app(self) -> Cashmere:
        return Cashmere(
            domain="demo",
            namespace_prefix="test",
            cashmere_emitter=None,
            cashmere_consumer=None,
            cashmere_threader=None,
            exported_events=None,
        )

    def test_should_add_subscription_to_app(self):
        app = self._get_app()

        @app.subscribe(SampleEventV1)
        async def handler(event: SampleEventV1):
            pass

        assert app.subscriptions == set(
            [CashmereSubscription(domain="demo", event=SampleEventV1, handler=handler)]
        )

    def test_should_add_subscription_to_app_with_timeout(self):
        app = self._get_app()

        @app.subscribe(SampleEventV1, timeout_seconds=60)
        async def handler(event: SampleEventV1):
            pass

        assert app.subscriptions == set(
            [CashmereSubscription(domain="demo", event=SampleEventV1, handler=handler)]
        )
        assert app.subscriptions.pop().timeout_seconds == 60

    def test_subscription_should_have_correct_qualified_short_name(self):
        app = self._get_app()

        @app.subscribe(SampleEventV1)
        async def handler(event: SampleEventV1):
            pass

        assert (
            app.subscriptions.pop().get_qualified_short_name()
            == "kNsLGAXzNa1E3P01LDDqyw.handler"
        )

    def test_subscription_should_have_correct_qualified_long_name(self):
        app = self._get_app()

        @app.subscribe(SampleEventV1)
        async def handler(event: SampleEventV1):
            pass

        assert (
            app.subscriptions.pop().get_qualified_long_name()
            == "demo.SampleEventV1.handler"
        )

    def test_should_raise_exception_when_duplicates_are_added(self):
        app = self._get_app()

        @app.subscribe(SampleEventV1)
        async def a_test_handler(event: SampleEventV1):
            pass

        with pytest.raises(
            CashmereSubscriptionException,
            match="Handler for event kNsLGAXzNa1E3P01LDDqyw.a_test_handler already exists.",
        ):

            @app.subscribe(SampleEventV1)
            async def a_test_handler(event: SampleEventV1):  # noqa: F811
                pass

    def test_should_allow_multiple_handlers_for_same_event(self):
        app = self._get_app()

        @app.subscribe(SampleEventV1)
        async def a_test_handler(event: SampleEventV1):
            pass

        @app.subscribe(SampleEventV1)
        async def another_test_handler(event: SampleEventV1):
            pass

        assert app.subscriptions == set(
            [
                CashmereSubscription(
                    domain="demo", event=SampleEventV1, handler=a_test_handler
                ),
                CashmereSubscription(
                    domain="demo", event=SampleEventV1, handler=another_test_handler
                ),
            ]
        )


class TestCashmereAppSubscribeDecoratorDependenciesInjection:
    def _get_app(self) -> Cashmere:
        return Cashmere(
            domain="demo",
            namespace_prefix="test",
            cashmere_emitter=None,
            cashmere_consumer=None,
            cashmere_threader=None,
            exported_events=None,
            dependencies=[
                AsIsDependency(name="test", dep="test"),
                AsIsDependency(name="something_else", dep=123),
                AsIsDependency(name="another", dep="another"),
            ],
        )

    async def test_should_inject_dependencies_into_handler(self):
        app = self._get_app()

        sample_event = SampleEventV1(item_one="a", item_two=1)

        @app.subscribe(SampleEventV1)
        async def handler(event: SampleEventV1, test: str, something_else: int):
            assert test == "test"
            assert something_else == 123
            assert event.item_one == "a"
            assert event.item_two == 1

        await app.subscriptions.pop().handler(sample_event.get_message())


class TestCashmereAppThreadTheLoom:
    class StubCashmereThreader:
        async def thread_the_loom(
            self,
            queue_names: list[str],
            event_topic_names: list[str],
            queue_names_to_event_topics_names: dict[str, str],
        ) -> None:
            self.called_thread_the_loom = True

            self.called_thread_the_loom_args = {
                "queue_names": queue_names,
                "event_topic_names": event_topic_names,
                "queue_names_to_event_topics_names": queue_names_to_event_topics_names,
            }

    async def test_raises_exception_when_threader_client_is_none(self):
        with pytest.raises(CashmereException, match="cashmere_threader is required"):
            app = Cashmere(
                domain="demo",
                namespace_prefix="test",
                cashmere_emitter=None,
                cashmere_consumer=None,
                cashmere_threader=None,
                exported_events=[],
            )
            await app.thread_the_loom()

    async def test_thread_the_loom_is_called_with_the_right_args_in_empty_state(self):
        app = Cashmere(
            domain="demo",
            namespace_prefix="test",
            cashmere_emitter=None,
            cashmere_consumer=None,
            cashmere_threader=self.StubCashmereThreader(),
            exported_events=None,
        )

        await app.thread_the_loom()

        assert app.cashmere_threader.called_thread_the_loom is True
        assert app.cashmere_threader.called_thread_the_loom_args == {
            "queue_names": [],
            "event_topic_names": [],
            "queue_names_to_event_topics_names": {},
        }

    async def test_thread_the_loom_is_called_with_the_right_args(self):
        # Setup
        class AnOrphanEvent(BaseSerializableEvent):
            @staticmethod
            def get_event_name() -> str:
                return "AnOrphanEvent"

        class ASubscribedEvent(BaseSerializableEvent):
            @staticmethod
            def get_event_name() -> str:
                return "ASubscribedEvent"

        app = Cashmere(
            domain="demo",
            namespace_prefix="test",
            cashmere_emitter=None,
            cashmere_consumer=None,
            cashmere_threader=self.StubCashmereThreader(),
            exported_events=[AnOrphanEvent],
        )

        # Setup: Subscribe handler to event
        @app.subscribe(ASubscribedEvent)
        def handler(event: ASubscribedEvent):
            pass

        # Act
        await app.thread_the_loom()

        # Assert
        assert app.cashmere_threader.called_thread_the_loom is True
        assert app.cashmere_threader.called_thread_the_loom_args["queue_names"] == [
            "test.Nfj2pbZ9auDVPtbU0YdQDQ.handler"
        ]
        assert set(
            app.cashmere_threader.called_thread_the_loom_args["event_topic_names"]
        ) == set(["test.ASubscribedEvent", "test.AnOrphanEvent"])
        assert app.cashmere_threader.called_thread_the_loom_args[
            "queue_names_to_event_topics_names"
        ] == {"test.Nfj2pbZ9auDVPtbU0YdQDQ.handler": "test.ASubscribedEvent"}


class TestCashmereAppEmitEvent:
    class StubEmitClient:
        async def emit(
            self,
            message: str,
            event_name: str,
            deduplication_id: str | None = None,
            group_id: str | None = None,
        ) -> None:
            self.called_emit = True
            self.called_emit_args = {
                "message": message,
                "event_name": event_name,
                "deduplication_id": deduplication_id,
                "group_id": group_id,
            }

    async def test_raises_exception_when_emit_client_is_none(self):
        with pytest.raises(CashmereException, match="cashmere_emit_client is required"):
            app = Cashmere(
                domain="demo",
                namespace_prefix="test",
                cashmere_emitter=None,
                cashmere_consumer=None,
                cashmere_threader=None,
                exported_events=[],
            )
            await app.emit(SampleEventV1(item_one="a", item_two=1))

    async def test_emit_is_called_with_the_right_args(self):
        # Setup
        class __StubCashmereThreader:
            async def thread_the_loom(
                self,
                queue_names: list[str],
                event_topic_names: list[str],
                queue_names_to_event_topics_names: dict[str, str],
            ) -> ThreadingResult:
                return ThreadingResult(
                    topic_name_to_arn={"test.SampleEventV1": "arn:test.SampleEventV1"},
                    queue_name_to_resource_holder={},
                )

        app = Cashmere(
            domain="demo",
            namespace_prefix="test",
            cashmere_emitter=self.StubEmitClient(),
            cashmere_consumer=None,
            cashmere_threader=__StubCashmereThreader(),
            exported_events=[SampleEventV1],
        )

        await app.thread_the_loom()

        sample_event = SampleEventV1(item_one="a", item_two=1)
        sample_event._cashmere_message = CashmereMessage(
            meta=CashmereMessageMetadata(
                created_at="2021-01-01T00:00:00Z",
                name="SampleEventV1",
                id="123",
            ),
            data={"item_one": "a", "item_two": 1},
        )

        # Act
        await app.emit(sample_event)

        # Assert
        assert app.cashmere_emitter.called_emit is True
        assert json.loads(app.cashmere_emitter.called_emit_args["message"]) == (
            {
                "meta": {
                    "created_at": "2021-01-01T00:00:00Z",
                    "name": "SampleEventV1",
                    "id": "123",
                },
                "data": {"item_one": "a", "item_two": 1},
            }
        )
        assert app.cashmere_emitter.called_emit_args["event_name"] == (
            "test.SampleEventV1"
        )
        assert app.cashmere_emitter.called_emit_args["deduplication_id"] is None
        assert app.cashmere_emitter.called_emit_args["group_id"] is None


class TestCashmereAppWeave:
    class StubConsumeClient:
        async def consume(
            self,
            subscriptions: Dict[QueueResourceHolder, CashmereSubscription],
        ) -> None:
            self.called_consume = True
            self.called_consume_args = subscriptions

    class StubThreader:
        async def thread_the_loom(
            self,
            queue_names: list[str],
            event_topic_names: list[str],
            queue_names_to_event_topics_names: dict[str, str],
        ) -> ThreadingResult:
            return ThreadingResult(
                topic_name_to_arn={"test.SampleEventV1": "arn:test.SampleEventV1"},
                queue_name_to_resource_holder={
                    "test.kNsLGAXzNa1E3P01LDDqyw._test_handler": QueueResourceHolder(
                        queue_name="test.kNsLGAXzNa1E3P01LDDqyw._test_handler",
                        queue_url="blah",
                        queue_arn="blah",
                        translated_queue_name="blah",
                        dead_letter_queue_arn="blah",
                        dead_letter_queue_url="blah",
                        translated_deadletter_queue_name="blah",
                    )
                },
            )

    async def test_weave_calls_consume(self):
        app = Cashmere(
            domain="demo",
            namespace_prefix="test",
            cashmere_emitter=None,
            cashmere_consumer=self.StubConsumeClient(),
            cashmere_threader=self.StubThreader(),
            exported_events=[SampleEventV1],
        )

        @app.subscribe(SampleEventV1)
        async def _test_handler(event: SampleEventV1):
            pass

        threading_result = await app.thread_the_loom()

        await app.weave()

        assert app.cashmere_consumer.called_consume is True
        assert app.cashmere_consumer.called_consume_args == {
            threading_result.queue_name_to_resource_holder[
                "test.kNsLGAXzNa1E3P01LDDqyw._test_handler"
            ]: app.subscriptions.pop()
        }
