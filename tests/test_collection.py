from typing import Dict

from src.app import Cashmere
from src.collection import CashmereCollection
from src.event import BaseSerializableEvent
from src.subscription import CashmereSubscription
from src.threader import QueueResourceHolder, ThreadingResult


class TestCashmereCollection:
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
        ) -> None:
            self.consume_called = True

            # hack: raise an exception to test that it was called
            raise Exception("consume called")

    class StubCashmereThreader:
        async def thread_the_loom(
            self,
            queue_names: list[str],
            event_topic_names: list[str],
            queue_names_to_event_topics_names: dict[str, str],
        ) -> ThreadingResult:
            self.thread_the_loom_called = True

            print("HELLLLLLO")
            print(queue_names)

            return ThreadingResult(
                topic_name_to_arn={"blah.SampleEventV1": "arn:blah.SampleEventV1"},
                queue_name_to_resource_holder={
                    "namespace_prefix.tKPRm9j2s7rMD1JUucdu3Q.handle_stub_event": QueueResourceHolder(
                        queue_url="blah",
                        queue_arn="blah",
                        dead_letter_queue_arn="blah",
                        dead_letter_queue_url="blah",
                        queue_name="blah",
                        translated_deadletter_queue_name="blah",
                        translated_queue_name="blah",
                    ),
                    "namespace_prefix.6/7NdW+TeZQ55WLEmhPVpw.handle_stub_event": QueueResourceHolder(
                        queue_url="blah",
                        queue_arn="blah",
                        dead_letter_queue_arn="blah",
                        dead_letter_queue_url="blah",
                        queue_name="blah",
                        translated_deadletter_queue_name="blah",
                        translated_queue_name="blah",
                    ),
                },
            )

    class StubEvent(BaseSerializableEvent):
        @staticmethod
        def get_event_name() -> str:
            return "StubEvent"

    def build_cashmere_app_with_subscriptions(
        self, domain: str, namespace_prefix: str
    ) -> Cashmere:
        cashmere_app = Cashmere(
            domain=domain,
            namespace_prefix=namespace_prefix,
            cashmere_emitter=self.StubEmitClient(),
            cashmere_consumer=self.StubConsumeClient(),
            cashmere_threader=self.StubCashmereThreader(),
            exported_events=[self.StubEvent],
        )

        @cashmere_app.subscribe(TestCashmereCollection.StubEvent)
        def handle_stub_event(event: TestCashmereCollection.StubEvent):
            pass

        return cashmere_app

    def test_initialization(self):
        cashmere_apps = [
            self.build_cashmere_app_with_subscriptions("app1", "namespace_prefix_one"),
            self.build_cashmere_app_with_subscriptions("app2", "namespace_prefix_two"),
        ]
        cashmere_collection = CashmereCollection(
            namespace_prefix="namespace_prefix", apps=cashmere_apps
        )
        assert cashmere_collection.namespace_prefix == "namespace_prefix"
        assert cashmere_collection.apps == cashmere_apps

    def test_initialization_overrides_app_namespace_prefix(self):
        cashmere_apps = [
            self.build_cashmere_app_with_subscriptions("app1", "namespace_prefix_one"),
            self.build_cashmere_app_with_subscriptions("app2", "namespace_prefix_two"),
        ]
        cashmere_collection = CashmereCollection(
            namespace_prefix="namespace_prefix", apps=cashmere_apps
        )
        assert all(
            app.namespace_prefix == "namespace_prefix"
            for app in cashmere_collection.apps
        )

    async def test_thread_the_loom(self):
        app1 = self.build_cashmere_app_with_subscriptions(
            "app1", "namespace_prefix_one"
        )
        app2 = self.build_cashmere_app_with_subscriptions(
            "app2", "namespace_prefix_two"
        )

        cashmere_apps = [app1, app2]

        cashmere_collection = CashmereCollection(
            namespace_prefix="namespace_prefix", apps=cashmere_apps
        )
        await cashmere_collection.thread_the_loom()
        assert app1.cashmere_threader.thread_the_loom_called
        assert app2.cashmere_threader.thread_the_loom_called

    async def test_weave(self):
        # Prepare
        app1 = self.build_cashmere_app_with_subscriptions(
            "app1", "namespace_prefix_one"
        )
        app2 = self.build_cashmere_app_with_subscriptions(
            "app2", "namespace_prefix_two"
        )

        cashmere_apps = [app1, app2]

        cashmere_collection = CashmereCollection(
            namespace_prefix="namespace_prefix", apps=cashmere_apps
        )
        await cashmere_collection.thread_the_loom()

        # Act
        await cashmere_collection.weave(fail_app_after_x_exceptions=2)

        # Assert
        assert app1.cashmere_consumer.consume_called
        assert app2.cashmere_consumer.consume_called
