import inspect
import json
from contextlib import AsyncExitStack
from functools import wraps
from typing import Any, Awaitable, Callable, Dict, List, Set, Type

from .consumer import CashmereConsumeClient
from .dependencies import DependencyTypes, solve_dependency
from .emitter import CashmereEmitClient
from .event import BaseSerializableEvent, CashmereMessage
from .exception import CashmereException
from .subscription import (
    DEFAULT_SUBSCRIPTION_TIMEOUT_SECONDS,
    CashmereSubscription,
    CashmereSubscriptionException,
)
from .threader import CashmereThreader, QueueResourceHolder, ThreadingResult

CashmereCallableHandlerType = Callable[[BaseSerializableEvent, Any], Awaitable[None]]

CashmereCallableWrappedType = Callable[[CashmereMessage], Awaitable[None]]


class Cashmere:
    domain: str
    namespace_prefix: str | None
    cashmere_emitter: CashmereEmitClient | None
    cashmere_consumer: CashmereConsumeClient | None
    cashmere_threader: CashmereThreader | None
    exported_events: List[Type[BaseSerializableEvent]] | None
    dependencies: List[DependencyTypes] | None

    # Do not set the subscriptions directly. Use the subscribe decorator.
    subscriptions: Set[CashmereSubscription]

    def __init__(
        self,
        domain: str,
        namespace_prefix: str | None,
        cashmere_emitter: CashmereEmitClient | None,
        cashmere_consumer: CashmereConsumeClient | None,
        cashmere_threader: CashmereThreader | None,
        exported_events: List[Type[BaseSerializableEvent]] | None,
        dependencies: List[DependencyTypes] | None = None,
    ):
        """
        CashmereApp is the main class that is used to create a Cashmere application.

        Args:
            domain:
                The domain of the application. This is used to namespace the SNS topics and SQS queues.

            namespace_prefix:
                The prefix of the SQS queues and SNS Events. This is used to namespace the two.
                e.g. production or test1234
                Warning! The prefix will be overriden by the CashmereCollection if one is initialized.

            cashmere_emit_client:
                The client used to emit events. It can be None.

            cashmere_consume_client:
                The client used to consume events. It can be None.

            cashmere_threader:
                The client used to setup the infrastructure. It can be None.

            exported_events:
                A list of event types that the application can emit. It can be None.
                This is only used to configure the required event channels in the infrastructure.

            dependencies:
                A dictionary of dependencies that can be injected into event handlers.
                The matching of dependencies is done by name. It can be None.
        """
        self.domain = domain
        self.namespace_prefix = namespace_prefix
        self.cashmere_emitter = cashmere_emitter
        self.cashmere_consumer = cashmere_consumer
        self.cashmere_threader = cashmere_threader
        self.exported_events = exported_events or []
        self.dependencies = dependencies or []

        self.subscriptions: Set[CashmereSubscription] = set()

        self.___full_queue_name_to_cashmere_subscription: dict[
            str, CashmereSubscription
        ] = {}

        self.__threading_result: ThreadingResult | None = None

    def __prefix_name_with_namespace(self, name: str) -> str:
        if not name:
            raise CashmereException("name is required")

        if self.namespace_prefix:
            return f"{self.namespace_prefix}.{name}"
        return name

    def __get_full_queue_name_from_subscription(
        self, subscription: CashmereSubscription
    ) -> str:
        return self.__prefix_name_with_namespace(
            subscription.get_qualified_short_name()
        )

    def __get_full_event_name_from_subscription(
        self, subscription: CashmereSubscription
    ) -> str:
        return self.__prefix_name_with_namespace(subscription.event.get_event_name())

    def __get_full_event_name_from_event(
        self, event_type: Type[BaseSerializableEvent]
    ) -> str:
        return self.__prefix_name_with_namespace(event_type.get_event_name())

    async def emit(self, event: BaseSerializableEvent) -> None:
        """
        Emit an event.
        """

        if not self.cashmere_emitter:
            raise CashmereException("cashmere_emit_client is required")

        message = json.dumps(event.get_message().to_dict())

        full_event_name = self.__get_full_event_name_from_event(event.__class__)

        await self.cashmere_emitter.emit(message=message, event_name=full_event_name)

    async def weave(self) -> None:
        """
        Begin consuming events for all the subscribed handlers.
        """

        if not self.cashmere_consumer:
            raise CashmereException("cashmere_consume_client is required")

        if not self.__threading_result:
            raise CashmereException(
                "thread_the_loom must be called before weaving the application"
            )

        queue_resource_to_subscription: Dict[
            QueueResourceHolder, CashmereSubscription
        ] = {}
        for (
            full_queue_name,
            subscription,
        ) in self.___full_queue_name_to_cashmere_subscription.items():
            queue_resource_holder = (
                self.__threading_result.queue_name_to_resource_holder[full_queue_name]
            )
            queue_resource_to_subscription[queue_resource_holder] = subscription

        await self.cashmere_consumer.consume(
            subscriptions=queue_resource_to_subscription
        )

    def subscribe(
        self,
        event: Type[BaseSerializableEvent],
        timeout_seconds: int = DEFAULT_SUBSCRIPTION_TIMEOUT_SECONDS,
    ) -> Callable:
        def decorator(
            handler: CashmereCallableHandlerType,
        ) -> CashmereCallableWrappedType:
            """
            The first argument of the handler is always the event and should be called "event".
            The rest of the arguments are the dependencies.
            The dependencies are matched by name and injected into the handler.
            """

            @wraps(handler)
            async def wrapper(message: CashmereMessage):
                kwargs = {}

                async with AsyncExitStack() as stack:
                    all_handler_args = set(inspect.getfullargspec(handler).args)
                    for dependency in self.dependencies or []:
                        if dependency.name in all_handler_args:
                            kwargs[dependency.name] = await solve_dependency(
                                dependency=dependency, stack=stack
                            )

                    await handler(event=event.from_event(message), **kwargs)  # type: ignore[call-arg]

            subscription = CashmereSubscription(
                self.domain, event, wrapper, timeout_seconds
            )
            if subscription in self.subscriptions:
                raise CashmereSubscriptionException(
                    f"Handler for event {subscription.get_qualified_short_name()} already exists."
                )
            self.subscriptions.add(subscription)

            return wrapper

        return decorator

    async def thread_the_loom(self) -> ThreadingResult:
        """
        Setup the Cashmere application by:
            - generating the real queue names to handlers mapping
            - creating the queues
            - creating the event topics
            - creating the subscription of queues to event topics
        """

        if not self.cashmere_threader:
            raise CashmereException("cashmere_threader is required")

        # generate the real queue names to handlers mapping
        self.___full_queue_name_to_cashmere_subscription = {
            self.__get_full_queue_name_from_subscription(subscription): subscription
            for subscription in self.subscriptions
        }

        # generate the queue names
        queue_names = list(self.___full_queue_name_to_cashmere_subscription.keys())

        # generate the event topics names from the subscriptions and given event types
        event_topic_names = list(
            set(
                [
                    self.__get_full_event_name_from_subscription(subscription)
                    for subscription in self.subscriptions
                ]
                + [
                    self.__get_full_event_name_from_event(event_type)
                    for event_type in self.exported_events or []
                ]
            )
        )

        # generate the queue names to event topics names mapping
        queue_names_to_event_topics_names = {
            self.__get_full_queue_name_from_subscription(
                subscription
            ): self.__get_full_event_name_from_subscription(subscription)
            for subscription in self.subscriptions
        }

        # setup the infrastructure
        threading_result = await self.cashmere_threader.thread_the_loom(
            queue_names=queue_names,
            event_topic_names=event_topic_names,
            queue_names_to_event_topics_names=queue_names_to_event_topics_names,
        )
        self.__threading_result = threading_result

        return threading_result
