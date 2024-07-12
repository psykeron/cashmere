import asyncio
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Awaitable, Callable, Dict, Protocol, Tuple

from aiobotocore.client import AioBaseClient
from aiobotocore.session import get_session
from botocore.exceptions import ReadTimeoutError
from loguru import logger

from .event import CashmereMessage
from .exception import one_line_error
from .subscription import CashmereSubscription
from .threader import QueueResourceHolder

QueueUrlType = str
ErrorQueueUrlType = str


CashmereSubscriptionHandlerType = Callable[[CashmereMessage], Awaitable[None]]


class CashmereConsumeClient(Protocol):
    async def consume(
        self, subscriptions: Dict[QueueResourceHolder, CashmereSubscription]
    ) -> None: ...


@dataclass
class RespectYourCoworkers:
    someone_has_finished_their_work: bool


@dataclass
class ConsumeMetrics:
    queue_url: str
    error_queue_url: str
    started_at: datetime
    finished_at: datetime | None
    messages_received: int
    messages_processed: int
    messages_in_success: int
    messages_in_error: int

    def get_new_aggregate_metrics(self, other: "ConsumeMetrics") -> "ConsumeMetrics":
        """
        returns a new ConsumeMetrics object with the sum of the metrics
        """
        return ConsumeMetrics(
            queue_url=self.queue_url,
            error_queue_url=self.error_queue_url,
            started_at=self.started_at,
            finished_at=other.finished_at,
            messages_received=self.messages_received + other.messages_received,
            messages_processed=self.messages_processed + other.messages_processed,
            messages_in_success=self.messages_in_success + other.messages_in_success,
            messages_in_error=self.messages_in_error + other.messages_in_error,
        )

    def __str__(self) -> str:
        metrics_str = ", ".join(
            [
                f"queue_url={self.queue_url}",
                f"error_queue_url={self.error_queue_url}",
                f"started_at={self.started_at}",
                f"finished_at={self.finished_at}",
                f"messages_received={self.messages_received}",
                f"messages_processed={self.messages_processed}",
                f"messages_in_success={self.messages_in_success}",
                f"messages_in_error={self.messages_in_error}",
            ]
        )
        return f"ConsumeMetrics({metrics_str})"


class SQSCashmereConsumeClient:
    def __init__(
        self,
        region: str,
        endpoint: str | None,
        aws_account_id: str,
        aws_secret_access_key: str,
        aws_access_key_id: str,
    ) -> None:
        self.region = region
        self.endpoint = endpoint
        self.aws_account_id = aws_account_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_access_key_id = aws_access_key_id

    async def consume(
        self, subscriptions: Dict[QueueResourceHolder, CashmereSubscription]
    ) -> None:
        get_session()

        queue_urls_to_subscriptions: Dict[
            Tuple[QueueUrlType, ErrorQueueUrlType], CashmereSubscription
        ] = {}

        queue_url_to_consumer_metrics: Dict[QueueUrlType, ConsumeMetrics] = {}

        for queue_resource, subscription in subscriptions.items():
            queue_urls_to_subscriptions[
                (queue_resource.queue_url, queue_resource.dead_letter_queue_url)
            ] = subscription

            queue_url_to_consumer_metrics[queue_resource.queue_url] = ConsumeMetrics(
                queue_url=queue_resource.queue_url,
                error_queue_url=queue_resource.dead_letter_queue_url,
                started_at=datetime.now(),
                finished_at=None,
                messages_received=0,
                messages_processed=0,
                messages_in_error=0,
                messages_in_success=0,
            )

        # begin consuming the queues
        while True:
            respect_your_coworkers = RespectYourCoworkers(False)
            results = await asyncio.gather(
                *[
                    self.__consume_queue(
                        queue_url,
                        error_queue_url,
                        subscription.handler,
                        subscription.timeout_seconds,
                        return_every_x_messages=10000,
                        return_every_x_iterations=1000,
                        consume_metrics_holder=queue_url_to_consumer_metrics[queue_url],
                        respect_your_coworkers=respect_your_coworkers,
                    )
                    for [
                        queue_url,
                        error_queue_url,
                    ], subscription in queue_urls_to_subscriptions.items()
                ],
                return_exceptions=True,
            )

            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Error consuming queue: {one_line_error(result)}")

            for metrics in queue_url_to_consumer_metrics.values():
                logger.trace(f"Total Metrics: {metrics}")

    async def __consume_queue(
        self,
        queue_url: str,
        error_queue_url: str,
        handler: CashmereSubscriptionHandlerType,
        timeout_seconds: int,
        return_every_x_messages: int,
        return_every_x_iterations: int,
        consume_metrics_holder: ConsumeMetrics,
        respect_your_coworkers: RespectYourCoworkers,
    ):
        """
        Consumes messages from a queue and calls the handler for each message.
        The handler is called with the message body as the first argument.
        The message is deleted from the queue after the handler is called.

        If there is an error in handling a message, the message is sent to the error queue,
        and then deleted from the original queue.

        Args:
            queue_url:
                The url of the queue to consume messages from.

            error_queue_url:
                The url of the queue to send messages to if there is an error processing the message.

            handler:
                The handler to call for each message.

            timeout_seconds:
                The timeout in seconds for the visibility timeout of the message.

            return_every_x_messages:
                The number of messages to process before returning to the event loop.

            return_every_x_iterations:
                The number of iterations to run before returning to the event loop.

            consume_metrics_holder:
                A ConsumeMetrics object to hold the metrics for this consume operation.

            respect_your_coworkers:
                A RespectYourCoworkers object to notify slower workers to stop working and come back to the event loop.

        """
        messages_to_process = (
            consume_metrics_holder.messages_processed + return_every_x_messages
        )
        iterations = 0
        while (
            consume_metrics_holder.messages_processed < messages_to_process
            and iterations < return_every_x_iterations
        ):
            if respect_your_coworkers.someone_has_finished_their_work:
                # return to begin a new era of prosperity and mutual respect
                return

            iterations += 1
            session = get_session()
            async with session.create_client(
                "sqs",
                region_name=self.region,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_access_key_id=self.aws_access_key_id,
                endpoint_url=self.endpoint,
            ) as client:
                consume_metrics = ConsumeMetrics(
                    queue_url=queue_url,
                    error_queue_url=error_queue_url,
                    started_at=datetime.now(),
                    finished_at=None,
                    messages_received=0,
                    messages_processed=0,
                    messages_in_error=0,
                    messages_in_success=0,
                )

                logger.trace(f"Waiting for messages queue: {queue_url}")

                receive_respone = None
                try:
                    max_messages_to_fetch = 1

                    sixty_seconds = 60
                    sixty_minutes = 60
                    twelve_hours = 12

                    max_visibility_timeout = (
                        twelve_hours * sixty_minutes * sixty_seconds
                    )
                    safety_margin = sixty_seconds * max_messages_to_fetch

                    visibility_timeout = (
                        timeout_seconds + safety_margin
                    ) * max_messages_to_fetch

                    receive_respone = await client.receive_message(
                        QueueUrl=queue_url,
                        WaitTimeSeconds=20,
                        MaxNumberOfMessages=max_messages_to_fetch,
                        VisibilityTimeout=min(
                            visibility_timeout, max_visibility_timeout
                        ),
                    )
                except ReadTimeoutError:
                    logger.debug(f"Timeout waiting for messages queue: {queue_url}")
                    continue
                except Exception as e:
                    logger.error(
                        f"Error receiving message from queue: {queue_url}, {one_line_error(e)}"
                    )
                    continue

                if "Messages" not in receive_respone:
                    logger.trace(f"No messages received from queue: {queue_url}")
                    continue

                consume_metrics.messages_received += len(receive_respone["Messages"])
                for message in receive_respone["Messages"]:
                    consume_metrics.messages_processed += 1
                    try:
                        await self.__handle_message(
                            sqs_client=client,
                            handler=handler,
                            message=message,
                            source_queue_url=queue_url,
                            error_queue_url=error_queue_url,
                        )
                        consume_metrics.messages_in_success += 1
                    except Exception as e:
                        consume_metrics.messages_in_error += 1
                        logger.error(
                            f"Error while processing message from queue: {queue_url}, {one_line_error(e)}"
                        )

            consume_metrics.finished_at = datetime.now()

        respect_your_coworkers.someone_has_finished_their_work = True

    async def __handle_message(
        self,
        sqs_client: AioBaseClient,
        handler: CashmereSubscriptionHandlerType,
        message: dict,
        source_queue_url: str,
        error_queue_url: str,
    ) -> None:
        error_str = None
        sqs_message_id = message["MessageId"]

        logger.trace(f"Processing message with id: {sqs_message_id}")

        message_body = json.loads(message["Body"])
        cashmere_payload = json.loads(message_body["Message"])
        md5_message_body = message["MD5OfBody"]
        encountered_exception_during_handle = False

        # handle message
        try:
            await handler(CashmereMessage.from_dict(cashmere_payload))
            logger.trace(f"Processed message with id: {sqs_message_id}")
        except Exception as e:
            encountered_exception_during_handle = True
            error_str = one_line_error(e)
            logger.error(
                f"Error handling message with id: {sqs_message_id}, error: {error_str}"
            )

        # send message to error queue if there was an error
        if encountered_exception_during_handle:
            cashmere_payload["error"] = error_str

            logger.trace(
                f"Sending message with id: {sqs_message_id} to {error_queue_url}"
            )
            try:
                await sqs_client.send_message(
                    QueueUrl=error_queue_url,
                    MessageBody=json.dumps(cashmere_payload),
                    MessageDeduplicationId=md5_message_body,  # md5 of the message body should be acceptable.
                    MessageGroupId=md5_message_body,
                )
            except Exception as e:
                logger.error(
                    "Error sending message to error queue, message will end up in dl after retries."
                )
                raise e

        # delete message from source queue
        try:
            logger.trace(
                f"Deleting message with id: {sqs_message_id}, receipt handle: {message['ReceiptHandle']}"
            )
            await sqs_client.delete_message(
                QueueUrl=source_queue_url,
                ReceiptHandle=message["ReceiptHandle"],
            )
            logger.trace(
                f"Deleted message with id: {sqs_message_id}, receipt handle: {message['ReceiptHandle']}"
            )
        except Exception as e:
            logger.error(
                f"Error deleting message with id: {sqs_message_id}, message will end up in dl after retries,"
                f" Error: {one_line_error(e)}"
            )
            raise e
