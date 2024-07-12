import hashlib
from typing import Protocol

from aiobotocore.session import get_session
from loguru import logger

from .utils_aws import translate_event_name_to_sns_topic_name


class CashmereEmitException(Exception):
    # an exception to wrap any exception raised by the client
    pass


class CashmereEmitClient(Protocol):
    async def emit(
        self,
        message: str,
        event_name: str,
        deduplication_id: str | None = None,
        group_id: str | None = None,
    ) -> None: ...


class SNSCashmereEmitClient:
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

    async def emit(
        self,
        message: str,
        event_name: str,
        deduplication_id: str | None = None,
        group_id: str | None = None,
    ) -> None:
        """
        Publishes a message to the SNS topic that matches the event name.
        """
        session = get_session()
        async with session.create_client(
            "sns",
            region_name=self.region,
            endpoint_url=self.endpoint if self.endpoint else None,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_access_key_id=self.aws_access_key_id,
        ) as client:
            message_hash = hashlib.sha256(message.encode()).hexdigest()
            message_deduplication_id = (
                deduplication_id if deduplication_id else message_hash
            )
            group_id = group_id if group_id else message_hash
            topic_arn = f"arn:aws:sns:{self.region}:{self.aws_account_id}:{translate_event_name_to_sns_topic_name(event_name, is_fifo=True)}"  # noqa: E501
            try:
                await client.publish(
                    TopicArn=topic_arn,
                    Message=message,
                    MessageDeduplicationId=message_deduplication_id,
                    MessageGroupId=group_id,
                )
                logger.trace(f"Published message to {topic_arn}")
            except Exception as e:
                raise CashmereEmitException("An error occurred during publish.") from e
