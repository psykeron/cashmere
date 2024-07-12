import json
from dataclasses import dataclass
from typing import Dict, List, Protocol, Tuple

from aiobotocore.session import get_session
from botocore.client import ClientError

from .utils_aws import (
    translate_event_name_to_sns_topic_name,
    translate_queue_name_to_sqs_queue_name,
)


@dataclass
class QueueResourceHolder:
    queue_name: str
    translated_queue_name: str
    queue_url: str
    queue_arn: str
    dead_letter_queue_url: str
    dead_letter_queue_arn: str
    translated_deadletter_queue_name: str

    def __hash__(self) -> int:
        return hash(f"{self.queue_name}")


@dataclass
class ThreadingResult:
    topic_name_to_arn: Dict[str, str]
    queue_name_to_resource_holder: Dict[str, QueueResourceHolder]


class CashmereThreader(Protocol):
    async def thread_the_loom(
        self,
        queue_names: List[str],
        event_topic_names: List[str],
        queue_names_to_event_topics_names: Dict[str, str],
    ) -> ThreadingResult:
        """
        Setup all the necessary infrastructure to consume and emit events

        Args:
            queue_names:
                The names of the queues to be created.

            event_topic_names:
                The names of the event topics to be created.

            queue_names_to_event_topics_names:
                A dictionary that maps queue names to event topic names to register
                the queue as a subscriber to the event topic.
        """
        ...


_EventTopicNameType = str
_EventTopicArnType = str
_QueueNameType = str
_QueueURLType = str
_QueueARNType = str


class SNSSQSWorkerCashmereThreader:
    """
    A CashmereThreader implementation that uses SNS and SQS to setup the infrastructure
    """

    # TODO: Have to add a test for this.

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

    async def thread_the_loom(
        self,
        queue_names: List[str],
        event_topic_names: List[str],
        queue_names_to_event_topics_names: Dict[str, str],
    ) -> ThreadingResult:
        """
        Setup all the necessary infrastructure to consume and emit events
        """

        topic_name_to_arn = await self.__create_sns_topics(event_topic_names)
        queue_name_to_resource_holder = await self.__create_sqs_queues(queue_names)
        await self.__subscribe_queues_to_event_topics(
            queue_names_to_event_topics_names,
            topic_name_to_arn,
            queue_name_to_resource_holder,
        )

        return ThreadingResult(
            topic_name_to_arn=topic_name_to_arn,
            queue_name_to_resource_holder=queue_name_to_resource_holder,
        )

    async def __list_existing_sns_topic_arns(self) -> List[str]:
        topic_arns = []

        session = get_session()
        async with session.create_client(
            "sns",
            region_name=self.region,
            endpoint_url=self.endpoint if self.endpoint else None,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_access_key_id=self.aws_access_key_id,
        ) as sns_client:
            # do the first call
            response = await sns_client.list_topics()
            next_token = response.get("NextToken")

            topic_arns.extend([topic["TopicArn"] for topic in response["Topics"]])

            # do the rest of the calls if there are more topics
            while next_token:
                response = await sns_client.list_topics(NextToken=next_token)
                next_token = response.get("NextToken") or None

                topic_arns.extend([topic["TopicArn"] for topic in response["Topics"]])

        return topic_arns

    async def __fetch_attributes_for_sns_topics(
        self, topic_arns: List[str]
    ) -> Dict[str, Dict[str, str]]:
        topic_arn_to_attributes = {}

        session = get_session()
        async with session.create_client(
            "sns",
            region_name=self.region,
            endpoint_url=self.endpoint if self.endpoint else None,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_access_key_id=self.aws_access_key_id,
        ) as sns_client:
            for topic_arn in topic_arns:
                response = await sns_client.get_topic_attributes(TopicArn=topic_arn)
                topic_arn_to_attributes[topic_arn] = response["Attributes"]

        return topic_arn_to_attributes

    def __extract_topic_name_from_arn(self, arn: str) -> str:
        return arn.split(":")[-1]

    async def __create_sns_topics(
        self, event_topic_names: List[str]
    ) -> Dict[_EventTopicNameType, _EventTopicArnType]:
        topic_name_to_arn = {}

        session = get_session()

        existing_topics_arns = await self.__list_existing_sns_topic_arns()
        existing_topics_arns_to_attributes = (
            await self.__fetch_attributes_for_sns_topics(existing_topics_arns)
        )

        # Create Event Topics on SNS
        async with session.create_client(
            "sns",
            region_name=self.region,
            endpoint_url=self.endpoint if self.endpoint else None,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_access_key_id=self.aws_access_key_id,
        ) as sns_client:
            # Create the event topics
            for event_topic_name in event_topic_names:
                translated_topic_name = translate_event_name_to_sns_topic_name(
                    event_topic_name, is_fifo=True
                )

                existing_event_topic_arn: str | None = next(
                    (
                        attributes["TopicArn"]
                        for attributes in existing_topics_arns_to_attributes.values()
                        if self.__extract_topic_name_from_arn(attributes["TopicArn"])
                        == translated_topic_name
                    ),
                    None,
                )

                if not existing_event_topic_arn:
                    print(f"Creating event topic: {event_topic_name}")
                    topic_creation_response = await sns_client.create_topic(
                        Name=translated_topic_name,
                        Attributes={
                            "FifoTopic": "true",
                            "ArchivePolicy": '{"MessageRetentionPeriod":"5"}',
                        },
                    )
                    topic_name_to_arn[event_topic_name] = topic_creation_response[
                        "TopicArn"
                    ]
                else:
                    print(
                        f"Skipping create as event topic already exists: {event_topic_name}"
                    )
                    topic_name_to_arn[event_topic_name] = existing_event_topic_arn

        return topic_name_to_arn

    async def __create_sqs_queues(
        self, queue_names: List[str]
    ) -> Dict[_QueueNameType, QueueResourceHolder]:
        session = get_session()
        queue_name_to_resource_holder: Dict[_QueueNameType, QueueResourceHolder] = {}
        async with session.create_client(
            "sqs",
            region_name=self.region,
            endpoint_url=self.endpoint if self.endpoint else None,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_access_key_id=self.aws_access_key_id,
        ) as sqs_client:
            # Create the queues
            for queue_name in queue_names:
                translated_queue_name = translate_queue_name_to_sqs_queue_name(
                    queue_name, is_fifo=True
                )
                translated_deadletter_queue_name = (
                    translate_queue_name_to_sqs_queue_name(
                        queue_name, is_fifo=True, is_deadletter=True
                    )
                )

                # create the dead letter queue for the queue
                (
                    dead_letter_queue_url,
                    dead_letter_queue_arn,
                ) = await self.__create_queue(
                    sqs_client,
                    translated_deadletter_queue_name,
                    {
                        "FifoQueue": "true",
                        "ContentBasedDeduplication": "false",
                    },
                )

                # create the queue
                queue_url, queue_arn = await self.__create_queue(
                    sqs_client,
                    translated_queue_name,
                    {
                        "FifoQueue": "true",
                        "ContentBasedDeduplication": "false",
                        "VisibilityTimeout": "300",
                    },
                )

                # update the dead-letter queue's redrive allow policy
                await sqs_client.set_queue_attributes(
                    QueueUrl=dead_letter_queue_url,
                    Attributes={
                        "RedriveAllowPolicy": f'{{"redrivePermission":"byQueue","sourceQueueArns":["{queue_arn}"]}}'
                    },
                )

                # Update the source queue's redrive policy
                await sqs_client.set_queue_attributes(
                    QueueUrl=queue_url,
                    Attributes={
                        "RedrivePolicy": f'{{"deadLetterTargetArn":"{dead_letter_queue_arn}","maxReceiveCount":"2"}}'
                    },
                )

                queue_name_to_resource_holder[queue_name] = QueueResourceHolder(
                    queue_name=queue_name,
                    translated_queue_name=translated_queue_name,
                    queue_url=queue_url,
                    queue_arn=queue_arn,
                    dead_letter_queue_url=dead_letter_queue_url,
                    dead_letter_queue_arn=dead_letter_queue_arn,
                    translated_deadletter_queue_name=translated_deadletter_queue_name,
                )

        return queue_name_to_resource_holder

    async def __create_queue(
        self, sqs_client, queue_name, attributes
    ) -> Tuple[_QueueURLType, _QueueARNType]:
        try:
            await sqs_client.create_queue(
                QueueName=queue_name,
                Attributes=attributes,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] != "SQS.Client.exceptions.QueueNameExists":
                raise e

        queue_url_response = await sqs_client.get_queue_url(QueueName=queue_name)
        queue_url = queue_url_response["QueueUrl"]

        queue_attributes_response = await sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["QueueArn"],
        )
        queue_arn = queue_attributes_response["Attributes"]["QueueArn"]

        return queue_url, queue_arn

    async def __subscribe_queues_to_event_topics(
        self,
        queue_names_to_event_topics_names: Dict[_QueueNameType, _EventTopicNameType],
        topic_name_to_arn: Dict[_EventTopicNameType, _EventTopicArnType],
        queue_name_to_resource_holder: Dict[_QueueNameType, QueueResourceHolder],
    ):
        session = get_session()

        # subscribe the queues to the event topics
        async with session.create_client(
            "sns",
            region_name=self.region,
            endpoint_url=self.endpoint if self.endpoint else None,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_access_key_id=self.aws_access_key_id,
        ) as sns_client:
            for (
                queue_name,
                event_topic_name,
            ) in queue_names_to_event_topics_names.items():
                await sns_client.subscribe(
                    TopicArn=topic_name_to_arn[event_topic_name],
                    Protocol="sqs",
                    Endpoint=queue_name_to_resource_holder[queue_name].queue_arn,
                    Attributes={},
                )

        # Update the source queue's policy to allow sns to send messages to it
        async with session.create_client(
            "sqs",
            region_name=self.region,
            endpoint_url=self.endpoint if self.endpoint else None,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_access_key_id=self.aws_access_key_id,
        ) as sqs_client:
            for (
                queue_name,
                event_topic_name,
            ) in queue_names_to_event_topics_names.items():
                # Update the source queue's policy to allow sns to send messages to it
                allow_sns_to_send_messages_to_sqs_policy = {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "Allow-SNS-to-Send-Messages",
                            "Effect": "Allow",
                            "Principal": {"Service": "sns.amazonaws.com"},
                            "Action": ["sqs:SendMessage", "sqs:SendMessageBatch"],
                            "Resource": queue_name_to_resource_holder[
                                queue_name
                            ].queue_arn,
                            "Condition": {
                                "ArnLike": {
                                    "aws:SourceArn": topic_name_to_arn[event_topic_name]
                                }
                            },
                        }
                    ],
                }

                await sqs_client.set_queue_attributes(
                    QueueUrl=queue_name_to_resource_holder[queue_name].queue_url,
                    Attributes={
                        "Policy": json.dumps(
                            allow_sns_to_send_messages_to_sqs_policy, indent=0
                        )
                    },
                )
