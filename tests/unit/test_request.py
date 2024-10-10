import json

from aws_lambda_typing.events import S3Event
from mypy_boto3_sns.service_resource import Topic
from mypy_boto3_sqs.service_resource import Queue


def test_request(s3_event: S3Event, sns_topic: Topic, sqs_queue: Queue) -> None:
    # Import here (rather than at top level) to ensure AWS mocks are established.
    # See http://docs.getmoto.org/en/latest/docs/getting_started.html#what-about-those-pesky-imports
    from hls_lpdaac_reconciliation.request.index import handler

    s3 = s3_event["Records"][0]["s3"]  # type: ignore
    bucket = s3["bucket"]["name"]
    key = s3["object"]["key"]  # type: ignore

    # Subscribe to the topic with an SQS queue so we can confirm that the handler
    # published a message to the topic by reading the message from the queue.
    sns_topic.subscribe(Protocol="sqs", Endpoint=sqs_queue.attributes["QueueArn"])

    message = handler(s3_event, None, sns_topic_arn=sns_topic.arn)
    messages = sqs_queue.receive_messages(MaxNumberOfMessages=2, WaitTimeSeconds=20)

    assert message == {"report": {"uri": f"s3://{bucket}/{key}"}}
    assert len(messages) == 1
    assert json.loads(json.loads(messages[0].body)["Message"]) == message
