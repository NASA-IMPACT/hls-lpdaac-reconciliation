# pyright: reportTypedDictNotRequiredAccess=false
from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, Any, Mapping

import boto3

if TYPE_CHECKING:  # pragma: no cover
    from aws_lambda_typing.events import S3Event
    from aws_lambda_typing.events.s3 import S3

sns_client = boto3.client("sns")


def handler(
    event: S3Event, _context: Any, *, topic_arn: str | None = None
) -> Mapping[str, Mapping[str, str]]:
    """Publish message to SNS topic with URL of HLS inventory report from S3 event."""

    s3: S3 = event["Records"][0]["s3"]
    bucket = s3["bucket"]["name"]
    key = s3["object"]["key"]
    message = {"report": {"uri": f"s3://{bucket}/{key}"}}
    topic_arn = topic_arn or os.environ["LPDAAC_REQUEST_TOPIC_ARN"]

    print(f"Publishing HLS inventory report to SNS topic '{topic_arn}': {message}")

    sns_client.publish(TopicArn=topic_arn, Message=json.dumps(message))

    return message
