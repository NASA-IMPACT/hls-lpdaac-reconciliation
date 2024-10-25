from __future__ import annotations

import json
from collections.abc import Mapping

from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient


def test_request_handler(
    cdk_outputs: Mapping[str, str], s3: S3Client, sqs: SQSClient
) -> None:
    bucket = cdk_outputs["HlsInventoryReportsBucketName"]
    key = "inventory.rpt"

    # Write S3 inventory report file (contents don't matter)
    s3.put_object(Bucket=bucket, Key=key, Body=bytes())

    # Read message from SQS queue that is subscribed to SNS topic to which the handler
    # should have published a message indicating the location of the inventory report.
    queue_url = cdk_outputs["LpdaacRequestQueueUrl"]
    # Set MaxNumberOfMessages > 1 to test that only 1 message was published
    result = sqs.receive_message(
        QueueUrl=queue_url, MaxNumberOfMessages=2, WaitTimeSeconds=20
    )
    messages = result.get("Messages", [])

    assert len(messages) == 1

    body = json.loads(messages[0].get("Body") or "{}")
    message = json.loads(body.get("Message"))

    assert message == {"report": {"uri": f"s3://{bucket}/{key}"}}
