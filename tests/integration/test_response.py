from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from time import sleep

from mypy_boto3_s3 import S3Client
from mypy_boto3_sns import SNSClient


def wait_until_modified(
    s3: S3Client, *, since: datetime, bucket: str, key: str
) -> datetime:
    for _ in range(10):
        if since < (modified := s3.head_object(Bucket=bucket, Key=key)["LastModified"]):
            return modified
        sleep(2)

    return since


def test_response_handler(
    s3: S3Client,
    sns: SNSClient,
    hls_bucket: str,
    lpdaac_bucket: str,
    response_topic_arn: str,
    trigger_keys: Sequence[str],
    report_path: Path,
) -> None:
    # Get timestamps of when the trigger files were created by the fixture.
    createds = [
        s3.head_object(Bucket=hls_bucket, Key=key)["LastModified"]
        for key in trigger_keys
    ]

    # Write reconciliation report file to the LPDAAC bucket.
    report_key = "reports/HLS_reconcile_2024239_2.0.json"
    s3.put_object(Bucket=lpdaac_bucket, Key=report_key, Body=report_path.read_bytes())

    # Send message to topic about the report in the LPDAAC bucket, which should
    # trigger "response" lambda with the message, causing the lambda to then
    # "touch" the trigger file in the HLS bucket.
    message_fixture = Path("tests") / "fixtures" / "message-discrepancies.txt"
    message = message_fixture.read_text().format(bucket=lpdaac_bucket)
    sns.publish(Message=message, TopicArn=response_topic_arn)

    # Count trigger files to make sure no additional trigger files were created.
    listing = s3.list_objects_v2(Bucket=hls_bucket, Prefix="S30/")
    key_count = listing["KeyCount"]
    assert key_count == len(trigger_keys), f"expected {len(trigger_keys)} trigger files"

    # Get last modified timestamps of the trigger files to make sure they were
    # "touched" since they were created.
    assert all(
        [
            wait_until_modified(s3, since=created, bucket=hls_bucket, key=key) > created
            for key, created in zip(trigger_keys, createds)
        ]
    ), "at least 1 trigger file was not 'touched'"
