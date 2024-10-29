from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from time import sleep

from mypy_boto3_s3 import S3Client
from mypy_boto3_sns import SNSClient


def wait_until_modified(
    s3: S3Client, *, since: datetime, bucket: str, key: str
) -> datetime:
    modified = since

    for _ in range(10):
        if since < (modified := s3.head_object(Bucket=bucket, Key=key)["LastModified"]):
            break
        sleep(2)

    return modified


def test_response_handler(
    cdk_outputs: Mapping[str, str], s3: S3Client, sns: SNSClient
) -> None:
    hls_bucket = cdk_outputs["HlsForwardBucketName"]
    lpdaac_bucket = cdk_outputs["LpdaacReconciliationReportsBucketName"]
    topic_arn = cdk_outputs["LpdaacResponseTopicArn"]

    # Write trigger file (contents don't matter; we just need a file to "touch")
    trigger_key = "S30/data/2124237/HLS.S30.T15XWH.2124237T194859.v2.0/HLS.S30.T15XWH.2124237T194859.v2.0.json"
    s3.put_object(Bucket=hls_bucket, Key=trigger_key, Body=bytes())
    # Read trigger file to get "original" timestamp
    trigger_object = s3.head_object(Bucket=hls_bucket, Key=trigger_key)
    created = trigger_object["LastModified"]

    # Write reconciliation report file to the LPDAAC bucket
    report_fixture = Path("tests") / "fixtures" / "HLS_reconcile_2024239_2.0.json"
    report_key = "reports/HLS_reconcile_2024239_2.0.json"
    s3.put_object(
        Bucket=lpdaac_bucket, Key=report_key, Body=report_fixture.read_bytes()
    )

    # Send message to topic about the report in the LPDAAC bucket, which should
    # trigger "response" lambda with the message, causing the lambda to then
    # "touch" the trigger file in the HLS bucket.
    message_fixture = Path("tests") / "fixtures" / "message-discrepancies.txt"
    message = message_fixture.read_text().format(bucket=lpdaac_bucket)
    sns.publish(Message=message, TopicArn=topic_arn)

    # Read trigger file to get "touched" timestamp
    touched = wait_until_modified(s3, since=created, bucket=hls_bucket, key=trigger_key)

    # Count trigger files to make sure no additional trigger files were created
    key_count = s3.list_objects_v2(Bucket=hls_bucket, Prefix="S30/", MaxKeys=2)[
        "KeyCount"
    ]

    assert touched > created and key_count == 1
