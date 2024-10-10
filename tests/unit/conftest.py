import os
from pathlib import Path
from typing import Iterator

import boto3
import pytest
from aws_lambda_typing.events import SNSEvent
from moto import mock_aws
from mypy_boto3_s3 import S3ServiceResource
from mypy_boto3_s3.service_resource import Bucket, Object


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def s3_resource(aws_credentials) -> Iterator[S3ServiceResource]:
    with mock_aws():
        yield boto3.resource("s3")


@pytest.fixture(scope="function")
def s3_bucket(s3_resource: S3ServiceResource) -> Bucket:
    bucket = s3_resource.Bucket("forward")
    bucket.create()

    return bucket


@pytest.fixture(scope="function")
def s3_trigger_object(s3_bucket: Bucket) -> Object:
    # NOTE: This aligns with the entry for HLS.S30.T15XWH.2124237T194859.v2.0 in
    # tests/unit/cassettes/test_lpdaac_forward_handler.yaml, where we have manually
    # set the CMR-Hits header to 0 to force a "not in CMR" result, which should then
    # attempt to "touch" this "trigger" object.  We don't care what's inside the file,
    # only that it exists, so we simply make the contents an empty JSON object.
    return s3_bucket.put_object(
        Key=(
            "S30/data/2124237/HLS.S30.T15XWH.2124237T194859.v2.0/"
            "HLS.S30.T15XWH.2124237T194859.v2.0.json"
        ),
        Body=b"{}",
    )


@pytest.fixture(scope="function")
def s3_report_bucket(s3_resource: S3ServiceResource) -> Bucket:
    bucket = s3_resource.Bucket("reconciliation-reports")
    bucket.create()

    return bucket


@pytest.fixture(scope="function")
def s3_reconciliation_report(s3_report_bucket: Bucket) -> Object:
    report = Path("tests") / "fixtures" / "HLS_reconcile_2024239_2.0.json"
    return s3_report_bucket.put_object(
        Key="reports/HLS_reconcile_2024239_2.0.json", Body=report.read_bytes()
    )


@pytest.fixture(scope="function")
def s3_historical_reconciliation_report(s3_report_bucket: Bucket) -> Object:
    report = Path("tests") / "fixtures" / "HLS_reconcile_2024239_2.0.json"
    return s3_report_bucket.put_object(
        Key="reports/HLS_historical_reconcile_2024239_2.0.json",
        Body=report.read_bytes(),
    )


@pytest.fixture(scope="function")
def sns_event_no_discrepancies() -> SNSEvent:
    return make_sns_event(
        subject="Rec-Report HLS lp-prod HLS_reconcile_2024239_2.0.rpt Ok",
        message="Test message",
    )


@pytest.fixture(scope="function")
def sns_event_discrepancies(
    s3_report_bucket: Bucket, s3_reconciliation_report: Object
) -> SNSEvent:
    # We don't explicitly use the value of s3_reconciliation_report here, but depending
    # on it ensures it exists in our test bucket before the SNS event is generated.
    # However, the S3 key of the report object must match the S3 key given within the
    # message-discrepancies.txt fixture.
    message_fixture = Path("tests") / "fixtures" / "message-discrepancies.txt"

    return make_sns_event(
        subject="Rec-Report HLS lp-prod HLS_reconcile_2024239_2.0.rpt",
        message=message_fixture.read_text().format(bucket=s3_report_bucket.name),
    )


@pytest.fixture(scope="function")
def sns_event_discrepancies_historical(
    s3_report_bucket: Bucket,
    s3_historical_reconciliation_report: Object,
) -> SNSEvent:
    # We don't explicitly use the value of s3_reconciliation_report here, but depending
    # on it ensures it exists in our test bucket before the SNS event is generated.
    # However, the S3 key of the report object must match the S3 key given within the
    # message-discrepancies-historical.txt fixture.
    message_fixture = (
        Path("tests") / "fixtures" / "message-discrepancies-historical.txt"
    )

    return make_sns_event(
        subject="Rec-Report HLS lp-prod HLS_historical_reconcile_2024239_2.0.rpt",
        message=message_fixture.read_text().format(bucket=s3_report_bucket.name),
    )


def make_sns_event(*, subject: str, message: str) -> SNSEvent:
    # We only care about "Subject" and "Message" within Records[0]["Sns"], but dummy
    # values are populated everywhere else to make the event conform to the SNSEvent
    # type definition.
    return {
        "Records": [
            {
                "EventVersion": "1.0",
                "EventSubscriptionArn": "arn:aws:sns:us-east-1:123456789012:MyTopic",
                "EventSource": "aws:sns",
                "Sns": {
                    "Subject": subject,
                    "Message": message,
                    "SignatureVersion": "1",
                    "Timestamp": "1970-01-01T00:00:00.000Z",
                    "Signature": "EXAMPLE",
                    "SigningCertUrl": "EXAMPLE",
                    "MessageId": "95df01b4-ee98-5cb9-9903-4c221d41eb5e",
                    "MessageAttributes": {
                        "Test": {"Type": "String", "Value": "TestString"}
                    },
                    "Type": "Notification",
                    "UnsubscribeUrl": "EXAMPLE",
                    "TopicArn": "arn:aws:sns:us-east-1:123456789012:MyTopic",
                },
            },
        ],
    }
