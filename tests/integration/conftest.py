from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Iterator

import boto3
import pytest

from mypy_boto3_lambda import LambdaClient
from mypy_boto3_s3 import S3Client
from mypy_boto3_sns import SNSClient
from mypy_boto3_sqs import SQSClient


@pytest.fixture(scope="session")
def lambda_() -> LambdaClient:
    return boto3.client("lambda")


@pytest.fixture(scope="session")
def s3() -> S3Client:
    return boto3.client("s3")


@pytest.fixture(scope="session")
def sns() -> SNSClient:
    return boto3.client("sns")


@pytest.fixture(scope="session")
def sqs() -> SQSClient:
    return boto3.client("sqs")


@pytest.fixture(scope="session")
def cdk_outputs() -> dict[str, str]:
    outputs_by_stack: dict[str, dict[str, str]] = json.loads(
        (Path() / "cdk.out" / "outputs.json").read_text()
    )

    return next(
        (
            outputs
            for stack, outputs in outputs_by_stack.items()
            if stack.casefold().endswith("resources")
        ),
        {},
    )


@pytest.fixture(scope="session")
def hls_bucket(cdk_outputs: dict[str, str]) -> str:
    return cdk_outputs["HlsForwardBucketName"]


@pytest.fixture(scope="session")
def lpdaac_bucket(cdk_outputs: dict[str, str]) -> str:
    return cdk_outputs["LpdaacReconciliationReportsBucketName"]


@pytest.fixture(scope="session")
def response_topic_arn(cdk_outputs: dict[str, str]) -> str:
    return cdk_outputs["LpdaacResponseTopicArn"]


@pytest.fixture(scope="session")
def report_path() -> Path:
    return Path("tests") / "fixtures" / "HLS_reconcile_2024239_2.0.json"


@pytest.fixture
def trigger_keys(
    s3: S3Client, hls_bucket: str, report_path: Path
) -> Iterator[Sequence[str]]:
    from hls_lpdaac_reconciliation.response import (
        group_granule_ids,
        notification_trigger_key,
    )

    keys = [
        notification_trigger_key(granule_id)
        for (_nfiles, granule_ids) in group_granule_ids(
            json.loads(report_path.read_text())
        ).values()
        for granule_id in granule_ids
    ]

    for key in keys:
        # Write trigger file (contents don't matter; we just need a file to "touch")
        s3.put_object(Bucket=hls_bucket, Key=key, Body=b"{}")

    yield keys

    for key in keys:
        s3.delete_object(Bucket=hls_bucket, Key=key)


@pytest.fixture
def hls_inventory_reports_bucket(cdk_outputs: dict[str, str]) -> str:
    return cdk_outputs["HlsInventoryReportsBucketName"]


@pytest.fixture
def hls_inventory_reports_id(cdk_outputs: dict[str, str]) -> str:
    return cdk_outputs["HlsInventoryReportId"]


@pytest.fixture
def lpdaac_request_queue_url(cdk_outputs: dict[str, str]) -> str:
    return cdk_outputs["LpdaacRequestQueueUrl"]
