from __future__ import annotations
import json
from pathlib import Path

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
