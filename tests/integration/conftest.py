from __future__ import annotations

import boto3
import botocore
import botocore.client
import pytest

from mypy_boto3_lambda import LambdaClient
from mypy_boto3_s3 import S3Client
from mypy_boto3_sns import SNSClient
from mypy_boto3_ssm import SSMClient


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
def ssm() -> SSMClient:
    return boto3.client("ssm")
