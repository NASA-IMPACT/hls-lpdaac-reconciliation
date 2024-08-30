from __future__ import annotations

from typing import TYPE_CHECKING

import boto3
import pytest

if TYPE_CHECKING:
    from mypy_boto3_lambda import LambdaClient
    from mypy_boto3_ssm import SSMClient


@pytest.fixture
def lambda_() -> LambdaClient:
    return boto3.client("lambda")


@pytest.fixture
def ssm() -> SSMClient:
    return boto3.client("ssm")
