from __future__ import annotations

from typing import TYPE_CHECKING

import boto3
import pytest

if TYPE_CHECKING:
    from mypy_boto3_lambda import LambdaClient


@pytest.fixture
def lambda_() -> LambdaClient:
    return boto3.client("lambda")
