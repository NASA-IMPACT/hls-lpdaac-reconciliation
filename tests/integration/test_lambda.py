from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_lambda import LambdaClient
    from mypy_boto3_ssm import SSMClient


def test_lambda(lambda_: LambdaClient, ssm: SSMClient):
    function_name = ssm.get_parameter(
        Name="/hls/tests/hls-lpdaac-reconciliation/response-function-name"
    )["Parameter"].get("Value")
    assert function_name is not None  # make type checker happy

    response = lambda_.invoke(FunctionName=function_name, Payload="{}")

    assert response["StatusCode"] == 200
    assert response["Payload"].read().decode("utf-8") == "Hello world!"
