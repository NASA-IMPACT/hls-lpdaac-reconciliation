from __future__ import annotations

from typing import TYPE_CHECKING

import boto3

if TYPE_CHECKING:
    from aws_lambda_typing.context import Context
    from aws_lambda_typing.events import SNSEvent

s3 = boto3.resource("s3")


def handler(event: SNSEvent, _: Context) -> str:
    return "Hello world!"
