from aws_cdk import (
    Duration,
    Stack,
    aws_lambda as lambda_,
)
from constructs import Construct


class HlsLpdaacReconciliationStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.response_lambda = lambda_.Function(
            self,
            "ReconciliationResponseHandler",
            code=lambda_.Code.from_asset("src/hls_lpdaac_reconciliation/response"),
            handler="index.handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            memory_size=128,
            timeout=Duration.minutes(15),
        )
