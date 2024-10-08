from typing import Optional

from aws_cdk import Duration, Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_sns as sns
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_sns_subscriptions as subs
from constructs import Construct


class HlsLpdaacReconciliationStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        hls_forward_bucket: str,
        hls_historical_bucket: str,
        response_sns_topic_arn: str,
        managed_policy_name: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        if managed_policy_name:
            account_id = iam.AccountRootPrincipal().account_id

            iam.PermissionsBoundary.of(self).apply(
                iam.ManagedPolicy.from_managed_policy_arn(
                    self,
                    "PermissionsBoundary",
                    f"arn:aws:iam::{account_id}:policy/{managed_policy_name}",
                )
            )

        self.response_lambda = lambda_.Function(
            self,
            "ReconciliationResponseHandler",
            code=lambda_.Code.from_asset("src", exclude=["**/*.egg-info"]),
            handler="hls_lpdaac_reconciliation/response/index.handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            memory_size=128,
            timeout=Duration.minutes(15),
            environment={
                "HLS_FORWARD_BUCKET": hls_forward_bucket,
                "HLS_HISTORICAL_BUCKET": hls_historical_bucket,
            },
        )

        # Subscribe lambda function to topic
        sns.Topic.from_topic_arn(
            self, "ResponseTopic", topic_arn=response_sns_topic_arn
        ).add_subscription(
            subs.LambdaSubscription(self.response_lambda)  # type: ignore
        )

        # Allow lambda function to read/write forward and historical buckets
        s3.Bucket.from_bucket_name(
            self, "ForwardBucket", hls_forward_bucket
        ).grant_read_write(self.response_lambda)
        s3.Bucket.from_bucket_name(
            self, "HistoricalBucket", hls_historical_bucket
        ).grant_read_write(self.response_lambda)
