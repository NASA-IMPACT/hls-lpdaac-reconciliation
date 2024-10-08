from typing import Optional

from aws_cdk import RemovalPolicy, Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_sns as sns
from aws_cdk import aws_ssm as ssm
from constructs import Construct


class HlsLpdaacReconciliationStackIT(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
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

        self.hls_forward_bucket = s3.Bucket(
            self,
            "test-forward",
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.hls_historical_bucket = s3.Bucket(
            self,
            "test-historical",
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.response_sns_topic = sns.Topic(self, "test-response")

        # Set SSM Parameters for use within integration tests

        ssm.StringParameter(
            self,
            "forward_bucket_name",
            string_value=self.hls_forward_bucket.bucket_name,
            parameter_name=("/hls/tests/hls-lpdaac-reconciliation/forward-bucket-name"),
        )

        ssm.StringParameter(
            self,
            "historical_bucket_name",
            string_value=self.hls_historical_bucket.bucket_name,
            parameter_name=(
                "/hls/tests/hls-lpdaac-reconciliation/historical-bucket-name"
            ),
        )

        ssm.StringParameter(
            self,
            "response_topic_arn",
            string_value=self.response_sns_topic.topic_arn,
            parameter_name=("/hls/tests/hls-lpdaac-reconciliation/response-topic-arn"),
        )
