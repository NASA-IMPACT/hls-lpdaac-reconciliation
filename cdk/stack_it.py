from typing import Optional

from aws_cdk import CfnOutput, RemovalPolicy, Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subs
from aws_cdk import aws_sqs as sqs
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

        self.hls_inventory_reports_bucket = s3.Bucket(
            self,
            "HlsInventoryReports",
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.hls_forward_bucket = s3.Bucket(
            self,
            "HlsForward",
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.hls_historical_bucket = s3.Bucket(
            self,
            "HlsHistorical",
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.lpdaac_request_topic = sns.Topic(self, "LpdaacRequestTopic")
        self.lpdaac_request_queue = sqs.Queue(self, "LpdaacRequestQueue")
        self.lpdaac_response_topic = sns.Topic(self, "LpdaacResponseTopic")

        # Subscribe a queue to the topic so we can receive messages from the queue
        # to confirm that an message was sent to the topic via the lambda handler.
        self.lpdaac_request_topic.add_subscription(
            subs.SqsSubscription(self.lpdaac_request_queue)
        )

        # Set outputs for use within integration tests

        CfnOutput(
            self,
            "HlsInventoryReportsBucketName",
            value=self.hls_inventory_reports_bucket.bucket_name,
        )
        CfnOutput(
            self,
            "LpdaacRequestQueueUrl",
            value=self.lpdaac_request_queue.queue_url,
        )
        CfnOutput(
            self,
            "HlsForwardBucketName",
            value=self.hls_forward_bucket.bucket_name,
        )
        CfnOutput(
            self,
            "HlsHistoricalBucketName",
            value=self.hls_historical_bucket.bucket_name,
        )
        CfnOutput(
            self,
            "LpdaacResponseTopicArn",
            value=self.lpdaac_response_topic.topic_arn,
        )
