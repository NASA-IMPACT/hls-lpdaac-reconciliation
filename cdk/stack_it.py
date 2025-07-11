from typing import Any

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
        managed_policy_name: str | None = None,
        **kwargs: Any,
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

        self.hls_inventory_reports_bucket = self.make_bucket("HlsInventoryReports")
        self.hls_forward_bucket = self.make_bucket("HlsForward")
        self.hls_historical_bucket = self.make_bucket("HlsHistorical")
        self.lpdaac_reports_bucket = self.make_bucket("LpdaacReconciliationReports")

        self.lpdaac_request_topic = sns.Topic(self, "LpdaacRequestTopic")
        self.lpdaac_response_topic = sns.Topic(self, "LpdaacResponseTopic")

        # Subscribe a queue to the topic so we can receive messages from the queue
        # to confirm that an message was sent to the topic via the lambda handler.
        lpdaac_request_queue = sqs.Queue(self, "LpdaacRequestQueue")
        self.lpdaac_request_topic.add_subscription(
            subs.SqsSubscription(lpdaac_request_queue)
        )

        self.hls_forward_bucket_inventory_id = "hls_v2_parquet"
        self.hls_forward_bucket.add_inventory(
            enabled=True,
            destination=s3.InventoryDestination(
                bucket=s3.Bucket.from_bucket_name(
                    self,
                    "HlsInventoryReportsBucket",
                    self.hls_inventory_reports_bucket.bucket_name,
                ),
                prefix=None,
            ),
            inventory_id=self.hls_forward_bucket_inventory_id,
            format=s3.InventoryFormat.PARQUET,
            frequency=s3.InventoryFrequency.DAILY,
            objects_prefix=None,
            optional_fields=["Size", "LastModifiedDate"],
        )

        # Set outputs for use within integration tests
        CfnOutput(
            self,
            "HlsInventoryReportId",
            value=self.hls_forward_bucket_inventory_id,
        )
        CfnOutput(
            self,
            "HlsInventoryReportsBucketName",
            value=self.hls_inventory_reports_bucket.bucket_name,
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
            "LpdaacReconciliationReportsBucketName",
            value=self.lpdaac_reports_bucket.bucket_name,
        )
        CfnOutput(
            self,
            "LpdaacRequestQueueUrl",
            value=lpdaac_request_queue.queue_url,
        )
        CfnOutput(
            self,
            "LpdaacResponseTopicArn",
            value=self.lpdaac_response_topic.topic_arn,
        )

    def make_bucket(self, construct_id: str) -> s3.Bucket:
        return s3.Bucket(
            self,
            construct_id,
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
