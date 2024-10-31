from typing import Optional

from aws_cdk import Duration, Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_event_sources as sources
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_notifications as s3n
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subs
from constructs import Construct


class HlsLpdaacReconciliationStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        hls_inventory_reports_bucket: str,
        hls_forward_bucket: str,
        hls_historical_bucket: str,
        lpdaac_request_topic_arn: str,
        lpdaac_response_topic_arn: str,
        lpdaac_reconciliation_reports_bucket: str,
        notification_email_address: str,
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

        # ----------------------------------------------------------------------
        # Request reconciliation report
        # ----------------------------------------------------------------------

        # Bucket where HLS inventory reports are written.
        inventory_reports_bucket = s3.Bucket.from_bucket_name(
            self, "HlsInventoryReportsBucket", hls_inventory_reports_bucket
        )
        # LPDAAC topic to send notifications of new inventory reports.
        lpdaac_request_topic = sns.Topic.from_topic_arn(
            self, "LpdaacRequestTopic", topic_arn=lpdaac_request_topic_arn
        )
        # Lambda function that publishes message to LPDAAC topic when a new inventory
        # report is created in the inventory reports bucket.
        lpdaac_request_lambda = lambda_.Function(
            self,
            "ReconciliationRequestHandler",
            code=lambda_.Code.from_asset("src", exclude=["**/*.egg-info"]),
            handler="hls_lpdaac_reconciliation/request/index.handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            memory_size=128,
            timeout=Duration.minutes(15),
            environment={
                "LPDAAC_REQUEST_TOPIC_ARN": lpdaac_request_topic_arn,
            },
        )
        # Notify lambda function when new inventory report is created.
        inventory_reports_bucket.add_object_created_notification(
            s3n.LambdaDestination(
                lpdaac_request_lambda  # pyright: ignore[reportArgumentType]
            ),
            s3.NotificationKeyFilter(suffix=".rpt"),
        )
        # Allow lambda function to publish message of new inventory report to the topic.
        lpdaac_request_topic.grant_publish(lpdaac_request_lambda)

        # ----------------------------------------------------------------------
        # Handle reconciliation report response
        # ----------------------------------------------------------------------

        lpdaac_response_lambda = lambda_.Function(
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

        lpdaac_response_topic = sns.Topic.from_topic_arn(
            self, "LpdaacResponseTopic", topic_arn=lpdaac_response_topic_arn
        )

        # Subscribe response lambda function to response topic
        lpdaac_response_lambda.add_event_source(
            sources.SnsEventSource(lpdaac_response_topic)
        )
        lpdaac_response_topic.add_subscription(
            subs.EmailSubscription(
                notification_email_address  # pyright: ignore[reportArgumentType]
            )
        )

        # Allow lambda function to access buckets
        s3.Bucket.from_bucket_name(
            self, "HlsForwardBucket", hls_forward_bucket
        ).grant_read_write(lpdaac_response_lambda)
        s3.Bucket.from_bucket_name(
            self, "HlsHistoricalBucket", hls_historical_bucket
        ).grant_read_write(lpdaac_response_lambda)
        s3.Bucket.from_bucket_name(
            self, "LpdaacReconciliationReports", lpdaac_reconciliation_reports_bucket
        ).grant_read(lpdaac_response_lambda)
