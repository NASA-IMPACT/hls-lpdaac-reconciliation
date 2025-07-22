from typing import Any, cast

from aws_cdk import Duration, RemovalPolicy, Stack
from aws_cdk import aws_glue as glue
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
        hls_inventory_reports_id: str,
        hls_inventory_reports_bucket: str,
        hls_forward_bucket: str,
        hls_historical_bucket: str,
        lpdaac_request_topic_arn: str,
        lpdaac_response_topic_arn: str,
        lpdaac_reconciliation_reports_bucket: str,
        notification_email_address: str,
        report_extension: str = ".rpt",
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

        # ----------------------------------------------------------------------
        # Generate HLS product inventory report
        # ----------------------------------------------------------------------

        self.inventory_table = self.make_inventory_table(
            location=f"s3://{hls_inventory_reports_bucket}/{hls_forward_bucket}/{hls_inventory_reports_id}/hive",
        )
        inventory_table_name = cast(
            glue.CfnTable.TableInputProperty, self.inventory_table.table_input
        ).name

        # Bucket where HLS inventory reports are written.
        inventory_reports_bucket = s3.Bucket.from_bucket_name(
            self, "HlsInventoryReportsBucket", hls_inventory_reports_bucket
        )

        # Lambda function that generates a "rpt" report file derived from an Athena
        # query against the HLS product S3 inventory.
        inventory_report_lambda = lambda_.Function(
            self,
            "InventoryReportHandler",
            code=lambda_.Code.from_asset("src", exclude=["**/*.egg-info"]),
            handler="hls_lpdaac_reconciliation/generate_report/index.handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            memory_size=512,
            timeout=Duration.minutes(15),
            environment={
                "QUERY_OUTPUT_PREFIX": f"s3://{hls_inventory_reports_bucket}/queries",
                "REPORT_OUTPUT_PREFIX": f"s3://{hls_inventory_reports_bucket}/reconciliation_reports",
                "HLS_PRODUCT_VERSION": "2.0",
                "HLS_LPDAAC_REPORT_EXTENSION": report_extension,
                "INVENTORY_TABLE_NAME": inventory_table_name,  # type: ignore
            },
        )
        inventory_reports_bucket.grant_read_write(inventory_report_lambda)
        inventory_report_lambda.role.add_managed_policy(  # type: ignore
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonAthenaFullAccess")
        )

        # Trigger inventory report as soon as the S3 inventory report has been published.
        # We assume that the checksum of the manifest can act as a "sentinel file".
        inventory_report_lambda.add_event_source(
            sources.S3EventSourceV2(
                inventory_reports_bucket,
                events=[s3.EventType.OBJECT_CREATED],
                filters=[
                    s3.NotificationKeyFilter(
                        prefix=f"{hls_forward_bucket}/{hls_inventory_reports_id}/",
                        suffix="manifest.checksum",
                    ),
                ],
            )
        )

        # ----------------------------------------------------------------------
        # Request reconciliation report
        # ----------------------------------------------------------------------

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
            memory_size=4096,
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

    def make_inventory_table(self, *, location: str) -> glue.CfnTable:
        from aws_cdk.aws_glue import CfnTable

        # See https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory-athena-query.html
        table = CfnTable(
            self,
            "hls_inventory",
            catalog_id=self.account,
            database_name="default",
            table_input=CfnTable.TableInputProperty(
                # Table names cannot contain dashes, so replace them with underscores
                name=f"hls_inventory_{self.stack_name.replace('-', '_')}",
                owner="hadoop",
                table_type="EXTERNAL_TABLE",
                parameters={
                    "projection.enabled": "true",
                    "projection.dt.type": "date",
                    "projection.dt.format": "yyyy-MM-dd-HH-mm",
                    "projection.dt.range": "2025-06-03-00-00,NOW",
                    "projection.dt.interval": "1",
                    "projection.dt.interval.unit": "HOURS",
                },
                partition_keys=[
                    CfnTable.ColumnProperty(name="dt", type="string"),
                ],
                storage_descriptor=CfnTable.StorageDescriptorProperty(
                    columns=[
                        CfnTable.ColumnProperty(name="bucket", type="string"),
                        CfnTable.ColumnProperty(name="key", type="string"),
                        CfnTable.ColumnProperty(name="size", type="bigint"),
                        CfnTable.ColumnProperty(
                            name="last_modified_date", type="timestamp"
                        ),
                    ],
                    input_format="org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat",
                    location=location,
                    number_of_buckets=-1,
                    output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    serde_info=CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        parameters={
                            "serialization.format": "1",
                        },
                    ),
                ),
            ),
        )

        table.apply_removal_policy(RemovalPolicy.DESTROY)

        return table
