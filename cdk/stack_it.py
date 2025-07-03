from typing import Any, cast

from aws_cdk import CfnOutput, RemovalPolicy, Stack
from aws_cdk import aws_glue as glue
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

        self.inventory_table = self.make_inventory_table(
            location="s3://impact-hls-inventories/hls-global-v2-forward/HLS_data_products_parquet/hive",
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

        # Set outputs for use within integration tests

        table_input = cast(
            glue.CfnTable.TableInputProperty, self.inventory_table.table_input
        )

        CfnOutput(
            self,
            "HlsInventoryTable",
            value=str(table_input.name),
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
                    "projection.dt.format": "yyyy-MM-dd",
                    "projection.dt.range": "2025-06-03,NOW",
                    "projection.dt.interval": "1",
                    "projection.dt.interval.unit": "DAYS",
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
