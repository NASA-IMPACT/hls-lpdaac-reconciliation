#!/usr/bin/env python3
import os

import aws_cdk as cdk
from stack import HlsLpdaacReconciliationStack
from stack_it import HlsLpdaacReconciliationStackIT

stack_name = os.environ["HLS_LPDAAC_STACK"]
notification_email_address = os.environ["HLS_LPDAAC_NOTIFICATION_EMAIL_ADDRESS"]
managed_policy_name = os.getenv("HLS_LPDAAC_MANAGED_POLICY_NAME", "mcp-tenantOperator")

stack_it = HlsLpdaacReconciliationStackIT(
    app := cdk.App(),
    f"{stack_name}-lpdaac-reconciliation-it-resources",
    managed_policy_name=managed_policy_name,
)

HlsLpdaacReconciliationStack(
    app,
    f"{stack_name}-lpdaac-reconciliation-it",
    hls_inventory_reports_bucket=stack_it.hls_inventory_reports_bucket.bucket_name,
    hls_forward_bucket=stack_it.hls_forward_bucket.bucket_name,
    hls_historical_bucket=stack_it.hls_historical_bucket.bucket_name,
    lpdaac_request_topic_arn=stack_it.lpdaac_request_topic.topic_arn,
    lpdaac_response_topic_arn=stack_it.lpdaac_response_topic.topic_arn,
    lpdaac_reconciliation_reports_bucket=stack_it.lpdaac_reports_bucket.bucket_name,
    notification_email_address=notification_email_address,
    managed_policy_name=managed_policy_name,
)

for k, v in dict(
    Project="hls",
    App="HLS-LPDAAC-Reconciliation",
).items():
    cdk.Tags.of(app).add(k, v, apply_to_launched_instances=True)

app.synth()
