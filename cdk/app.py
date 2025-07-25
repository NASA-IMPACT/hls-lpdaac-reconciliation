#!/usr/bin/env python3
import os

import aws_cdk as cdk

from stack import HlsLpdaacReconciliationStack

stack_name = os.environ["HLS_LPDAAC_STACK"]
inventory_reports_id = os.environ["HLS_LPDAAC_INVENTORY_REPORTS_ID"]
inventory_reports_bucket = os.environ["HLS_LPDAAC_INVENTORY_REPORTS_BUCKET"]
reconciliation_reports_bucket = os.environ["HLS_LPDAAC_RECONCILIATION_REPORTS_BUCKET"]
forward_bucket = os.environ["HLS_LPDAAC_FORWARD_BUCKET"]
historical_bucket = os.environ["HLS_LPDAAC_HISTORICAL_BUCKET"]
request_topic_arn = os.environ["HLS_LPDAAC_REQUEST_TOPIC_ARN"]
response_topic_arn = os.environ["HLS_LPDAAC_RESPONSE_TOPIC_ARN"]
notification_email_address = os.environ["HLS_LPDAAC_NOTIFICATION_EMAIL_ADDRESS"]
managed_policy_name = os.getenv("HLS_LPDAAC_MANAGED_POLICY_NAME", "mcp-tenantOperator")
# Add ability to override report extension to prevent triggering LPDAAC notifications.
# This is used to help side by side test
report_extension = os.getenv("HLS_LPDAAC_REPORT_SUFFIX", ".rpt")

HlsLpdaacReconciliationStack(
    app := cdk.App(),
    f"{stack_name}-lpdaac-reconciliation",
    hls_inventory_reports_id=inventory_reports_id,
    hls_inventory_reports_bucket=inventory_reports_bucket,
    hls_forward_bucket=forward_bucket,
    hls_historical_bucket=historical_bucket,
    lpdaac_request_topic_arn=request_topic_arn,
    lpdaac_response_topic_arn=response_topic_arn,
    lpdaac_reconciliation_reports_bucket=reconciliation_reports_bucket,
    notification_email_address=notification_email_address,
    report_extension=report_extension,
    managed_policy_name=managed_policy_name,
)

for k, v in dict(
    Project="hls",
    App="HLS-LPDAAC-Reconciliation",
).items():
    cdk.Tags.of(app).add(k, v, apply_to_launched_instances=True)

app.synth()
