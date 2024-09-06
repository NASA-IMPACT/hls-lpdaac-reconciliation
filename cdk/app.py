#!/usr/bin/env python3
import os
from turtle import forward

import aws_cdk as cdk

from stack import HlsLpdaacReconciliationStack

stack_name = os.environ["HLS_LPDAAC_STACK"]
forward_bucket = os.environ["HLS_LPDAAC_FORWARD_BUCKET"]
historical_bucket = os.environ["HLS_LPDAAC_HISTORICAL_BUCKET"]
response_sns_topic_arn = os.environ["HLS_LPDAAC_RESPONSE_SNS_TOPIC_ARN"]
notify_sns_topic_arn = os.environ["HLS_LPDAAC_NOTIFY_SNS_TOPIC_ARN"]
managed_policy_name = os.getenv("HLS_LPDAAC_MANAGED_POLICY_NAME", "mcp-tenantOperator")

HlsLpdaacReconciliationStack(
    app := cdk.App(),
    f"{stack_name}-lpdaac-reconciliation",
    hls_forward_bucket=forward_bucket,
    hls_historical_bucket=historical_bucket,
    response_sns_topic_arn=response_sns_topic_arn,
    managed_policy_name=managed_policy_name,
)

for k, v in dict(
    Project="hls",
    App="HLS-LPDAAC-Reconciliation",
).items():
    cdk.Tags.of(app).add(k, v, apply_to_launched_instances=True)

app.synth()
