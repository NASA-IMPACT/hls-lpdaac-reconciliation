#!/usr/bin/env python3
import os

import aws_cdk as cdk
from aws_cdk import aws_ssm as ssm
from stack import HlsLpdaacReconciliationStack
from stack_it import HlsLpdaacReconciliationStackIT

stack_name = os.environ["HLS_LPDAAC_STACK"]
managed_policy_name = os.getenv("HLS_LPDAAC_MANAGED_POLICY_NAME", "mcp-tenantOperator")

stack_it = HlsLpdaacReconciliationStackIT(
    app := cdk.App(),
    f"{stack_name}-lpdaac-reconciliation-it-resources",
    managed_policy_name=managed_policy_name,
)

stack = HlsLpdaacReconciliationStack(
    app,
    f"{stack_name}-lpdaac-reconciliation-it",
    hls_forward_bucket=stack_it.hls_forward_bucket.bucket_name,
    hls_historical_bucket=stack_it.hls_historical_bucket.bucket_name,
    response_sns_topic_arn=stack_it.response_sns_topic.topic_arn,
    managed_policy_name=managed_policy_name,
)

for k, v in dict(
    Project="hls",
    App="HLS-LPDAAC-Reconciliation",
).items():
    cdk.Tags.of(app).add(k, v, apply_to_launched_instances=True)

app.synth()
