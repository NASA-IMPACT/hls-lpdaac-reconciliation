#!/usr/bin/env python3
import os

import aws_cdk as cdk
from aws_cdk import aws_ssm as ssm
from stack import HlsLpdaacReconciliationStack

stack_name = os.environ["HLS_LPDAAC_STACK"]
forward_bucket = os.environ["HLS_LPDAAC_FORWARD_BUCKET"]
historical_bucket = os.environ["HLS_LPDAAC_HISTORICAL_BUCKET"]
response_sns_topic_arn = os.environ["HLS_LPDAAC_RESPONSE_SNS_TOPIC_ARN"]
notify_sns_topic_arn = os.environ["HLS_LPDAAC_NOTIFY_SNS_TOPIC_ARN"]
managed_policy_name = os.getenv("HLS_LPDAAC_MANAGED_POLICY_NAME", "mcp-tenantOperator")

# TODO: create HlsLpdaacReconciliationStackIT that exposes resources expected
# by HlsLpdaacReconciliationStack:
#
# forward bucket (data bucket representing where HLS granules reside)
# historical_bucket (same as above, but for historical data)
# response sns topic (for reconciliation report messages that trigger the lambda func)
#
# We don't actually want to use the env vars above for these resources.

stack = HlsLpdaacReconciliationStack(
    app := cdk.App(),
    f"{stack_name}-lpdaac-reconciliation-it",
    hls_forward_bucket=forward_bucket,
    hls_historical_bucket=historical_bucket,
    response_sns_topic_arn=response_sns_topic_arn,
    managed_policy_name=managed_policy_name,
)

ssm.StringParameter(
    stack,
    "reconciliation-response-function-name",
    string_value=stack.response_lambda.function_name,
    parameter_name=("/hls/tests/hls-lpdaac-reconciliation/response-function-name"),
)

for k, v in dict(
    Project="hls",
    App="HLS-LPDAAC-Reconciliation",
).items():
    cdk.Tags.of(app).add(k, v, apply_to_launched_instances=True)

app.synth()
