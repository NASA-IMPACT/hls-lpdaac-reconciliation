#!/usr/bin/env python3
import os

import aws_cdk as cdk
from aws_cdk import aws_ssm as ssm
from stack import HlsLpdaacReconciliationStack

stack_name = os.environ["HLS_LPDAAC_STACK"]
managed_policy_name = os.getenv("HLS_LPDAAC_MANAGED_POLICY_NAME", "mcp-tenantOperator")

stack = HlsLpdaacReconciliationStack(
    app := cdk.App(),
    f"{stack_name}-lpdaac-reconciliation-it",
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
