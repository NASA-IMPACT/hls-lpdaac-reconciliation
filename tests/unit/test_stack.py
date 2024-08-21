import aws_cdk as core
import aws_cdk.assertions as assertions

from hls_lpdaac_reconciliation import HlsLpdaacReconciliationStack


def test_stack_created():
    app = core.App()
    stack = HlsLpdaacReconciliationStack(app, "test")
    template = assertions.Template.from_stack(stack)

    assert template


#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
