import aws_cdk as core
import aws_cdk.assertions as assertions

from cdk import HlsLpdaacReconciliationStack


def test_stack_created():
    app = core.App()
    stack = HlsLpdaacReconciliationStack(
        app,
        "test",
        hls_forward_bucket="forward",
        hls_historical_bucket="historical",
        response_sns_topic_arn="arn:aws:sns:us-east-1:123456789012:MyTopic",
    )
    template = assertions.Template.from_stack(stack)

    assert template


#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
