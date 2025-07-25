from __future__ import annotations

import pytest
from aws_lambda_typing.events import SNSEvent
from mypy_boto3_s3.service_resource import Bucket, Object


@pytest.mark.vcr()
def test_lpdaac_forward_handler(
    sns_event_discrepancies: SNSEvent,
    s3_bucket: Bucket,
    s3_trigger_object: Object,
) -> None:
    # Import here (rather than at top level) to ensure AWS mocks are established.
    # See http://docs.getmoto.org/en/latest/docs/getting_started.html#what-about-those-pesky-imports
    from hls_lpdaac_reconciliation.response.index import handler, Status

    results = handler(sns_event_discrepancies, None, hls_forward_bucket=s3_bucket.name)

    assert results == {
        "HLSL30___2.0": {},
        "HLSS30___2.0": {
            # "trigger" file exists in forward bucket (as written for
            # the s3_trigger_object test fixture)
            Status.TRIGGERED: 1,
            # "trigger" files do NOT exist in forward bucket
            Status.MISSING: 2,
        },
    }


@pytest.mark.vcr()
def test_lpdaac_historical_handler(
    sns_event_discrepancies_historical: SNSEvent,
    s3_bucket: Bucket,
    s3_trigger_object: Object,
) -> None:
    # Import here (rather than at top level) to ensure AWS mocks are established.
    # See http://docs.getmoto.org/en/latest/docs/getting_started.html#what-about-those-pesky-imports
    from hls_lpdaac_reconciliation.response.index import handler, Status

    results = handler(
        sns_event_discrepancies_historical, None, hls_historical_bucket=s3_bucket.name
    )

    assert results == {
        "HLSL30___2.0": {},
        "HLSS30___2.0": {
            # "trigger" file exists in forward bucket (as written for the
            # s3_trigger_object test fixture)
            Status.TRIGGERED: 1,
            # "trigger" files do NOT exist in forward bucket
            Status.MISSING: 2,
        },
    }


def test_no_discrepancies(
    sns_event_no_discrepancies: SNSEvent,
    s3_bucket: Bucket,
) -> None:
    from hls_lpdaac_reconciliation.response.index import handler

    # handler should return immediately with an empty object since the message's
    # subject contains "Ok"
    assert {} == handler(
        sns_event_no_discrepancies, None, hls_forward_bucket=s3_bucket.name
    )
