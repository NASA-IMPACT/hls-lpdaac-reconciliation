from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from enum import StrEnum, auto
from functools import reduce
from typing import Any, Mapping, Optional, Sequence, TYPE_CHECKING

import boto3

if TYPE_CHECKING:  # pragma: no cover
    from aws_lambda_typing.events import SNSEvent

from hls_lpdaac_reconciliation.response import (
    decode_collection_id,
    extract_report_location,
    group_granule_ids,
    notification_trigger_key,
)


class Status(StrEnum):
    SKIPPED = auto()
    TRIGGERED = auto()
    MISSING = auto()


s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")


def handler(
    event: SNSEvent,
    _context: Any,
    *,
    hls_forward_bucket: Optional[str] = None,
    hls_historical_bucket: Optional[str] = None,
) -> Mapping[str, Mapping[Status, Sequence[str]]]:
    """Handle AWS SNS message from LPDAAC regarding Cumulus ingestion reconciliation.

    Skip message if the subject of the message contains `"Ok"`.  In this case, there
    are no discrepancies (i.e., LPDAAC successfully ingested all granules that we
    notified them about since the last report).

    Otherwise, do the following:

    - Extract S3 location of JSON reconciliation report from message and read report
    - Extract all granule IDs from report, along with associated collection IDs
    - Check CMR for existence of each granule (search by collection ID and granule ID)
    - If granule is in CMR, do nothing (not sure why, but perhaps this means that
      the granule was successfully ingested after the report was generated)
    - Otherwise, "touch" the granule's notification trigger file (its non-STAC *.json
      file) to trigger a new notification to LPDAAC to try to ingest the granule again.

    Parameters
    ----------
    event:
        SNS event sent from LPDAAC containing message regarding a reconciliation report
    _context:
        not used
    hls_forward_bucket:
        name of the bucket to find the granule's files when original ingestion was
        triggered by "forward" processing (AWS will never supply this; it's to allow
        unit testing without having to monkeypatch `os.environ`)
    hls_historical_bucket:
        name of the bucket to find the granule's files when original ingestion was
        triggered by "historical" processing (AWS will never supply this; it's to allow
        unit testing without having to monkeypatch `os.environ`)

    Returns
    -------
    Mapping from collection ID (`"<SHORT_NAME>___<VERSION>"`) to a sub-mapping from
    granule `Status` to sequence of granule IDs resulting in each status after
    processing.
    """
    sns_message = event["Records"][0]["Sns"]
    subject = sns_message["Subject"]
    print("Subject:", subject)

    if subject and "Ok" in subject:
        # When the subject contains (ends with) "Ok", the message itself
        # indicates that there are no discrepencies, so there's nothing to do.
        # Example: "[External] Rec-Report HLS lp-prod HLS_reconcile_2024240_2.0.rpt Ok"
        return {}

    message = sns_message["Message"]
    report_bucket_name, report_key = extract_report_location(message)
    report = read_report(report_bucket_name, report_key)
    data_bucket_name = (
        hls_historical_bucket or os.environ["HLS_HISTORICAL_BUCKET"]
        if "historical" in report_key
        else hls_forward_bucket or os.environ["HLS_FORWARD_BUCKET"]
    )

    summary = process_report(report, data_bucket_name)
    print(f"Processing summary: {summary}")

    return summary


def read_report(bucket_name: str, key: str) -> Sequence[Mapping[str, Any]]:
    """Read JSON reconciliation report from S3.

    Parameters
    ----------
    bucket_name:
        Name of the bucket containing the JSON reconciliation report file
    key:
        Key of the JSON reconciliation report file

    Returns
    -------
    Reconciliation report as a sequence of sub-reports, one per collection.
    Each collection sub-report is a mapping of the following form (abridged,
    excluding additional elements of no importance to this functionality):

        {
            "<SHORT_NAME>___<VERSION>": {
                "report": {
                    "<FILENAME>": {
                        "granuleId": "<GRANULE_ID>",
                    },
                    ...
                }
            }
        }
    """

    print(f"Reading report from s3://{bucket_name}/{key}")
    obj = s3_resource.Object(bucket_name, key)
    return json.loads(obj.get()["Body"].read().decode("utf-8"))


def process_report(
    report: Sequence[Mapping[str, Any]], data_bucket_name: str
) -> Mapping[str, Mapping[Status, Sequence[str]]]:
    """Trigger reingestion for every granule in a report that is not in CMR.

    Arguments
    ---------
    report:
        Reconciliation report produced by LPDAAC
    data_bucket_name:
        Name of bucket containing HLS granules.  This is where to find a granule's
        "trigger" file for notifying LPDAAC to ingest the granule.  Unfortunately,
        the report from LPDAAC does not contain the source S3 location of each
        granule in the report.

    Returns
    -------
    Mapping from collection ID (`"<SHORT_NAME>___<VERSION>"`) to mapping from granule
    `Status` to sequence of granule IDs resulting in each status after processing:

        {
            "<SHORT_NAME>___<VERSION>": {
                <Status.MISSING: 'missing'>: ["<GRANULE_ID_1>", ..., "<GRANULE_ID_N>"],
                ...
            },
            ...
        }
    """

    granule_ids_per_status_per_collection = {}
    for collection_id, (n_files, granule_ids) in group_granule_ids(report).items():
        print(
            f"{len(granule_ids)} granule ({n_files} file) missing from {collection_id}"
        )

        short_name, version = decode_collection_id(collection_id)
        granule_ids_per_status_per_collection[collection_id] = process_collection(
            short_name=short_name,
            version=version,
            granule_ids=granule_ids,
            data_bucket_name=data_bucket_name,
        )

    return granule_ids_per_status_per_collection


def process_collection(
    *,
    short_name: str,
    version: str,
    granule_ids: Sequence[str],
    data_bucket_name: str,
) -> Mapping[Status, Sequence[str]]:
    """Trigger reingestion for every granule in a list that is not in CMR.

    Arguments
    ---------
    short_name:
        Short name of the collection containing the granules
    version:
        Version of the collection containing the granules
    granule_ids:
        Sequence of IDs of granules to possibly trigger reingestion for
    data_bucket_name:
        Name of source S3 bucket containing the granule files, including the
        "trigger" file for triggering LPDAAC notification

    Returns
    -------
    Mapping from granule `Status` to sequence of granule IDs with the status:

        {
            <Status.MISSING: 'missing'>: ["<GRANULE_ID_1>", ..., "<GRANULE_ID_N>"],
            ...
        }
    """

    def group_by_status(
        acc: Mapping[Status, Sequence[str]], granule_id: str
    ) -> Mapping[Status, Sequence[str]]:
        status = process_granule(
            short_name=short_name,
            version=version,
            granule_id=granule_id,
            data_bucket_name=data_bucket_name,
        )

        return {**acc, status: [*acc.get(status, []), granule_id]}

    return reduce(group_by_status, granule_ids, {})


def process_granule(
    *, short_name: str, version: str, granule_id: str, data_bucket_name: str
) -> Status:
    """Touch S3 notification trigger file for a granule if it is not in CMR.

    Triggers LPDAAC reingestion of the granule by touching the notification
    "trigger" file, which causes a message to be sent to LPDAAC for ingestion
    of the granule that the trigger file belongs to.

    Parameters
    ----------
    short_name:
        collection short name as it exists in the CMR
    version:
        collection version as it exists in the CMR
    granule_id:
        granule UR or producer granule id of a granule
    data_bucket_name:
        name of the bucket that contains the notification trigger file

    Returns
    -------
    status:
        `Status.SKIPPED` if the granule is in the CMR, `Status.TRIGGERED` if it is not
        in the CMR and it's associated "trigger" file was found in the specified data
        bucket (and touched, to trigger re-ingestion), otherwise `Status.MISSING`
        (indicating HLS reprocessing is required)
    """
    if granule_in_cmr(short_name=short_name, version=version, granule_id=granule_id):
        print(f"{granule_id} is already available in CMR. Skipping.")
        return Status.SKIPPED

    if s3_object_exists(data_bucket_name, key := notification_trigger_key(granule_id)):
        s3_client.copy_object(
            Bucket=data_bucket_name,
            Key=key,
            CopySource=f"{data_bucket_name}/{key}",
            MetadataDirective="REPLACE",
        )
        return Status.TRIGGERED

    print(
        f"{granule_id} needs to be resubmitted to the step function execution:"
        f"notification trigger file not found: s3://{data_bucket_name}/{key}"
    )

    return Status.MISSING


def granule_in_cmr(*, short_name: str, version: str, granule_id: str) -> bool:
    """Indicate whether or not metadata for a granule in a collection exists in the CMR.

    Parameters
    ----------
    short_name:
        name of collection that should contain the granule metadata
    version:
        version of collection that should contain the granule metadata
    granule_id:
        granule ur or producer granule id of a granule

    Returns
    -------
    `True` if there is metadata in the CMR for the granule; `False` otherwise.
    """

    params = urllib.parse.urlencode(
        {
            "short_name": short_name,
            "version": version,
            "readable_granule_name": granule_id,
        }
    )
    url = f"https://cmr.earthdata.nasa.gov/search/granules?{params}"

    with urllib.request.urlopen(url) as response:
        return response.headers.get("cmr-hits", "0") != "0"


def s3_object_exists(bucket: str, key: str) -> bool:
    """Return `True` if an S3 object exists; `False` otherwise."""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except s3_client.exceptions.ClientError:
        return False
