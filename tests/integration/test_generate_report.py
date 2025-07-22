# pyright: reportTypedDictNotRequiredAccess=false
from __future__ import annotations

import datetime as dt
from itertools import product
from tempfile import NamedTemporaryFile
from uuid import uuid4

import pandas as pd
import pytest
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient


@pytest.fixture
def df_inventory(hls_bucket: str) -> pd.DataFrame:
    product_ids = ["HLS", "HLS-VI"]
    satellite_ids = ["L30", "S30"]
    file_extensions = [
        ".EVI.tif",
        ".NBR.tif",
        "_stac.json",
        ".json",
        ".jpg",
        ".xml",
    ]
    last_modified_date = dt.datetime.now(dt.UTC) - dt.timedelta(days=2)
    records = []

    for i, (product_id, satellite_id, file_extension) in enumerate(
        product(product_ids, satellite_ids, file_extensions)
    ):
        granule_id = (
            f"{product_id}.{satellite_id}.{last_modified_date:%Y%jT%H%M%S}.v2.0"
        )
        key = "/".join(
            [
                f"{satellite_id}_VI" if "VI" in product_id else satellite_id,
                "data",
                f"{last_modified_date:%Y%j}",
                granule_id,
                f"{granule_id}{file_extension}",
            ]
        )
        records.append(
            {
                "bucket": hls_bucket,
                "key": key,
                "size": 1234 + i,
                "last_modified_date": last_modified_date,
            }
        )

    return pd.DataFrame.from_records(records)


@pytest.fixture
def df_fake_inventory(
    s3: S3Client,
    hls_bucket: str,
    hls_inventory_reports_id: str,
    hls_inventory_reports_bucket: str,
    df_inventory: pd.DataFrame,
) -> pd.DataFrame:
    """Create a fake S3 inventory for some bucket.

    The inventory has 3 components:

    - Part 1 - the report data
      - s3://{inventory-bucket}/{data-bucket}/{inventory-id}/data/{uuid4}.parquet
    - Part 2 - a "hive" style layout with symlinks pointing to the data (part 1)
      - s3://{inventory-bucket}/{data-bucket}/{inventory-id}/hive/dt={report-date}/symlink.txt
    - Part 3 - the manifest describing the report
      - s3://{inventory-bucket}/{data-bucket}/{inventory-id}/{report-date}/manifest.json
      - s3://{inventory-bucket}/{data-bucket}/{inventory-id}/{report-date}/manifest.checksum
    """
    report_date = "2025-06-03-01-00"
    inventory_parquet_key = (
        f"{hls_bucket}/{hls_inventory_reports_id}/data/{uuid4()}.parquet"
    )

    # Part 1 - the report data
    with NamedTemporaryFile() as tmp_inventory:
        df_inventory.to_parquet(tmp_inventory.name)
        s3.upload_file(
            tmp_inventory.name,
            hls_inventory_reports_bucket,
            inventory_parquet_key,
        )

    # Part 2 - the "hive" partitioned layout symlinking to the data
    s3.put_object(
        Bucket=hls_inventory_reports_bucket,
        Key=f"{hls_bucket}/{hls_inventory_reports_id}/hive/dt={report_date}/symlink.txt",
        Body=f"s3://{hls_inventory_reports_bucket}/{inventory_parquet_key}".encode(),
    )

    # Part 3 - the manifest and checksum
    for ext in ("json", "checksum"):
        s3.put_object(
            Bucket=hls_inventory_reports_bucket,
            Key=f"{hls_bucket}/{hls_inventory_reports_id}/{report_date}/manifest.{ext}",
            Body=b"test",
        )

    return df_inventory


def test_inventory_report_generation(
    s3: S3Client,
    df_fake_inventory: pd.DataFrame,
    hls_inventory_reports_bucket: str,
    sqs: SQSClient,
    lpdaac_request_queue_url: str,
) -> None:
    """Ensure we generate a HLS product report when a S3 inventory is published.

    This test works by doing the following:

    1. Creating a fake S3 inventory for representative HLS data product files.
    2. Uploading the fake S3 inventory and associated metadata files to the inventory
       bucket.
    3. The report generation Lambda is triggered and generates the report.
    4. Once the report file exists, we download the report and compare against our faked
       S3 inventory data.
    """
    # We expect our report to exclude *.v2.0.json files, which are the files
    # that trigger granule notifications to LP DAAC, but are not included as
    # granule files themselves.
    df_fake_inventory = df_fake_inventory.query(
        "not key.str.endswith('v2.0.json')"
    ).reset_index()

    report_start_date = dt.datetime.now() - dt.timedelta(days=2)
    expected_report_key = (
        f"reconciliation_reports/{report_start_date:%Y%j}/"
        f"HLS_reconcile_{report_start_date:%Y%j}_2.0.rpt"
    )

    # Ensure our request handler generates a message, which we must delete to
    # avoid interfering with other tests.  We do this before any assertions to
    # make sure the message is deleted even if the test fails.

    messages = sqs.receive_message(
        QueueUrl=lpdaac_request_queue_url,
        # We use a max value greater than 1 so that we can assert that there is
        # actually only 1 message in the queue.  That is, if we get more than 1
        # message, then something is wrong.
        MaxNumberOfMessages=2,
        WaitTimeSeconds=20,
    ).get("Messages", [])

    for message in messages:
        receipt = message["ReceiptHandle"]
        sqs.delete_message(QueueUrl=lpdaac_request_queue_url, ReceiptHandle=receipt)

    assert len(messages) == 1

    # Now that we've confirmed that we've received a message regarding creation
    # of a new inventory report (and deleted the message), we can test that the
    # report contents are what we expect them to be.

    with NamedTemporaryFile() as tmp_report_file:
        s3.download_file(
            Bucket=hls_inventory_reports_bucket,
            Key=expected_report_key,
            Filename=tmp_report_file.name,
        )
        df_report = pd.read_csv(
            tmp_report_file.name,
            header=None,
            names=[
                "short_name",
                "version",
                "filename",
                "size",
                "last_modified",
                "checksum",
            ],
            dtype={
                "version": str,
                "size": int,
            },
            parse_dates=["last_modified"],
            # Do NOT convert "NA" checksum values to NaNs (i.e., keep "NA" strings
            # so we can assert that they're all literally "NA" strings).
            keep_default_na=False,
        )

    unique_short_names = {"HLSL30", "HLSS30", "HLSL30_VI", "HLSS30_VI"}
    fake_filename = df_fake_inventory["key"].str.rsplit("/", n=1, expand=True)[1]
    date_diff = df_fake_inventory["last_modified_date"] - df_report["last_modified"]

    assert len(df_report) == len(df_fake_inventory)
    assert set(df_report["short_name"]) == unique_short_names
    assert (df_report["version"] == "2.0").all()
    assert (df_report["filename"] == fake_filename).all()
    assert (df_report["size"] == df_fake_inventory["size"]).all()
    # Our output produces time down to only milliseconds (3 decimal places, not 6)
    assert (abs(date_diff) < pd.Timedelta(milliseconds=1)).all()
    assert (df_report["checksum"] == "NA").all()
