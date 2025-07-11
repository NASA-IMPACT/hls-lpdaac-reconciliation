from __future__ import annotations

import datetime as dt
from itertools import product
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterator
from uuid import uuid4

import pandas as pd
import pytest
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient


@pytest.fixture
def clean_hls_inventory_reports_bucket(
    s3: S3Client,
    hls_bucket: str,
    hls_inventory_reports_id: str,
    hls_inventory_reports_bucket: str,
) -> Iterator[str]:
    """Yield the HLS inventory reports bucket and clean up afterwards"""
    yield hls_inventory_reports_bucket

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=hls_inventory_reports_bucket):
        for obj in page.get("Contents", []):
            s3.delete_object(Bucket=hls_inventory_reports_bucket, Key=obj["Key"])


def create_fake_s3_inventory_data(hls_bucket: str) -> pd.DataFrame:
    product_ids = ["HLS", "HLS-VI"]
    satellite_ids = ["L30", "S30"]
    file_extensions = [
        ".EVI.tif",
        ".NBR.tif",
        "_stac.json",
        ".jpg",
        ".xml",
    ]
    last_modified_date = dt.datetime.now(dt.UTC) - dt.timedelta(days=2)

    records = []
    for i, (product_id, satellite_id, file_extension) in enumerate(
        product(product_ids, satellite_ids, file_extensions)
    ):
        prefix = f"{satellite_id}_VI" if "VI" in product_id else satellite_id
        granule_id = (
            f"{product_id}.{satellite_id}.{last_modified_date:%Y%jT%H%M%S}.v2.0"
        )
        records.append(
            {
                "bucket": hls_bucket,
                "key": f"{prefix}/data/{last_modified_date:%Y%j}/{granule_id}/{granule_id}{file_extension}",
                "size": 1234 + i,
                "last_modified_date": last_modified_date,
            }
        )

    return pd.DataFrame.from_records(records)


def create_fake_s3_inventory(
    s3: S3Client,
    hls_bucket: str,
    hls_inventory_reports_id: str,
    hls_inventory_reports_bucket: str,
) -> pd.DataFrame:
    """Create a fake S3 inventory for some bucket

    The inventory has 3 components,
    ```
    # Part 1 - the report data
    s3://{inventory-bucket}/{data-bucket}/{inventory-id}/data/{uuid4}.parquet
    ...
    # Part 2 - a "hive" style layout with symlinks pointing to the data (part 1)
    s3://{inventory-bucket}/{data-bucket}/{inventory-id}/hive/dt={report-date}/symlink.txt
    ...
    # Part 3 - the manifest describing the report
    s3://{inventory-bucket}/{data-bucket}/{inventory-id}/{report-date}/manifest.json
    s3://{inventory-bucket}/{data-bucket}/{inventory-id}/{report-date}/manifest.checksum
    ...
    ```
    """
    df_inventory = create_fake_s3_inventory_data(hls_bucket)

    report_date = "2025-06-03"
    inventory_parquet_key = (
        f"{hls_bucket}/{hls_inventory_reports_id}/data/{uuid4()}.parquet"
    )
    with TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # Part 1 - the report data
        tmp_inventory = tmp_path / "inventory.parquet"
        df_inventory.to_parquet(tmp_inventory)
        s3.upload_file(
            str(tmp_inventory),
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
    hls_bucket: str,
    hls_inventory_reports_id: str,
    clean_hls_inventory_reports_bucket: str,
    sqs: SQSClient,
    lpdaac_request_queue_url: str,
) -> None:
    """Ensure we generate a HLS product report when a S3 inventory is published

    This test works by,
    1. Creating a fake S3 inventory for representative HLS data product files.
    2. Uploading the fake S3 inventory and associated metadata files to the inventory
       bucket.
    3. The report generation Lambda is triggered and generates the report.
    4. Once the report file exists, we download the report and compare against our faked
       S3 inventory data.
    """
    df_fake_inventory = create_fake_s3_inventory(
        s3,
        hls_bucket,
        hls_inventory_reports_id,
        clean_hls_inventory_reports_bucket,
    )

    report_start_date = dt.datetime.today() - dt.timedelta(days=2)
    expected_report_key = (
        f"reconciliation_reports/{report_start_date:%Y%j}/"
        f"HLS_reconcile_{report_start_date:%Y%j}_2.0.rpt"
    )

    waiter = s3.get_waiter("object_exists")
    try:
        waiter.wait(
            Bucket=clean_hls_inventory_reports_bucket,
            Key=expected_report_key,
            WaiterConfig={
                "Delay": 10,
                "MaxAttempts": 30,
            },
        )
    except Exception:
        raise TimeoutError("Timed out waiting for report file to exist")

    with TemporaryDirectory() as tmp_dir:
        tmp_report_path = Path(tmp_dir) / "report.rpt"
        s3.download_file(
            Bucket=clean_hls_inventory_reports_bucket,
            Key=expected_report_key,
            Filename=str(tmp_report_path),
        )
        df_report = pd.read_csv(
            tmp_report_path,
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
        )

    assert df_report.shape[0] == df_fake_inventory.shape[0]
    assert set(df_report["short_name"].unique()) == {
        "HLSL30",
        "HLSS30",
        "HLS-VIL30",
        "HLS-VIS30",
    }
    assert (df_report["version"] == "2.0").all()
    assert (
        df_report["filename"]
        == df_fake_inventory["key"].str.rsplit("/", n=1, expand=True)[1]
    ).all()
    assert (df_report["size"] == df_fake_inventory["size"]).all()
    assert (
        (df_report["last_modified"] - df_fake_inventory["last_modified_date"])
        < pd.Timedelta(seconds=1)  # we lose miliseconds in Athena report
    ).all()
    assert df_report["checksum"].isna().all()

    # Ensure our request handler also generates a message, which we
    # should delete to avoid interfering with other tests
    result = sqs.receive_message(
        QueueUrl=lpdaac_request_queue_url, MaxNumberOfMessages=2, WaitTimeSeconds=20
    )
    messages = result.get("Messages", [])

    for message in messages:
        receipt = message["ReceiptHandle"]  # pyright: ignore[reportTypedDictNotRequiredAccess]
        sqs.delete_message(QueueUrl=lpdaac_request_queue_url, ReceiptHandle=receipt)
    assert len(messages) == 1
