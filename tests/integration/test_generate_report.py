# pyright: reportTypedDictNotRequiredAccess=false
from __future__ import annotations

import datetime as dt
import json
from collections.abc import Iterator, Sequence
from itertools import product
from pathlib import Path
from uuid import uuid4

import pandas as pd
import pytest
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    # We dynamically parameterize report_uri values since they must be generated
    # by first generating our inventory fixture.  This indirection allows our
    # report_df fixture (below) to depend on our report_uri fixture, which in
    # turn depends on our inventory fixture, so that our test_report test
    # function can be dynamically parameterized with each report_df.
    if "report_uri" in metafunc.fixturenames:
        metafunc.parametrize(
            "report_uri",
            ["L30", "L30_VI", "S30", "S30_VI"],
            indirect=True,
            scope="session",
        )


def consume_messages(sqs: SQSClient, queue_url: str) -> Iterator[str]:
    timeout = dt.datetime.now() + dt.timedelta(seconds=20)

    # Since we expect to generate multiple report files, each triggering a
    # distinct message, the first call to sqs.receive_message might return a
    # message before all messages are queued, so we'll call repeatedly until we
    # don't get any more messages, or we reach our time limit.
    while (time_remaining := (timeout - dt.datetime.now())) > dt.timedelta():
        for message in sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=time_remaining.seconds,
        ).get("Messages", []):
            receipt = message["ReceiptHandle"]
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)

            # Yield the message payload (we don't care about anything else)
            yield json.loads(message.get("Body", "{}")).get("Message", "")


@pytest.fixture(scope="session")
def hls_bucket_listing(hls_bucket: str) -> Sequence[object]:
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

    for i, product_id, satellite_id, file_extension in product(
        range(100), product_ids, satellite_ids, file_extensions
    ):
        granule_id = (
            f"{product_id}.{satellite_id}.{last_modified_date:%Y%jT%H%M%S}_{i:03d}.v2.0"
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
                "last_modified_date": last_modified_date + pd.Timedelta(seconds=i),
            }
        )

    return records


@pytest.fixture(scope="session")
def inventory_df(
    s3: S3Client,
    hls_bucket: str,
    hls_inventory_reports_id: str,
    hls_inventory_reports_bucket: str,
    hls_bucket_listing: Sequence[object],
    tmp_path_factory: pytest.TempPathFactory,
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
    inventory = pd.DataFrame.from_records(hls_bucket_listing)
    inventory_parquet_key = (
        f"{hls_bucket}/{hls_inventory_reports_id}/data/{uuid4()}.parquet"
    )

    # Part 1 - the report data
    tmp_file = tmp_path_factory.getbasetemp() / "inventory.parquet"
    inventory.to_parquet(tmp_file)
    s3.upload_file(str(tmp_file), hls_inventory_reports_bucket, inventory_parquet_key)

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

    # Now that we've written the inventory report, we'll remove the *.v2.0.json
    # files because we expect our reports for LP DAAC not to contain them.
    # We'll also add a filename column and sort by it, since we expect the
    # generated reports to be sorted by filename.
    return (
        inventory.query("not key.str.endswith('v2.0.json')")
        .assign(filename=lambda df: df["key"].str.rsplit("/", n=1, expand=True)[1])
        .sort_values("filename", ignore_index=True)
    )


@pytest.fixture(scope="session")
def report_uris(
    # Dependency that should eventually result in "report" messages in the queue
    inventory_df: pd.DataFrame,
    sqs: SQSClient,
    lpdaac_request_queue_url: str,
) -> Sequence[str]:
    uris = tuple(
        json.loads(message)["report"]["uri"]
        for message in consume_messages(sqs, lpdaac_request_queue_url)
    )

    # We expect 1 for each of our 4 products (L30, L30_VI, S30, S30_VI)
    assert len(uris) == 4

    return uris


@pytest.fixture
def report_uri(
    report_uris: Sequence[str],
    request: pytest.FixtureRequest,
) -> str:
    # We expect the request parameter to be a product (e.g., "L30"), and we'll
    # look for the report URI corresponding to the product.  This allows us to
    # dynamically parameterize testing report outputs, so each can be tested
    # independently.
    product = request.param
    return next(uri for uri in report_uris if f"{product}_2.0" in uri)


@pytest.fixture
def report_df(s3: S3Client, report_uri: str, tmp_path: Path) -> pd.DataFrame:
    bucket, key = report_uri.removeprefix("s3://").split("/", 1)
    tmp_csv = tmp_path / "report.csv"
    s3.download_file(Bucket=bucket, Key=key, Filename=str(tmp_csv))

    return pd.read_csv(
        tmp_csv,
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


def test_inventory_report_generation(
    inventory_df: pd.DataFrame,
    report_df: pd.DataFrame,
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
    # The report should be for 1 specific product
    short_names: set[str] = set(report_df["short_name"])
    assert len(short_names) == 1
    product = list(short_names)[0].removeprefix("HLS")

    # Select only inventory files related to report's product
    product_inventory_df = inventory_df.query(
        f"key.str.startswith('{product}/')"
    ).reset_index()

    date_diff = product_inventory_df["last_modified_date"] - report_df["last_modified"]

    assert len(report_df) == len(product_inventory_df)
    assert (report_df["version"] == "2.0").all()
    assert (report_df["filename"] == product_inventory_df["filename"]).all()
    assert (report_df["size"] == product_inventory_df["size"]).all()
    # Our output produces time down to only milliseconds (3 decimal places, not 6)
    assert (abs(date_diff) < pd.Timedelta(milliseconds=1)).all()
    assert (report_df["checksum"] == "NA").all()
