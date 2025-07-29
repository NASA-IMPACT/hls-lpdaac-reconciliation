# pyright: reportTypedDictNotRequiredAccess=false
from __future__ import annotations

import datetime as dt
import os
import time
from collections.abc import Iterable, Iterator, Sequence
from itertools import groupby
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, Final

import boto3

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_athena.client import AthenaClient
    from mypy_boto3_athena.type_defs import RowTypeDef
    from mypy_boto3_s3.client import S3Client

from hls_lpdaac_reconciliation.generate_report import as_records, parse_date, to_csv

_DEFAULT_PRODUCT_PREFIXES: Final[Sequence[str]] = ("S30", "L30", "S30_VI", "L30_VI")


def fetch_rows(athena: AthenaClient, query_id: str) -> Iterator[RowTypeDef]:
    """Fetch rows from a query as "records" (dicts).

    Assumes the query has already completed successfully.
    """

    for page in athena.get_paginator("get_query_results").paginate(
        QueryExecutionId=query_id,
        PaginationConfig={"PageSize": 1_000},
    ):
        yield from page["ResultSet"]["Rows"]


def write_records_to_s3(
    s3: S3Client,
    records: Iterable[dict[str, str]],
    object_url: str,
) -> None:
    """Read query results and write to CSV on S3.

    Per convention this CSV file does NOT have a header.
    """
    bucket, key = object_url.removeprefix("s3://").split("/", 1)

    with TemporaryDirectory() as tmp_dir:
        tmp_dest = Path(tmp_dir) / "report.rpt"

        if nrows := to_csv(tmp_dest, records):
            print(f"Completed writing {nrows} rows to report CSV file.")
            s3.upload_file(str(tmp_dest), bucket, key)
            print(f"Uploaded report with {nrows} rows to {object_url}")
        else:
            print("No rows returned. Nothing to see here.")


def execute_query(
    athena: AthenaClient,
    *,
    catalog: str,
    database: str,
    output_prefix: str,
    sql: str,
) -> str:
    """Run and wait on query to complete, returning the query execution ID."""

    start_response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Catalog": catalog, "Database": database},
        ResultConfiguration={"OutputLocation": output_prefix},
    )
    query_id = start_response["QueryExecutionId"]
    await_query_execution(athena, query_id)

    return query_id


def await_query_execution(
    athena: AthenaClient,
    query_id: str,
    max_attempts: int = 60,
    delay_seconds: int = 5,
) -> None:
    """Wait on query execution, returning result location.

    Alas boto3 doesn't have a waiter for Athena yet.
    """
    for _ in range(max_attempts):
        response = athena.get_query_execution(QueryExecutionId=query_id)
        state = response["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            print("Query has completed successfully")
            return

        if state in {"CANCELLED", "FAILED"}:
            reason = response["QueryExecution"]["Status"]["StateChangeReason"]
            msg = f"Query was unsuccessful ({state}): {reason}"
            raise RuntimeError(msg)

        print(f"Waiting {delay_seconds} seconds for query_id={query_id}")
        time.sleep(delay_seconds)

    raise TimeoutError(f"Timed out executing query_id={query_id}")


def generate_report(
    *,
    athena: AthenaClient,
    s3: S3Client,
    table: str,
    start_date: dt.date,
    end_date: dt.date,
    report_output_url_format: str,
    query_output_prefix: str,
    catalog: str = "AwsDataCatalog",
    database: str = "default",
    product_prefixes: Sequence[str] | None = None,
) -> None:
    """Run Athena query to generate report, returning report CSV location

    Parameters
    ----------
    athena
        boto3 Athena client to use
    s3
        boto3 S3 client to use
    catalog
        Athena/Glue catalog name (default: "AwsDataCatalog")
    database
        Athena/Glue database name (default: "default")
    table
        HLS S3 inventory table name
    start_date
        Starting date of the report
    end_date
        Ending date of the report
    report_output_url_format
        Format string for the S3 URL where each reconciliation report will be
        written, with a placeholder for `product` (to be replaced with `"L30"`,
        `"L30_VI"`, `"S30"`, or `"S30_VI"`)
    query_output_prefix
        S3 prefix for the Athena query results
    product_prefixes
        Optionally, provide a list of the HLS product S3 prefixes that will be
        included in the report. This option exists to support generating reports for
        subsets of products (e.g., just the HLS-VI). If None is provided, all product
        prefixes will be included ("S30", "L30", "S30_VI", "L30_VI").
    """
    print(f"Generating report for {start_date} to {end_date}")

    product_prefixes = product_prefixes or _DEFAULT_PRODUCT_PREFIXES
    key_pattern = rf"^({'|'.join(product_prefixes)})/data/.*(tif|jpg|xml|stac\.json)$"
    sql = rf"""
    SELECT
        -- short_name is the concatenation of "HLS" and the first component of the key.
        -- For example, "HLSL30_VI" for key "L30_VI/data/2025067/HLS-VI.L30.T...".
        regexp_replace(key, '^([^/]+).*', 'HLS$1') AS short_name,
        regexp_extract(key, 'v([0-9]+(?:\.[0-9]+)*)', 1) AS version,
        regexp_extract(key, '[^/]+$') AS filename,
        size,
        -- Use format_datetime, which allows us to include 3 decimal places for seconds
        -- value (based upon Java's formatting capabilities), whereas date_format
        -- supports only 6 decimal places (based upon Python's formatting).  This is to
        -- guarantee we output timestamps formatted exactly as previously done, so that
        -- we don't potentially cause problems on the LP DAAC side that parses it.
        -- See https://trino.io/docs/current/functions/datetime.html#java-date-functions
        format_datetime(last_modified_date, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''') AS last_modified,
        'NA' as checksum
    FROM {table}
    WHERE dt = (SELECT max(dt) FROM {table})
        AND last_modified_date BETWEEN
            TIMESTAMP '{start_date:%Y-%m-%d}'
            AND TIMESTAMP '{end_date:%Y-%m-%d}'
        AND regexp_like(key, '{key_pattern}')
    -- Order by key (and thus, product) first, so we can leverage itertools.groupby
    -- to efficiently write a separate report file for each product.  This also
    -- keeps related granule files adjacent to each other to help LP DAAC.
    ORDER BY key, last_modified_date
    """

    query_id = execute_query(
        athena,
        catalog=catalog,
        database=database,
        sql=sql,
        output_prefix=query_output_prefix,
    )
    records = as_records(fetch_rows(athena, query_id))

    # groupby *assumes* items are in sorted order, so it can remain completely
    # lazy, which is why we sorted first by short_name in our query above.
    for product, product_records in groupby(
        records, lambda rec: rec["short_name"].removeprefix("HLS")
    ):
        product_report_url = report_output_url_format.format(product=product)
        write_records_to_s3(s3, product_records, product_report_url)


def handler(event: dict[str, Any], _context: object) -> None:
    """Generate report based on S3 inventory for a given date.

    This function supports overriding the start date of the inventory report
    by passing an ISO formatted date in the event payload. The report will always
    end 1 day after the start date.

    By default this function will generate a report from (TODAY - 2) to (TODAY - 1).

    For example,

    ```json
    {
      "report_start_date": "2025-07-01"
    }
    ```

    You can also override the product specific S3 prefixes included in the
    report by specifying in the event handler. By default all prefixes are
    included:

    ```json
    {
        "product_prefixes": ["S30", "L30", "S30_VI", "L30_VI"]
    }
    ```

    To only include HLS-VI products you would submit a payload like:

    ```json
    {
        "product_prefixes": ["S30_VI", "L30_VI"]
    }
    ```

    This Lambda handler depends on some environment variables:

    - `INVENTORY_TABLE_NAME` (name of a table within the "default" database)
    - `QUERY_OUTPUT_PREFIX` (S3 key prefix for query output)
    - `REPORT_OUTPUT_PREFIX` (S3 key prefix for report output -- destined for LP DAAC)
    - `HLS_PRODUCT_VERSION` (HLS version, excluding the `v` prefix -- e.g., "2.0")
    """
    report_start_date = (
        parse_date(report_start_date_str)
        if (report_start_date_str := event.get("report_start_date"))
        else (dt.datetime.now() - dt.timedelta(days=2)).date()
    )
    report_end_date = report_start_date + dt.timedelta(days=1)
    inventory_table_name = os.environ["INVENTORY_TABLE_NAME"]
    query_output_prefix = os.environ["QUERY_OUTPUT_PREFIX"]
    report_output_prefix = (os.environ["REPORT_OUTPUT_PREFIX"]).rstrip("/")

    if not report_output_prefix.startswith("s3://"):
        msg = f"Report output prefix must start with s3://: {report_output_prefix!r}"
        raise ValueError(msg)

    product_prefixes: Sequence[str] | None = event.get("product_prefixes")
    product_version = os.environ["HLS_PRODUCT_VERSION"]
    report_extension = os.getenv("HLS_LPDAAC_REPORT_EXTENSION", ".rpt")
    report_output_url_format = (
        f"{report_output_prefix}/{report_start_date:%Y%j}/"
        f"HLS_reconcile_{report_start_date:%Y%j}_{{product}}"
        f"_{product_version}{report_extension}"
    )

    generate_report(
        athena=boto3.client("athena"),
        s3=boto3.client("s3"),
        table=inventory_table_name,
        start_date=report_start_date,
        end_date=report_end_date,
        report_output_url_format=report_output_url_format,
        query_output_prefix=query_output_prefix,
        product_prefixes=product_prefixes,
    )
