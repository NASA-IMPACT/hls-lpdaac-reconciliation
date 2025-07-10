from __future__ import annotations

import csv
import datetime as dt
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Iterator, Sequence

import boto3

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client
    from mypy_boto3_athena.client import AthenaClient


@dataclass
class AthenaQueryResults:
    """Container for Athena query results"""

    query_id: str
    output_location: str
    athena: AthenaClient = field(default_factory=lambda: boto3.client("athena"))
    s3: S3Client = field(default_factory=lambda: boto3.client("s3"))

    column_transforms: ClassVar[Sequence[tuple[str, Callable[[str], Any]]]] = (
        ("short_name", str),
        ("version", str),
        ("filename", str),
        ("size", int),
        ("last_modified", str),
        ("checksum", str),
    )

    def _parse_row(self, row: dict[str, list[dict[str, str]]]) -> dict[str, Any]:
        """Parse a row from a ResultSet"""
        return {
            column: transform(data["VarCharValue"])
            for (column, transform), data in zip(self.column_transforms, row["Data"])
        }

    def _fetch_results(self) -> Iterator[dict[str, Any]]:
        """Fetch results from query, yielding rows as dict"""
        print(f"Fetching results for {self.query_id}")
        paginator = self.athena.get_paginator("get_query_results")

        skipped_header = False
        for i_page, page in enumerate(
            paginator.paginate(
                QueryExecutionId=self.query_id,
                PaginationConfig={
                    "PageSize": 1_000,
                    "StartingToken": None,
                },
            )
        ):
            for row in page["ResultSet"]["Rows"]:
                # First row is the header and we want to throw this away
                if not skipped_header:
                    skipped_header = True
                else:
                    parsed = self._parse_row(row)
                    yield parsed

    def write_results_to_s3(self, destination: str):
        """Read query results and write to CSV on S3

        Per convention this CSV file does NOT have a header.
        """
        bucket, key = destination.replace("s3://", "").split("/", 1)
        fieldnames = [column for column, _ in self.column_transforms]

        with TemporaryDirectory() as tmp_dir:
            tmp_dest = Path(tmp_dir) / "report.rpt"
            with tmp_dest.open("w", newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                i = 0
                for i, row in enumerate(self._fetch_results()):
                    if i % 10_000 == 0:
                        print(f"... wrote row {i} of report")
                    writer.writerow(row)

                print(f"Completed writing {i} rows to report CSV file.")

            if i > 0:
                self.s3.upload_file(
                    str(tmp_dest),
                    bucket,
                    key,
                )
                print(f"Uploaded report with {i} rows to {destination}")
            else:
                print("No rows returned. Nothing to report.")


@dataclass
class AthenaQueryClient:
    """Client to help run Athena queries"""

    catalog_name: str
    database_name: str
    athena: AthenaClient = field(default_factory=lambda: boto3.client("athena"))

    def execute_query(self, sql: str, output_prefix: str) -> AthenaQueryResults:
        """Run and wait on query to complete, returning query results"""
        start_response = self.athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={
                "Catalog": self.catalog_name,
                "Database": self.database_name,
            },
            ResultConfiguration={
                "OutputLocation": output_prefix,
            },
        )
        query_id = start_response["QueryExecutionId"]

        query_output_location = self._wait_query_execution(query_id)

        return AthenaQueryResults(
            query_id=query_id,
            output_location=query_output_location,
            athena=self.athena,
        )

    def _wait_query_execution(
        self, query_id: str, max_attempts: int = 50, delay_seconds: int = 15
    ) -> str:
        """Wait on query execution, returning result location

        Alas boto3 doesn't have a waiter for Athena yet.
        """
        attempt = 0
        while attempt < max_attempts:
            response = self.athena.get_query_execution(
                QueryExecutionId=query_id,
            )
            state = response["QueryExecution"]["Status"]["State"]

            if state == "SUCCEEDED":
                print("Query has completed successfully")
                return response["QueryExecution"]["ResultConfiguration"][
                    "OutputLocation"
                ]
            elif state in ("CANCELLED", "FAILED"):
                raise RuntimeError(f"Query is unsuccessful ({state}). Aborting")
            else:
                print(f"Waiting {delay_seconds} seconds for query_id={query_id}")
                time.sleep(delay_seconds)

            attempt += 1

        raise TimeoutError(f"Timed out executing query_id={query_id}")


def generate_report(
    *,
    catalog_name: str,
    database_name: str,
    table_name: str,
    start_date: dt.datetime,
    end_date: dt.datetime,
    report_output_location: str,
    query_output_prefix: str,
    product_prefixes: Sequence[str] | None,
):
    """Run Athena query to generate report, returning report CSV location

    Parameters
    ----------
    catalog_name
        Athena/Glue catalog name
    database_name
        Athena/Glue database name
    table_name
        HLS S3 inventory table name
    start_date
        Starting date of the report
    end_date
        Ending date of the report
    report_output_location
        S3 path where the final reconciliation report will be written
    query_output_prefix
        S3 prefix for the Athena query results
    product_prefixes
        Optionally, provide a list of the HLS product S3 prefixes that will be
        included in the report. This option exists to support generating reports for
        subsets of products (e.g., just the HLS-VI). If None is provided, all product
        prefixes will be included ("S30", "L30", "S30_VI", "L30_VI").
    """
    athena = AthenaQueryClient(catalog_name=catalog_name, database_name=database_name)

    product_prefixes_regex = f"({'|'.join(product_prefixes)})"
    sql = f"""
    WITH inventory AS (
        SELECT *
        FROM {table_name}
        WHERE dt = (SELECT max(dt) FROM {table_name})
            AND last_modified_date BETWEEN
                TIMESTAMP '{start_date:%Y-%m-%d}'
                AND TIMESTAMP '{end_date:%Y-%m-%d}'
    ),
    parsed AS (
        SELECT regexp_extract(key, '[^/]+$') as filename,
            size,
            date_format(last_modified_date, '%Y-%m-%dT%H:%m:%sZ') AS last_modified,
            'NA' as checksum
        FROM inventory
        WHERE REGEXP_LIKE(
                key,
                '^{product_prefixes_regex}/data/.*(tif|jpg|xml|stac.json)$'
            )
    )
    SELECT
        REGEXP_REPLACE(
            filename,
            '([aA-zZ0-9]+)\\.([aA-zZ0-9]+).*',
            '$1$2'
        ) AS short_name,
        REGEXP_EXTRACT(filename, 'v([0-9]\\.[0-9])', 1) AS version,
        filename,
        size,
        last_modified,
        checksum
    FROM parsed
    """

    result = athena.execute_query(
        sql=sql,
        output_prefix=query_output_prefix,
    )
    result.write_results_to_s3(report_output_location)


def handler(
    event: dict,
    _context: Any,
    *,
    inventory_table_name: str | None = None,
    query_output_prefix: str | None = None,
    report_output_prefix: str | None = None,
    product_version: str | None = None,
) -> None:
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
    included,
    ```json
    {
        "product_prefixes": ["S30", "L30", "S30_VI", "L30_VI"]
    }
    ```
    or to only include HLS-VI products you would submit a payload like,
    ```json
    {
        "product_prefixes": ["S30_VI", "L30_VI"]
    }
    ```

    This Lambda handler depends on some constant values that may be injected
    directly into this function to assist with testing. These values are,

    * `inventory_table_name` overrides the `INVENTORY_TABLE_NAME`
    * `query_output_prefix` overrides the `QUERY_OUTPUT_PREFIX` envvar
    * `report_output_prefix` overrides the `REPORT_OUTPUT_PREFIX` envvar
    * `product_version` overrides the `HLS_PRODUCT_VERSION` envvar
    """
    if report_start_date_str := event.get("report_start_date"):
        report_start_date = dt.datetime.fromisoformat(report_start_date_str)
    else:
        report_start_date = dt.datetime.now() - dt.timedelta(days=2)
    report_end_date = report_start_date + dt.timedelta(days=1)

    inventory_table_name = inventory_table_name or os.environ["INVENTORY_TABLE_NAME"]
    query_output_prefix = query_output_prefix or os.environ["QUERY_OUTPUT_PREFIX"]

    report_output_prefix = (
        report_output_prefix or os.environ["REPORT_OUTPUT_PREFIX"]
    ).rstrip("/")
    if not report_output_prefix.startswith("s3://"):
        raise ValueError(
            "The report output prefix should start with s3://, "
            f"got '{report_output_prefix}'"
        )

    product_prefixes = event.get("product_prefixes", ["S30", "L30", "S30_VI", "L30_VI"])

    product_version = product_version or os.environ["HLS_PRODUCT_VERSION"]
    report_output_location = (
        f"{report_output_prefix}/{report_start_date:%Y%j}/"
        f"HLS_reconcile_{report_start_date:%Y%j}_{product_version}.csv"
    )

    generate_report(
        catalog_name="AwsDataCatalog",
        database_name="default",
        table_name=inventory_table_name,
        start_date=report_start_date,
        end_date=report_end_date,
        report_output_location=report_output_location,
        query_output_prefix=query_output_prefix,
        product_prefixes=product_prefixes,
    )
