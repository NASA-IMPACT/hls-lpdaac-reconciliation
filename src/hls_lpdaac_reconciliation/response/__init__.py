from __future__ import annotations

import re
from collections.abc import Callable
from typing import TYPE_CHECKING, Literal, Mapping, Sequence, TypeVar

K = TypeVar("K")
V = TypeVar("V")
W = TypeVar("W")


if TYPE_CHECKING:
    from hls_lpdaac_reconciliation.response.index import Status

GRANULE_ID_PATTERN = re.compile(r"^(?P<granule_id>.+\.v\d(?:\.\d)*)")


def extract_report_location(message: str) -> tuple[str, str]:
    """Extract AWS S3 bucket and key of reconciliation report from SNS message.

    Expect message to be a multi-line string with AWS S3 location of the report
    embedded in the message in the form "Report available at BUCKET/KEY.", where
    BUCKET is the name of the bucket containing the report, and KEY is the full
    object key of the report within the bucket.

    Returns
    -------
    (bucket_name, key)
        tuple indicating AWS S3 location of reconciliation report

    Raises
    ------
    ValueError
        if the message does not contain text indicating a report location
    """

    if match := re.search(
        r"Report\s+available\s+at\s+(?P<loc>.+)[.]", message, re.MULTILINE
    ):
        location = match["loc"]
        bucket, key = location.split("/", 1)

        return bucket, key

    raise ValueError(f"Cannot determine report location from message: '{message}'")


def group_granule_ids(
    report: Sequence[
        Mapping[
            # collection_id -> {"report": files}
            str,
            Mapping[
                Literal["report"],
                # filename -> {"granuleId": granule_id}
                Mapping[str, Mapping[Literal["granuleId"], str]],
            ],
        ]
    ],
) -> Mapping[str, tuple[int, Sequence[str]]]:
    """Group unique granule IDs by collection from a report.

    A report must be structured like so (keys within the structure other than those
    shown here are ignored):

        [
            {
                "<SHORT_NAME>___<VERSION>": {
                    "report": {
                        "<FILENAME>": {
                            "granuleId": "<GRANULE_ID>"
                        },
                        ...
                    }
                }
            },
            ...
        ]

    where:

    - `<SHORT_NAME>`: short name of a collection (e.g., `HLSS30`) as named in the CMR
    - `<VERSION>`: version of the collection (e.g., `2.0`)
    - `<FILENAME>`: base filename of a file within a granule
    - `<GRANULE_ID>`: ID of the granule that contains the file

    Assumes `<SHORT_NAME>___<VERSION>` values are unique across all items within the
    report sequence, and the separator is a sequence of 3 underscores (`_`), as per
    Cumulus convention.

    Returns the total count of files and all of the unique `<GRANULE_ID>` values
    (as a sorted tuple), grouped by unique collection ID:

        {
            "<SHORT_NAME>___<VERSION>": (<FILE COUNT>, ("<GRANULE_ID>", ...)),
            ...
        }
    """
    return {
        collection_id: (
            len(collection_info["report"]),
            tuple(
                sorted(
                    {
                        granule_id_for_file(filename)
                        for filename in collection_info["report"]
                    }
                )
            ),
        )
        for collection_report in report
        for collection_id, collection_info in collection_report.items()
    }


def granule_id_for_file(filename: str) -> str:
    """Determine the granule ID for a file."""
    # Example: HLS.S30.T15XWH.2024237T194859.v2.0_stac.json
    # Match everything through to the version number vX[.Y[...]] (e.g., v2.0),
    # ignoring all characters following the version number.
    if not (m := re.match(GRANULE_ID_PATTERN, filename)):
        raise ValueError(f"Unable to determine granule ID for file {filename!r}")

    return m["granule_id"]


def notification_trigger_key(granule_id: str) -> str:
    """Returns the key for the S3 notification trigger object for a granule."""
    # Example: HLS.S30.T15XWH.2024237T194859.v2.0
    # 'HLS', 'S30', 'T15XWH', '2024237T194859', ['v2', '0']
    prefix, instrument, _tile_id, datetime, *_version = granule_id.split(".")
    # Prefix is either HLS or HLS-VI
    _hls, *vi = prefix.split("-")
    date, _time = datetime.split("T")

    return (
        f"{instrument}{'_VI' if vi else ''}/data/{date}/{granule_id}/{granule_id}.json"
    )


def summarize_report(
    processed_report: Mapping[str, Mapping[Status, Sequence[str]]],
) -> Mapping[str, Mapping[Status, int]]:
    """Summarize granule counts per status and collection.

    Since it is possible for an input report to be very large (due to a large
    number of missing granules), we want to avoid logging such potentially large
    output.  Therefore, we want to summarize the results by providing granule
    counts rather than lists of granule IDs.

    We map an input like this:

    ```plain
    "<SHORT_NAME>___<VERSION>": {
        <Status.MISSING: 'missing'>: ["<GRANULE_ID_1>", ..., "<GRANULE_ID_N>"],
        ...
    },
    ```

    to a summary like this:

    ```plain
    "<SHORT_NAME>___<VERSION>": {
        <Status.MISSING: 'missing'>: 12345,
        ...
    },
    ```
    """
    return map_values(
        processed_report,
        lambda granule_ids_by_status: map_values(granule_ids_by_status, len),
    )


def map_values(mapping: Mapping[K, V], f: Callable[[V], W]) -> Mapping[K, W]:
    """Map the values of a mapping using a function.

    Parameters
    ----------
    mapping
        A mapping where values will be transformed.
    f
        Unary function to use for transforming a value from `mapping` to a new
        value.

    Returns
    -------
    Mapping[K, W]
        A new mapping with the same keys as `mapping`, but with values produced
        by passing values from `mapping` through the function `f`.
    """
    return {k: f(v) for k, v in mapping.items()}
