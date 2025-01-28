import re
from typing import Literal, Mapping, Sequence


def decode_collection_id(collection_id: str) -> tuple[str, str]:
    """Decode a collection ID into a tuple of its name and version."""
    short_name, version = collection_id.split("___", 1)
    return short_name, version


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
    ]
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
                        file_info["granuleId"]
                        for file_info in collection_info["report"].values()
                    }
                )
            ),
        )
        for collection_report in report
        for collection_id, collection_info in collection_report.items()
    }


def notification_trigger_key(granule_id: str) -> str:
    """Returns the key for the S3 notification trigger object for a granule."""
    # Example: HLS.S30.T15XWH.2024237T194859.v2.0
    # 'HLS', 'S30', 'T15XWH', '2024237T194859', ['v2', '0']
    _hls, instrument, _tile_id, datetime, *_version = granule_id.split(".")
    date, _time = datetime.split("T")

    return f"{instrument}/data/{date}/{granule_id}/{granule_id}.json"
