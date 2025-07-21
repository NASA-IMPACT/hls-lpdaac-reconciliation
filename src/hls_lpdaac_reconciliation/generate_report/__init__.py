# pyright: reportTypedDictNotRequiredAccess=false
from __future__ import annotations

import csv
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path

TYPE_CHECKING = False

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_athena.type_defs import RowTypeDef

REPORT_FIELD_NAMES: Sequence[str] = (
    "short_name",
    "version",
    "filename",
    "size",
    "last_modified",
    "checksum",
)


def row_values(row: RowTypeDef) -> Sequence[str]:
    """Extract varchar values from a row.

    Examples
    --------
    >>> row = {"Data": [{"VarCharValue": "val00"}, {"VarCharValue": "val01"}]}
    >>> row_values(row)
    ('val00', 'val01')
    """
    return tuple(datum["VarCharValue"] for datum in row["Data"])


def as_records(rows: Iterable[RowTypeDef]) -> Iterator[dict[str, str]]:
    """Convert "raw" rows into "records" (dicts).

    Assumes the values in the first row are the column names, and uses these
    column names as the keys of each "record" produced by the returned iterator.
    The column names are assumed to be in the same order as the corresponding
    values in each row, such that the nth name is associated with the nth value
    of each row.

    Examples
    --------
    >>> rows = [{"Data": [{"VarCharValue": "col0"}, {"VarCharValue": "col1"}]},
    ...         {"Data": [{"VarCharValue": "val00"}, {"VarCharValue": "val01"}]},
    ...         {"Data": [{"VarCharValue": "val10"}, {"VarCharValue": "val11"}]}]
    >>> list(as_records(rows))
    [{'col0': 'val00', 'col1': 'val01'},
     {'col0': 'val10', 'col1': 'val11'}]
    """

    irows = iter(rows)
    header = row_values(next(irows))

    return map(lambda row: dict(zip(header, row_values(row))), irows)


def to_csv(path: Path, records: Iterable[dict[str, str]]) -> int:
    """Write query result records to a CSV file.

    Returns
    -------
    int
        number of rows written to disk
    """

    from functools import reduce
    from itertools import batched

    def reducer(nrows: int, batch: Sequence[dict[str, str]]) -> int:
        writer.writerows(batch)
        return nrows + len(batch)

    # Use newline="", per guidance at https://docs.python.org/3/library/csv.html#id4
    with path.open("w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=REPORT_FIELD_NAMES)
        nrows = reduce(reducer, batched(records, 10_000), 0)

    return nrows
