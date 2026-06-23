"""Deterministic customer fixture with KNOWN duplicates -- the validation backbone.

``make_customers`` returns a small relation of customer records seeded with
planted duplicate groups: variants of the same person (typos, nicknames) that a
good entity-resolution model must pull into one cluster, plus genuinely distinct
people that must stay apart. ``EXPECTED_GROUPS`` records the ground-truth
partition so tests can assert "these rows land in the same cluster, those don't"
without depending on Splink's opaque internal cluster ids.
"""

from __future__ import annotations

import pandas as pd

# Each tuple is one record: (first_name, last_name, email, city) plus a stable
# row_id we add below. Rows are grouped by the *true* entity via EXPECTED_GROUPS.
_RECORDS: list[tuple[str, str, str, str]] = [
    # Group A: "John Smith" -- a nickname/typo variant sharing the email.
    ("John", "Smith", "jsmith@example.com", "New York"),
    ("Jon", "Smith", "jsmith@example.com", "New York"),
    ("Johnny", "Smith", "jsmith@example.com", "New York"),
    # Group B: "Jane Doe" -- a maiden/typo variant sharing the email.
    ("Jane", "Doe", "jane.doe@example.com", "Los Angeles"),
    ("Janet", "Doe", "jane.doe@example.com", "Los Angeles"),
    # Group C: singleton -- nobody else like him.
    ("Robert", "Jones", "rjones@example.com", "San Francisco"),
    # Group D: singleton -- shares a surname with C but a different person.
    ("Alice", "Jones", "alice.j@example.com", "Seattle"),
]

# Ground-truth partition: lists of row_ids that are the SAME real entity.
EXPECTED_GROUPS: list[list[int]] = [
    [0, 1, 2],  # John / Jon / Johnny Smith
    [3, 4],  # Jane / Janet Doe
    [5],  # Robert Jones
    [6],  # Alice Jones
]

# Comparison columns the default model matches on for this fixture.
COMPARISON_COLUMNS: list[str] = ["first_name", "last_name", "email"]


def make_customers() -> pd.DataFrame:
    """Build the deterministic customer relation with a stable ``row_id`` column.

    Returns:
        A pandas DataFrame with columns ``row_id, first_name, last_name, email,
        city`` -- the input relation a caller would dedup.
    """
    rows = [
        {
            "row_id": i,
            "first_name": fn,
            "last_name": ln,
            "email": em,
            "city": city,
        }
        for i, (fn, ln, em, city) in enumerate(_RECORDS)
    ]
    return pd.DataFrame(rows)
