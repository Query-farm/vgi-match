"""Pure-logic tests for the Splink-backed linkage core (no VGI, no subprocess).

These call ``vgi_match.linkage.resolve`` directly on the planted-duplicate
fixture and assert the ground-truth partition: known duplicates share a
cluster, distinct people don't. They also cover the input-validation errors.
"""

from __future__ import annotations

import pytest

from vgi_match import linkage

from .synthetic import COMPARISON_COLUMNS, EXPECTED_GROUPS, make_customers


def _clusters_by_row(result: dict[str, list]) -> dict[int, str]:
    """Map each row_id -> its assigned cluster_id string."""
    return dict(zip(result["row_id"], result["cluster_id"], strict=True))


def test_resolve_recovers_planted_groups() -> None:
    df = make_customers()
    result = linkage.resolve(df, columns=COMPARISON_COLUMNS)

    # Output preserves input order and every input column, plus the two new ones.
    assert result["row_id"] == df["row_id"].tolist()
    assert result["email"] == df["email"].tolist()
    assert "cluster_id" in result
    assert "match_probability" in result
    assert len(result["cluster_id"]) == len(df)

    by_row = _clusters_by_row(result)

    # Rows in the same ground-truth group share one cluster id...
    for group in EXPECTED_GROUPS:
        ids = {by_row[r] for r in group}
        assert len(ids) == 1, f"group {group} split across clusters {ids}"

    # ...and different groups get different cluster ids (no over-merging).
    group_cluster = [by_row[g[0]] for g in EXPECTED_GROUPS]
    assert len(set(group_cluster)) == len(EXPECTED_GROUPS), f"distinct entities collapsed: {group_cluster}"


def test_match_probability_is_valid() -> None:
    df = make_customers()
    result = linkage.resolve(df, columns=COMPARISON_COLUMNS)
    for p in result["match_probability"]:
        assert p is None or 0.0 <= float(p) <= 1.0


def test_empty_input_raises() -> None:
    df = make_customers().iloc[0:0]
    with pytest.raises(linkage.MatchError, match="non-empty"):
        linkage.resolve(df, columns=COMPARISON_COLUMNS)


def test_missing_column_raises() -> None:
    df = make_customers()
    with pytest.raises(linkage.MatchError, match="not found"):
        linkage.resolve(df, columns=["first_name", "nope"])


def test_no_columns_raises() -> None:
    df = make_customers()
    with pytest.raises(linkage.MatchError, match="no comparison columns"):
        linkage.resolve(df, columns=[])


def test_bad_threshold_raises() -> None:
    df = make_customers()
    with pytest.raises(linkage.MatchError, match=r"threshold must be"):
        linkage.resolve(df, columns=COMPARISON_COLUMNS, threshold=1.5)
