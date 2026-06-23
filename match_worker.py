# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "vgi-python",
#     "splink>=4",
#     "duckdb",
#     "pandas",
#     "pyarrow",
# ]
#
# [tool.uv.sources]
# vgi-python = { path = "../vgi-python" }
# ///
"""Stdio entry shim for the entity-resolution (match) VGI worker.

Lets the worker run straight from a source checkout (``uv run match_worker.py``)
and keeps ``import match_worker`` working for tests. The implementation lives in
``vgi_match.worker``; installed users invoke the ``vgi-match`` console script
(which points at ``vgi_match.worker:main``).

    ATTACH 'match' (TYPE vgi, LOCATION 'uv run match_worker.py');
    SELECT * FROM match.match_resolve((SELECT * FROM customers),
                                      columns := 'first_name,last_name,email');
"""

from vgi_match.worker import MatchWorker, main

__all__ = ["MatchWorker", "main"]

if __name__ == "__main__":
    main()
