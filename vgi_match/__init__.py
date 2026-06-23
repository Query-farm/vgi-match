"""Probabilistic entity resolution / record linkage as a VGI worker for DuckDB/SQL.

The implementation is split so each concern stays focused:

- ``linkage``   -- pure record-linkage logic (Splink's Fellegi-Sunter model:
  blocking, comparisons, unsupervised EM training, predict, cluster) over
  ``pandas`` frames; no Arrow or VGI dependency, directly unit-testable.
- ``buffering`` -- the single-bucket Sink+Source plumbing the function uses
  (buffer all input batches, then run Splink once over the whole relation).
- ``tables``    -- the VGI ``TableBufferingFunction`` wrapper: relation in via
  ``(SELECT ...)`` (``Arg(0)``), linkage config as named args; output schema is
  the input schema plus an appended ``cluster_id`` + ``match_probability``.

``match_worker.py`` at the repo root assembles these into the ``match`` catalog
and runs the worker over stdio (or HTTP).
"""

from __future__ import annotations

__version__ = "0.1.0"
