"""Append-only immutable experiment ledger (JSONL).

Every tournament run records one line. Lines are never modified, reordered,
or deleted: failed experiments stay in the ledger permanently -- that is the
point. Readers use read_all()/query(); writers use record(). Pure stdlib,
no I/O except the single append in record(), no network.
"""
from __future__ import annotations

import datetime as dt
import json
import os

LEDGER_VERSION = 1


def make_entry(family, version, metrics, status, feature_set_hash="",
               train_window=None, val_window=None, hyperparameters=None,
               commit_sha="", notes=""):
    """Constructor with sane defaults. metrics/status are the required core;
    everything else documents provenance for reproducibility."""
    return {
        "family": family,
        "version": version,
        "feature_set_hash": feature_set_hash,
        "train_window": train_window,
        "val_window": val_window,
        "hyperparameters": dict(hyperparameters or {}),
        "commit_sha": commit_sha,
        "metrics": dict(metrics or {}),
        "status": status,
        "notes": notes,
    }


def record(path, entry):
    """Append one entry as a single JSON line. Never touches existing lines.
    Returns the stored entry (input dict is not mutated)."""
    stored = dict(entry)
    stored["recorded_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    stored["ledger_version"] = LEDGER_VERSION
    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(stored, sort_keys=True) + "\n")
    return stored


def read_all(path):
    """Return every ledger entry in file order. Missing file -> []."""
    if not os.path.exists(path):
        return []
    out = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def query(path, family=None, status=None):
    """Filter entries by family and/or status. Both optional."""
    return [e for e in read_all(path)
            if (family is None or e.get("family") == family)
            and (status is None or e.get("status") == status)]
