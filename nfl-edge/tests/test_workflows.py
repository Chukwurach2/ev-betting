"""Workflow-behavior tests for NFL Edge GitHub Actions workflows.

These guard CI semantics that cannot be validated by unit tests of the
Python scripts themselves:

- The audit-dataset workflow must never mask a failing audit exit code.
  The audit step has to capture the audit script's exit status, still
  print the JSON/markdown report, and end with a hard failure (exit with
  the captured status). Artifacts must be uploaded unconditionally.
"""
import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"


def _load_workflow(name):
    path = WORKFLOWS / name
    assert path.exists(), f"workflow file missing: {path}"
    return yaml.safe_load(path.read_text())


def _audit_step(doc):
    steps = doc["jobs"]["audit"]["steps"]
    for step in steps:
        if step.get("name") == "Run dataset integrity audit":
            return step
    raise AssertionError("audit step not found")


def test_audit_workflow_does_not_mask_audit_exit_code():
    doc = _load_workflow("audit-dataset.yml")
    script = _audit_step(doc)["run"]
    # No swallow-the-status idioms.
    assert '|| echo "audit exited $?"' not in script
    assert not re.search(r"\|\|\s*echo\b", script), "exit code must not be swallowed"


def test_audit_workflow_captures_status_and_hard_fails():
    doc = _load_workflow("audit-dataset.yml")
    script = _audit_step(doc)["run"]
    # Exit status is captured immediately after the audit command.
    assert re.search(r"audit_dataset\.py[\s\S]*AUDIT_STATUS=\$?", script)
    assert "AUDIT_STATUS=$?" in script
    # Report is printed regardless of outcome.
    assert "/tmp/dataset_audit.json" in script
    # Final statement propagates the captured failure as a hard step failure.
    lines = [ln.strip() for ln in script.strip().splitlines() if ln.strip()]
    assert lines[-1] == 'exit "$AUDIT_STATUS"', (
        f"audit step must end with a hard failure on captured status, got: {lines[-1]!r}"
    )


def test_audit_workflow_uploads_artifacts_even_on_failure():
    doc = _load_workflow("audit-dataset.yml")
    steps = doc["jobs"]["audit"]["steps"]
    uploads = [
        s for s in steps
        if str(s.get("uses", "")).startswith("actions/upload-artifact")
    ]
    assert uploads, "audit workflow must upload audit artifacts"
    for step in uploads:
        assert step.get("if") == "always()", (
            "audit artifacts must be uploaded even when the audit fails"
        )
