"""Tests for ZeroQ envelope verify-and-repair engine."""
import json
import tempfile
from pathlib import Path
import pytest

from tools.verify_repair import (
    load_rubric,
    verify,
    repair,
    get_default_rubric_path,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
RUBRIC_PATH = REPO_ROOT / "rubric.json"


def test_rubric_schema_and_weights():
    rubric = load_rubric(RUBRIC_PATH)
    assert rubric.get("name") == "zeroq-envelope-rubric"
    assert rubric.get("passing_threshold") == 85
    checks = rubric.get("checks", [])
    assert len(checks) >= 5
    total_weight = sum(c["weight"] for c in checks)
    assert total_weight == 100


def test_verify_valid_envelope_scores_100():
    envelope = {
        "id": "msg_abc123",
        "topic": "telemetry.sensors.temp",
        "payload": {"reading_celsius": 24.5},
        "timestamp": 1732680000000,
        "ttl": 60000,
        "headers": {"content-type": "application/json"},
        "retryCount": 0,
    }
    res = verify(envelope, load_rubric(RUBRIC_PATH))
    assert res["passed"] is True
    assert res["score"] == 100
    assert len(res["violations"]) == 0


def test_mechanical_repair_recovers_failing_envelope(tmp_path):
    # Envelope with missing id, missing timestamp, string ttl, missing headers
    broken_envelope = {
        "topic": "events.audio.frame",
        "payload": {"chunk_id": 42},
        "ttl": "30000",
        "retryCount": -1,
    }

    ledger_file = tmp_path / "repair_ledger.jsonl"
    res = repair(broken_envelope, rubric_path=None, ledger_path=ledger_file, max_rounds=3)

    assert res["initial_score"] < 80
    assert res["passed"] is True
    assert res["final_score"] == 100
    assert res["envelope"]["id"].startswith("zeroq_")
    assert isinstance(res["envelope"]["timestamp"], int)
    assert res["envelope"]["ttl"] == 30000
    assert isinstance(res["envelope"]["headers"], dict)
    assert res["envelope"]["retryCount"] == 0

    # Verify ledger was written
    assert ledger_file.is_file()
    lines = [json.loads(line) for line in ledger_file.read_text().splitlines() if line.strip()]
    assert len(lines) >= 1
    assert lines[0]["status"] in ("repaired", "passed")


def test_non_mechanical_fault_reported_not_guessed(tmp_path):
    # Envelope with missing payload (non-mechanical defect)
    unfixable_envelope = {
        "id": "msg_001",
        "topic": "user.login",
        "payload": None,
        "timestamp": 1732680000000,
        "ttl": 60000,
        "headers": {},
        "retryCount": 0,
    }

    ledger_file = tmp_path / "ledger_unresolved.jsonl"
    res = repair(unfixable_envelope, rubric_path=None, ledger_path=ledger_file, max_rounds=3)

    assert res["passed"] is False
    assert res["envelope"]["payload"] is None  # Must NOT guess payload
    unresolved = [u["check_id"] for u in res["unresolved"]]
    assert "payload_integrity" in unresolved

    # Verify ledger logged unresolved status
    lines = [json.loads(line) for line in ledger_file.read_text().splitlines() if line.strip()]
    assert any(line.get("status") == "unresolved_non_mechanical" for line in lines)


def test_invalid_topic_naming_reported_as_non_mechanical(tmp_path):
    # Envelope with space/special chars in topic
    bad_topic_envelope = {
        "id": "msg_002",
        "topic": "illegal topic name with spaces!",
        "payload": {"data": "test"},
        "timestamp": 1732680000000,
        "ttl": 60000,
        "headers": {},
        "retryCount": 0,
    }

    res = repair(bad_topic_envelope, max_rounds=3)
    assert res["passed"] is False
    unresolved = [u["check_id"] for u in res["unresolved"]]
    assert "topic_format" in unresolved
