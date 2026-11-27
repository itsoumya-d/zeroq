"""Verify-and-Repair Engine for ZeroQ Message Envelopes.

Scores ZeroQ event envelopes from 0 to 100 according to a declarative rubric,
mechanically repairs format, TTL, timestamp, and header omissions across up to
three iterations, logs every action to an append-only JSONL ledger, and flags
non-mechanical defects for human review without guessing.
"""
from __future__ import annotations

import copy
import json
import re
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

TOPIC_REGEX = re.compile(r"^[a-zA-Z0-9_\-\.]+$")


def get_default_rubric_path() -> Path:
    """Find rubric.json in project root."""
    root = Path(__file__).resolve().parent.parent
    return root / "rubric.json"


def load_rubric(rubric_path: Optional[Path] = None) -> Dict[str, Any]:
    path = rubric_path or get_default_rubric_path()
    if not path.is_file():
        raise FileNotFoundError(f"Rubric file not found at {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def verify(
    envelope: Dict[str, Any],
    rubric: Optional[Dict[str, Any]] = None,
    rubric_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Score a ZeroQ message envelope against the weighted rubric."""
    r = rubric or load_rubric(rubric_path)
    checks = r.get("checks", [])
    passing_threshold = r.get("passing_threshold", 80)

    score = 0
    violations: List[Dict[str, Any]] = []

    for check in checks:
        cid = check["id"]
        weight = check["weight"]
        is_mechanical = check.get("mechanical", False)
        passed = False
        reason = ""

        if cid == "envelope_structure":
            has_id = bool(envelope.get("id"))
            has_topic = bool(envelope.get("topic"))
            ts = envelope.get("timestamp")
            has_ts = isinstance(ts, (int, float)) and ts > 0
            if has_id and has_topic and has_ts:
                passed = True
            else:
                reason = "Missing or invalid id, topic, or numeric timestamp."

        elif cid == "topic_format":
            topic = str(envelope.get("topic") or "").strip()
            if topic and TOPIC_REGEX.match(topic):
                passed = True
            else:
                reason = f"Topic '{topic}' does not conform to valid namespace characters."

        elif cid == "payload_integrity":
            payload = envelope.get("payload")
            if payload is not None:
                passed = True
            else:
                reason = "Payload is null or undefined."

        elif cid == "ttl_bounds":
            ttl = envelope.get("ttl")
            if isinstance(ttl, (int, float)) and ttl >= 0:
                passed = True
            else:
                reason = f"TTL '{ttl}' must be a non-negative integer."

        elif cid == "header_types":
            headers = envelope.get("headers")
            retry = envelope.get("retryCount")
            if isinstance(headers, dict) and isinstance(retry, int) and retry >= 0:
                passed = True
            else:
                reason = "Headers must be an object and retryCount must be non-negative integer."

        if passed:
            score += weight
        else:
            violations.append({
                "check_id": cid,
                "name": check.get("name", cid),
                "weight": weight,
                "mechanical": is_mechanical,
                "reason": reason,
            })

    return {
        "score": score,
        "passing_threshold": passing_threshold,
        "passed": score >= passing_threshold,
        "violations": violations,
    }


def repair(
    envelope: Dict[str, Any],
    rubric: Optional[Dict[str, Any]] = None,
    rubric_path: Optional[Path] = None,
    ledger_path: Optional[Path] = None,
    max_rounds: int = 3,
) -> Dict[str, Any]:
    """Iteratively repair mechanical envelope defects, logging rounds to JSONL ledger."""
    r = rubric or load_rubric(rubric_path)
    cur = copy.deepcopy(envelope)
    rounds_log: List[Dict[str, Any]] = []

    for round_num in range(1, max_rounds + 1):
        v = verify(cur, r)
        repairs_applied: List[str] = []

        if v["passed"]:
            record = {
                "round": round_num,
                "timestamp": int(time.time() * 1000),
                "score": v["score"],
                "status": "passed",
                "repairs": repairs_applied,
            }
            rounds_log.append(record)
            if ledger_path:
                _append_ledger(ledger_path, record)
            break

        # Check for non-mechanical violations that cannot be fixed automatically
        non_mechanical = [vio for vio in v["violations"] if not vio["mechanical"]]
        mechanical = [vio for vio in v["violations"] if vio["mechanical"]]

        if not mechanical and non_mechanical:
            record = {
                "round": round_num,
                "timestamp": int(time.time() * 1000),
                "score": v["score"],
                "status": "unresolved_non_mechanical",
                "non_mechanical_violations": non_mechanical,
            }
            rounds_log.append(record)
            if ledger_path:
                _append_ledger(ledger_path, record)
            break

        # Apply mechanical fixes
        for vio in mechanical:
            cid = vio["check_id"]
            if cid == "envelope_structure":
                if not cur.get("id"):
                    cur["id"] = f"zeroq_{uuid.uuid4().hex[:12]}"
                    repairs_applied.append("generated_missing_id")
                ts = cur.get("timestamp")
                if not isinstance(ts, (int, float)) or ts <= 0:
                    cur["timestamp"] = int(time.time() * 1000)
                    repairs_applied.append("set_current_timestamp")

            elif cid == "ttl_bounds":
                ttl = cur.get("ttl")
                if isinstance(ttl, str) and ttl.isdigit():
                    cur["ttl"] = int(ttl)
                    repairs_applied.append("coerced_ttl_string_to_int")
                else:
                    cur["ttl"] = 60000
                    repairs_applied.append("defaulted_missing_ttl")

            elif cid == "header_types":
                if not isinstance(cur.get("headers"), dict):
                    cur["headers"] = {}
                    repairs_applied.append("initialized_empty_headers")
                retry = cur.get("retryCount")
                if not isinstance(retry, int) or retry < 0:
                    cur["retryCount"] = 0
                    repairs_applied.append("reset_negative_or_missing_retry_count")

        record = {
            "round": round_num,
            "timestamp": int(time.time() * 1000),
            "score": v["score"],
            "status": "repaired",
            "repairs": repairs_applied,
        }
        rounds_log.append(record)
        if ledger_path:
            _append_ledger(ledger_path, record)

    final_v = verify(cur, r)
    return {
        "rounds": len(rounds_log),
        "initial_score": rounds_log[0]["score"] if rounds_log else final_v["score"],
        "final_score": final_v["score"],
        "passed": final_v["passed"],
        "envelope": cur,
        "rounds_log": rounds_log,
        "unresolved": final_v["violations"],
    }


def _append_ledger(ledger_path: Path, record: Dict[str, Any]) -> None:
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    with open(ledger_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")
