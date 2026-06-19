"""Small fail-open client shared by production applications."""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

DEFAULT_URL = os.getenv("EVIDENCE_QUALITY_URL", "http://127.0.0.1:8791").rstrip("/")
DEFAULT_TIMEOUT = float(os.getenv("EVIDENCE_QUALITY_TIMEOUT_SECONDS", "1.5"))


def _post(path: str, payload: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:
    request = urllib.request.Request(
        f"{DEFAULT_URL}{path}",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout or DEFAULT_TIMEOUT) as response:
            result = json.loads(response.read().decode("utf-8"))
    except (OSError, ValueError, urllib.error.URLError):
        return {}
    return result if isinstance(result, dict) else {}


def rerank(
    use_case: str,
    query: str,
    candidates: list[dict[str, Any]],
    limit: int | None = None,
    retrieval_intent: dict[str, Any] | None = None,
):
    if not candidates:
        return []
    payload = {"use_case": use_case, "query": query, "candidates": candidates}
    if retrieval_intent:
        payload["retrieval_intent"] = retrieval_intent
    result = _post("/rank", payload)
    ranked = result.get("ranked") if isinstance(result, dict) else None
    if not isinstance(ranked, list):
        return candidates[:limit] if limit else candidates
    by_id = {str(item.get("id")): item for item in candidates if isinstance(item, dict)}
    ordered = [by_id.get(str(item.get("id"))) for item in ranked if isinstance(item, dict)]
    ordered = [item for item in ordered if item is not None]
    return ordered[:limit] if limit else ordered


def quality_check(text: str, protected_values: list[str] | None = None) -> dict[str, Any]:
    return _post("/quality-check", {"text": str(text or ""), "protected_values": list(protected_values or [])})
