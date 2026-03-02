import json
import time
from pathlib import Path
from typing import Any, Dict, Set


History = Dict[str, Any]


def load_history(path: str) -> History:
    # History file format:
    # {
    #   "updated_at": 1234567890,
    #   "sent_browse_ids": ["MPREb_...", ...]
    # }
    p = Path(path)
    if not p.exists():
        return {"updated_at": int(time.time()), "sent_browse_ids": []}

    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        return {"updated_at": int(time.time()), "sent_browse_ids": []}

    sent = data.get("sent_browse_ids", [])
    if not isinstance(sent, list):
        sent = []

    return {
        "updated_at": int(data.get("updated_at", int(time.time()))),
        "sent_browse_ids": sent,
    }


def save_history(path: str, history: History) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "updated_at": int(time.time()),
        "sent_browse_ids": list(history.get("sent_browse_ids", [])),
    }

    with p.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def sent_set(history: History) -> Set[str]:
    # Convert to set for fast membership checks
    sent_ids = history.get("sent_browse_ids", [])
    return {str(x) for x in sent_ids if x}


def mark_sent(history: History, browse_id: str) -> None:
    # Keep insertion order stable (list), but avoid duplicates
    browse_id = str(browse_id)
    current = history.get("sent_browse_ids", [])
    if browse_id not in current:
        current.append(browse_id)
    history["sent_browse_ids"] = current

def history_count(history: History) -> int:
    # Number of unique sent IDs stored in history.
    return len(sent_set(history))