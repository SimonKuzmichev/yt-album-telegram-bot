from __future__ import annotations

import json
import os
from typing import Any, Dict, Mapping

from cryptography.fernet import Fernet, InvalidToken


MASTER_KEY_ENV_VAR = "CREDENTIALS_MASTER_KEY"
ENVELOPE_PREFIX = "fernet:v1:"
REDACTED_VALUE = "[REDACTED]"
_SENSITIVE_FIELD_MARKERS = (
    "access",
    "authorization",
    "cookie",
    "credential",
    "password",
    "refresh",
    "secret",
    "token",
)


def _get_fernet() -> Fernet:
    raw_key = os.getenv(MASTER_KEY_ENV_VAR, "").strip()
    if not raw_key:
        raise RuntimeError(f"{MASTER_KEY_ENV_VAR} is not set")
    try:
        return Fernet(raw_key.encode("utf-8"))
    except Exception as exc:
        raise RuntimeError(f"{MASTER_KEY_ENV_VAR} is invalid") from exc


def encrypt_for_storage(payload: Any) -> str:
    serialized = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    encrypted = _get_fernet().encrypt(serialized.encode("utf-8")).decode("utf-8")
    return f"{ENVELOPE_PREFIX}{encrypted}"


def decrypt_for_runtime(payload: str) -> Any:
    if not payload.startswith(ENVELOPE_PREFIX):
        raise RuntimeError("Unsupported encrypted payload format")
    token = payload[len(ENVELOPE_PREFIX):].encode("utf-8")
    try:
        decrypted = _get_fernet().decrypt(token)
    except InvalidToken as exc:
        raise RuntimeError("Encrypted payload could not be decrypted") from exc
    return json.loads(decrypted.decode("utf-8"))


def redact_sensitive_mapping(payload: Mapping[str, Any]) -> Dict[str, Any]:
    redacted: Dict[str, Any] = {}
    for key, value in payload.items():
        normalized_key = key.strip().lower()
        if any(marker in normalized_key for marker in _SENSITIVE_FIELD_MARKERS):
            redacted[key] = REDACTED_VALUE
            continue
        if isinstance(value, Mapping):
            redacted[key] = redact_sensitive_mapping(value)
            continue
        redacted[key] = value
    return redacted
