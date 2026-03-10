from __future__ import annotations


def is_auth_error(exc: Exception) -> bool:
    """
    Heuristic check for expired/invalid auth (browser headers/cookies).
    ytmusicapi may raise different exception types depending on version and
    underlying HTTP client, so we use a conservative string-based heuristic.
    """
    msg = str(exc).lower()

    # Common HTTP status codes / keywords
    auth_markers = [
        "401",
        "403",
        "unauthorized",
        "forbidden",
        "invalid or expired",
        "login",
        "sign in",
        "authentication",
        "auth",
    ]

    return any(m in msg for m in auth_markers)


def format_auth_help() -> str:
    """
    Human-readable instructions to recover from expired auth.
    Keep it short so it fits nicely in Telegram.
    """
    return (
        "⚠️ YouTube Music auth looks expired.\n\n"
        "Fix:\n"
        "1) Regenerate the user's YT Music credential blob outside the bot\n"
        "2) Store it with scripts/upsert_provider_credentials.py\n"
        "3) Queue /refresh for that user\n"
    )
