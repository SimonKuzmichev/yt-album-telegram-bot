import argparse
import json
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from src.credentials_encryption import redact_sensitive_mapping
from src.db import upsert_user_provider_account_credentials


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Insert or update encrypted provider credentials for a user.")
    parser.add_argument("--user-id", type=int, required=True, help="Internal app.users.id value")
    parser.add_argument("--provider", required=True, help="Provider name, for example ytmusic or spotify")
    parser.add_argument("--credentials-file", required=True, help="Path to a JSON file with provider credentials")
    parser.add_argument("--status", default="active", help="Provider account status to store")
    parser.add_argument(
        "--inactive",
        action="store_true",
        help="Store this account as inactive instead of making it the active provider for the user",
    )
    parser.add_argument(
        "--token-expires-at",
        default="",
        help="Optional ISO-8601 timestamp for token expiry, for example 2026-03-10T12:00:00+00:00",
    )
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = _parse_args()

    credentials_path = Path(args.credentials_file)
    credentials = json.loads(credentials_path.read_text(encoding="utf-8"))
    if not isinstance(credentials, dict):
        raise RuntimeError("Credential file must contain a JSON object")

    token_expires_at = None
    if args.token_expires_at.strip():
        token_expires_at = datetime.fromisoformat(args.token_expires_at.strip())

    row = upsert_user_provider_account_credentials(
        user_id=args.user_id,
        provider=args.provider,
        credentials=credentials,
        status=args.status,
        is_active=not args.inactive,
        token_expires_at=token_expires_at,
    )

    print(
        json.dumps(
            {
                "account_id": row["id"],
                "user_id": row["user_id"],
                "provider": row["provider"],
                "status": row["status"],
                "is_active": row["is_active"],
                "token_expires_at": row["token_expires_at"].isoformat() if row.get("token_expires_at") else None,
                "credential_fields": sorted(redact_sensitive_mapping(credentials).keys()),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
