import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from dotenv import load_dotenv

from src.credentials_encryption import redact_sensitive_mapping
from src.db import enqueue_job_once, upsert_user_provider_account_credentials


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
    parser.add_argument(
        "--enqueue-revalidate",
        action="store_true",
        help="Queue a revalidate_provider job after updating credentials",
    )
    parser.add_argument(
        "--enqueue-sync",
        action="store_true",
        help="Queue a sync_library job after updating credentials",
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

    queued_jobs = []
    now_utc = datetime.now(timezone.utc)
    if args.enqueue_revalidate:
        revalidate_key = f"revalidate:{row['id']}"
        queued_row = enqueue_job_once(
            idempotency_key=revalidate_key,
            idempotency_expires_at=now_utc + timedelta(minutes=15),
            job_id=uuid4(),
            user_id=int(row["user_id"]),
            job_type="revalidate_provider",
            run_at=now_utc,
            payload={
                "idempotency_key": revalidate_key,
                "user_provider_account_id": int(row["id"]),
                "provider": row["provider"],
            },
        )
        if queued_row is not None:
            queued_jobs.append("revalidate_provider")

    if args.enqueue_sync:
        sync_key = f"sync-now:{row['id']}"
        queued_row = enqueue_job_once(
            idempotency_key=sync_key,
            idempotency_expires_at=now_utc + timedelta(minutes=15),
            job_id=uuid4(),
            user_id=int(row["user_id"]),
            job_type="sync_library",
            run_at=now_utc,
            payload={
                "idempotency_key": sync_key,
                "user_provider_account_id": int(row["id"]),
                "provider": row["provider"],
            },
        )
        if queued_row is not None:
            queued_jobs.append("sync_library")

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
                "queued_jobs": queued_jobs,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
