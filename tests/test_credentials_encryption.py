import os
import unittest
from unittest.mock import patch

from cryptography.fernet import Fernet

from src.credentials_encryption import (
    MASTER_KEY_ENV_VAR,
    decrypt_for_runtime,
    encrypt_for_storage,
    redact_sensitive_mapping,
)


class EncryptForStorageTests(unittest.TestCase):
    def test_round_trips_json_payload(self) -> None:
        key = Fernet.generate_key().decode("utf-8")
        payload = {
            "access_token": "secret-access-token",
            "refresh_token": "secret-refresh-token",
            "nested": {"cookie": "value", "display_name": "friend"},
        }

        with patch.dict(os.environ, {MASTER_KEY_ENV_VAR: key}, clear=False):
            encrypted = encrypt_for_storage(payload)
            decrypted = decrypt_for_runtime(encrypted)

        self.assertTrue(encrypted.startswith("fernet:v1:"))
        self.assertEqual(decrypted, payload)

    def test_raises_when_master_key_is_missing(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError):
                encrypt_for_storage({"token": "abc"})

    def test_raises_when_envelope_prefix_is_unknown(self) -> None:
        key = Fernet.generate_key().decode("utf-8")
        with patch.dict(os.environ, {MASTER_KEY_ENV_VAR: key}, clear=False):
            with self.assertRaises(RuntimeError):
                decrypt_for_runtime("plain-text")


class RedactSensitiveMappingTests(unittest.TestCase):
    def test_redacts_common_secret_fields(self) -> None:
        payload = {
            "access_token": "secret",
            "refresh_token": "secret-2",
            "cookie_blob": "cookie",
            "display_name": "friend",
            "nested": {
                "authorization_header": "Bearer token",
                "country": "US",
            },
        }

        redacted = redact_sensitive_mapping(payload)

        self.assertEqual(redacted["access_token"], "[REDACTED]")
        self.assertEqual(redacted["refresh_token"], "[REDACTED]")
        self.assertEqual(redacted["cookie_blob"], "[REDACTED]")
        self.assertEqual(redacted["display_name"], "friend")
        self.assertEqual(redacted["nested"]["authorization_header"], "[REDACTED]")
        self.assertEqual(redacted["nested"]["country"], "US")


if __name__ == "__main__":
    unittest.main()
