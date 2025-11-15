"""Utilities for encrypting sensitive data such as OAuth tokens."""
from __future__ import annotations

import os
from functools import lru_cache

from cryptography.fernet import Fernet, InvalidToken


class EncryptionError(RuntimeError):
    """Raised when encryption or decryption fails."""


@lru_cache(maxsize=1)
def _get_fernet() -> Fernet:
    key = os.getenv("ENCRYPTION_KEY")
    if not key:
        raise EncryptionError("ENCRYPTION_KEY is not configured.")
    if isinstance(key, str):
        key_bytes = key.encode()
    else:
        key_bytes = key
    try:
        return Fernet(key_bytes)
    except (ValueError, TypeError) as exc:
        raise EncryptionError("Invalid ENCRYPTION_KEY format.") from exc


def encrypt_text(value: str) -> str:
    if value is None:
        raise EncryptionError("Cannot encrypt empty value.")
    fernet = _get_fernet()
    return fernet.encrypt(value.encode()).decode()


def decrypt_text(value: str) -> str:
    if not value:
        raise EncryptionError("Cannot decrypt empty value.")
    fernet = _get_fernet()
    try:
        return fernet.decrypt(value.encode()).decode()
    except InvalidToken as exc:
        raise EncryptionError("Failed to decrypt value.") from exc
