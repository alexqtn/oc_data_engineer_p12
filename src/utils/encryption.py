# ============================================================
# encryption.py — pgcrypto helpers for PII encryption/decryption
# PostgreSQL handles the crypto, Python builds the SQL expressions
# ============================================================

from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text
import os


ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")

ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")

if not ENCRYPTION_KEY:
    raise ValueError(
        "Missing ENCRYPTION_KEY. "
        "Ensure ENCRYPTION_KEY is set in your .env file."
    )

# Single source of truth: all PII columns that require encryption
ENCRYPTED_COLUMNS = [
    "rh_last_name",
    "rh_first_name",
    "rh_birth_date",
    "rh_gross_salary",
    "rh_street_number",
    "rh_street_name",
    "rh_postal_code",
    "rh_city",
]


def encrypt_value(value: str) -> text:
    """
    Returns a SQL expression that encrypts a value via pgcrypto.
    Used in INSERT/UPDATE queries.

    Usage:
        session.execute(text(
            "INSERT INTO employees (rh_last_name) VALUES (pgp_sym_encrypt(:val, :key))"
        ), {"val": "Colin", "key": ENCRYPTION_KEY})
    """
    return text("pgp_sym_encrypt(:val, :key)").bindparams(val=str(value), key=ENCRYPTION_KEY)


def _decrypt_value(column_name: str) -> str:
    """Internal helper — builds decrypt SQL for one column."""
    return f"pgp_sym_decrypt({column_name}, '{ENCRYPTION_KEY}') AS {column_name}"


def build_decrypt_select(columns: list[str] = None) -> str:
    """
    Builds a SELECT fragment that decrypts specified columns.
    Defaults to all encrypted columns if none specified.

    Usage:
        build_decrypt_select(["rh_last_name"])  → one column
        build_decrypt_select()                  → all 8 columns
    """
    if columns is None:
        columns = ENCRYPTED_COLUMNS
    return ", ".join(_decrypt_value(col) for col in columns)