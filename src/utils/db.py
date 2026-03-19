# ============================================================
# db.py — Centralized database connection for Sport Data Solution
# Entry points: get_engine() for reads, get_session() for writes
# ============================================================

from pathlib import Path
from contextlib import contextmanager

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os


# Load .env from project root (2 levels up from src/utils/)
ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")


# Priority: EXTERNAL (local Mac) → INTERNAL (Docker container)
DB_HOST = os.getenv("POSTGRES_EXTERNAL_HOST", os.getenv("POSTGRES_HOST", "localhost"))
DB_PORT = os.getenv("POSTGRES_EXTERNAL_PORT", os.getenv("POSTGRES_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB", "sportdb")
DB_USER = os.getenv("POSTGRES_WRITER_USER")
DB_PASSWORD = os.getenv("POSTGRES_WRITER_PASSWORD")


# Fail early with clear message instead of cryptic SQLAlchemy error
if not DB_USER or not DB_PASSWORD:
    raise ValueError(
        "Missing database credentials. "
        "Ensure POSTGRES_WRITER_USER and POSTGRES_WRITER_PASSWORD "
        "are set in your .env file."
    )


DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


# Singleton engine — created once at import, reused everywhere
# pool_pre_ping=True catches stale connections after PostgreSQL restart
_engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    pool_pre_ping=True,
    echo=False,
)


# Session factory — each call creates a new session
# autocommit/autoflush=False: we control when writes happen
_SessionFactory = sessionmaker(
    bind=_engine,
    autocommit=False,
    autoflush=False,
)


def get_engine():
    """Returns the Engine. Use for Pandas: pd.read_sql(query, get_engine())"""
    return _engine


@contextmanager
def get_session():
    """
    Context manager for write operations.
    Auto-rollback on error, auto-close on exit.

    Usage:
        with get_session() as session:
            session.execute(text("INSERT INTO ..."))
            session.commit()
    """
    session = _SessionFactory()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
