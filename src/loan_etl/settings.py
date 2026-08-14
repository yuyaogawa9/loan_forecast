"""Typed configuration loaded from secret.env.

Fails loudly at construction rather than letting a missing key surface later as
an opaque HTTP 400 from FRED or a write into an unintended directory.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
SECRET_ENV = REPO_ROOT / "secret.env"
SCHEMA_DIR = REPO_ROOT / "config" / "schemas"


class ConfigError(RuntimeError):
    """Raised when configuration is missing or unusable."""


@dataclass(frozen=True)
class Settings:
    data_root: Path
    fred_api_key: str
    schema_version: int
    schema_dir: Path = SCHEMA_DIR

    # --- Derived data-lake locations ---------------------------------------
    @property
    def raw_freddie(self) -> Path:
        return self.data_root / "raw" / "freddie"

    @property
    def raw_fred(self) -> Path:
        return self.data_root / "raw" / "fred"

    @property
    def bronze(self) -> Path:
        return self.data_root / "bronze"

    @property
    def curated(self) -> Path:
        return self.data_root / "curated"

    @property
    def quarantine(self) -> Path:
        return self.data_root / "quarantine"

    @property
    def manifests(self) -> Path:
        return self.data_root / "_manifests"

    def require_fred_key(self) -> str:
        """Call this at the point of use so ETL stages that need no network
        access still run without a key configured."""
        if not self.fred_api_key:
            raise ConfigError(
                "FRED_API_KEY is empty in secret.env. Get a free key at "
                "https://fred.stlouisfed.org/docs/api/api_key.html"
            )
        return self.fred_api_key

    def ensure_dirs(self) -> None:
        for p in (
            self.raw_freddie,
            self.raw_fred,
            self.bronze,
            self.curated,
            self.quarantine,
            self.manifests,
        ):
            p.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    if SECRET_ENV.exists():
        load_dotenv(SECRET_ENV)
    else:
        # Fall back to the ambient environment so CI / one-off runs work, but
        # say so -- a silently missing secret.env is a confusing failure.
        print(f"[settings] {SECRET_ENV} not found; using ambient environment only.")

    raw_root = os.environ.get("DATA_ROOT", "").strip()
    if not raw_root:
        raise ConfigError(
            f"DATA_ROOT is not set. Copy .env.example to {SECRET_ENV.name} and set it "
            "to an absolute path OUTSIDE the repo."
        )

    data_root = Path(raw_root).expanduser().resolve()
    if data_root == REPO_ROOT or REPO_ROOT in data_root.parents:
        raise ConfigError(
            f"DATA_ROOT ({data_root}) is inside the repo ({REPO_ROOT}). "
            "Keep the data lake outside the working tree."
        )

    try:
        schema_version = int(os.environ.get("FREDDIE_SCHEMA_VERSION", "47"))
    except ValueError as exc:
        raise ConfigError("FREDDIE_SCHEMA_VERSION must be an integer") from exc

    return Settings(
        data_root=data_root,
        fred_api_key=os.environ.get("FRED_API_KEY", "").strip(),
        schema_version=schema_version,
    )