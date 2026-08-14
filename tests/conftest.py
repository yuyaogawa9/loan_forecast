from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from loan_etl.settings import SCHEMA_DIR, Settings  # noqa: E402

from .fixtures import write_vintage  # noqa: E402

VINTAGE = 2007


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """A Settings pointing at an isolated temp data lake.

    Constructed directly rather than through get_settings(), which is lru_cached
    and reads the real secret.env.
    """
    s = Settings(
        data_root=tmp_path / "lake",
        fred_api_key="test-key",
        schema_version=47,
        schema_dir=SCHEMA_DIR,
    )
    s.ensure_dirs()
    return s


@pytest.fixture
def raw_vintage(settings: Settings) -> Settings:
    write_vintage(settings.raw_freddie, VINTAGE)
    return settings


@pytest.fixture
def bronze(raw_vintage: Settings) -> Settings:
    from loan_etl.clean.ingest import ingest_vintage

    for dataset in ("origination", "performance"):
        ingest_vintage(raw_vintage, dataset, VINTAGE, keep_extracted=True)
    return raw_vintage


@pytest.fixture
def curated(bronze: Settings) -> Settings:
    from loan_etl.derive.panel import build_loan_month, build_outcomes

    build_loan_month(bronze, VINTAGE, with_macro=False)
    build_outcomes(bronze, VINTAGE)
    return bronze