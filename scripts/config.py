"""
scripts/config.py

Centralized configuration, environment variable parsing, and static mappings
for the Congress ingestion pipeline.
"""

from pathlib import Path
import os

# Project and Database Paths
ROOT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = ROOT_DIR / "dev.duckdb"

ENV = "STAGE"

# Automatically load .env if present in root
_env_file = ROOT_DIR / ".env"
if _env_file.exists():
    with open(_env_file, encoding="utf-8") as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                _k = _k.strip()
                _v = _v.strip().strip("\"'")
                if _k and _v:
                    os.environ.setdefault(_k, _v)

# Congress API Settings
API_KEY = os.environ.get("CONGRESS_API_KEY", "").strip()
if not API_KEY or API_KEY == "your_api_key_here":
    raise ValueError(
        "CONGRESS_API_KEY environment variable is not set. Please set it or add it to .env"
    )

BASE_URL = "https://api.congress.gov/v3"
CONGRESS = 119

# Default chamber configuration: 'house', 'senate', or 'both'
DEFAULT_CHAMBER = os.environ.get("CHAMBER", "house").lower()

# Execution scope limits (can be overridden via environment variables or CLI arguments)
# None = full ingestion without caps
MEMBER_LIMIT = int(os.environ["MEMBER_LIMIT"]) if os.environ.get("MEMBER_LIMIT") else 5

# Maps 2-digit state FIPS codes to abbreviations
STATE_FIPS_TO_ABBR = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "12": "FL", "13": "GA",
    "15": "HI", "16": "ID", "17": "IL", "18": "IN", "19": "IA",
    "20": "KS", "21": "KY", "22": "LA", "23": "ME", "24": "MD",
    "25": "MA", "26": "MI", "27": "MN", "28": "MS", "29": "MO",
    "30": "MT", "31": "NE", "32": "NV", "33": "NH", "34": "NJ",
    "35": "NM", "36": "NY", "37": "NC", "38": "ND", "39": "OH",
    "40": "OK", "41": "OR", "42": "PA", "44": "RI", "45": "SC",
    "46": "SD", "47": "TN", "48": "TX", "49": "UT", "50": "VT",
    "51": "VA", "53": "WA", "54": "WV", "55": "WI", "56": "WY",
}

# Legislation bill and amendment types by chamber
HOUSE_BILL_TYPES = {"HR", "HRES", "HJRES", "HCONRES"}
SENATE_BILL_TYPES = {"S", "SRES", "SJRES", "SCONRES"}
SENATE_AMENDMENTS = "SAMDT"
HOUSE_AMENDMENTS = "HAMDT"
