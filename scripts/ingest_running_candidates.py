import os
import logging
import requests
import duckdb
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from ingestion import load_candidates

# Set up paths
BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "dev.duckdb"
LOG_PATH = BASE_DIR / "logs" / "openfec_ingestion.log"

LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)
ELECTION_YEAR = 2026

# -------------------------------------------------------------------
# Helper Utilities
# -------------------------------------------------------------------

def clean_district(district_val: Any) -> str:
    """Standardizes district into a 2-character string ('01', '02', '00')."""
    if district_val is None or str(district_val).strip() in ("", "0", "00", "None"):
        return "00"
    return str(district_val).strip().zfill(2)


def map_fec_office_to_chamber(office_code: str) -> str:
    """Maps OpenFEC office code ('H', 'S', 'P') to chamber name."""
    mapping = {"H": "House", "S": "Senate", "P": "President"}
    return mapping.get(str(office_code).upper(), "Unknown")


def get_fec_api_key() -> str:
    """Get the OpenFEC API key from environment or project .env file."""
    api_key = os.getenv("FEC_API_KEY", "").strip()
    if not api_key:
        env_file = BASE_DIR / ".env"
        if env_file.exists():
            for line in env_file.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    if key.strip() == "FEC_API_KEY":
                        api_key = value.strip().strip("\"'")
                        break

    if not api_key:
        raise RuntimeError("FEC_API_KEY is not set in environment or project .env file.")
    return api_key

def map_fec_party(party_raw: str | None) -> str:
    """Normalizes OpenFEC party names/codes to standard single-word party names

    matching raw_members conventions.
    """
    if not party_raw:
        return "Unknown"

    cleaned = party_raw.strip().upper()

    # Exact code & string mappings
    party_map = {
        # Standard OpenFEC Codes & Full Names
        "DEM": "Democratic",
        "DEMOCRATIC PARTY": "Democratic",
        "REP": "Republican",
        "REPUBLICAN PARTY": "Republican",
        "LIB": "Libertarian",
        "LIBERTARIAN PARTY": "Libertarian",
        "GRE": "Green",
        "GREEN PARTY": "Green",
        "IND": "Independent",
        "INDEPENDENT": "Independent",
        "CON": "Constitution",
        "CONSTITUTION PARTY": "Constitution",
        "NOP": "Independent",
        "NO PARTY AFFILIATION": "Independent",
    }

    if cleaned in party_map:
        return party_map[cleaned]

    # Partial string matching fallback for edge-case minor parties
    if "DEMOCRAT" in cleaned:
        return "Democratic"
    if "REPUBLICAN" in cleaned:
        return "Republican"
    if "LIBERTARIAN" in cleaned:
        return "Libertarian"
    if "GREEN" in cleaned:
        return "Green"
    if "INDEPENDENT" in cleaned:
        return "Independent"

    # Return title-cased raw string if no rule matches
    return party_raw.strip().title()

# -------------------------------------------------------------------
# Database Schema & Load Functions
# -------------------------------------------------------------------

def init_fec_db(db):
    """Ensures raw_fec_candidates table exists."""
    db.execute("""
               CREATE TABLE IF NOT EXISTS main.raw_fec_candidates (
                                                                      candidate_id        VARCHAR NOT NULL,
                                                                      name                VARCHAR,
                                                                      state               VARCHAR,
                                                                      chamber             VARCHAR,
                                                                      district            VARCHAR,
                                                                      party               VARCHAR,
                                                                      geoid_cd            VARCHAR,
                                                                      campaign_url        VARCHAR,
                                                                      committee_id        VARCHAR,
                                                                      incumbent_challenge VARCHAR,
                                                                      election_year       INTEGER,
                                                                      fec_update_date     TIMESTAMP,
                                                                      row_hash            VARCHAR,
                                                                      ingested_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                                                      PRIMARY KEY (candidate_id)
                   );
               """)



# -------------------------------------------------------------------
# Campaign URL Resolution & Ingestion Pipeline
# -------------------------------------------------------------------

def get_candidate_campaign_url(api_key: str, candidate_id: str, committee_id: Optional[str]) -> Optional[str]:
    """Tries multiple FEC endpoints to extract a valid candidate campaign website URL."""
    if not committee_id:
        return None

    # Step 1: Query Form 1 (Statement of Organization) filings
    filings_url = f"https://api.open.fec.gov/v1/committee/{committee_id}/filings/"
    params = {"api_key": api_key, "form_type": "F1", "sort": "-receipt_date", "per_page": 3}
    try:
        res = requests.get(filings_url, params=params, timeout=10)
        if res.status_code == 200:
            for filing in res.json().get("results", []):
                # Check web address fields in raw filing payload
                url = filing.get("website_url") or filing.get("url")
                if url and url.strip():
                    clean_url = url.strip()
                    return clean_url if clean_url.startswith(("http://", "https://")) else f"https://{clean_url}"
    except Exception as e:
        logger.warning(f"Error fetching Form 1 filings for committee {committee_id}: {e}")

    # Step 2: Check electronic filings (e-file Form 1s)
    efile_url = "https://api.open.fec.gov/v1/efile/reports/house-senate/"
    params = {"api_key": api_key, "committee_id": committee_id, "form_type": "F1", "per_page": 1}
    try:
        res = requests.get(efile_url, params=params, timeout=10)
        if res.status_code == 200:
            for report in res.json().get("results", []):
                url = report.get("website_url")
                if url and url.strip():
                    clean_url = url.strip()
                    return clean_url if clean_url.startswith(("http://", "https://")) else f"https://{clean_url}"
    except Exception as e:
        logger.warning(f"Error checking e-filings for committee {committee_id}: {e}")

    return None


def fetch_committee_details_batch(api_key: str, candidate_ids: List[str]) -> Dict[str, Dict[str, Optional[str]]]:
    """Fetches principal campaign committees for a batch of candidate IDs (up to 100)."""
    if not candidate_ids:
        return {}

    url = "https://api.open.fec.gov/v1/committees/"
    params = {
        "api_key": api_key,
        "candidate_id": candidate_ids,  # Pass list of candidate IDs
        "designation": "P",            # Principal Campaign Committee
        "cycle": 2026,
        "per_page": 100
    }

    committee_map = {}
    try:
        res = requests.get(url, params=params, timeout=15)
        if res.status_code == 200:
            for cmte in res.json().get("results", []):
                cmte_id = cmte.get("committee_id")
                web_url = cmte.get("website_url")

                # Associated candidate IDs
                c_ids = cmte.get("candidate_ids") or []
                for cid in c_ids:
                    committee_map[cid] = {
                        "committee_id": cmte_id,
                        "website_url": web_url
                    }
    except Exception as e:
        logger.error(f"Failed to fetch batch committee details: {e}")

    return committee_map


def ingest_candidates():
    """Main ingestion orchestrator: fetches all 2026 candidates and updates DuckDB."""
    api_key = get_fec_api_key()
    url = "https://api.open.fec.gov/v1/candidates/"
    page = 1
    total_candidates = []

    logger.info(f"Starting OpenFEC candidate ingestion for election_year={ELECTION_YEAR}...")

    # Step 1: Paginate through all non-incumbent 2026 candidates
    while True:
        params = {
            "api_key": api_key,
            "election_year": ELECTION_YEAR,
            "candidate_status": "C",
            "incumbent_challenge": ["C", "O"],  # Challengers and Open seats only
            "office": ["H", "S"],
            "per_page": 100,
            "page": page,
        }

        response = requests.get(url, params=params, timeout=15)
        if response.status_code != 200:
            logger.error(f"FEC API request failed on page {page}: {response.status_code} {response.text}")
            break

        data = response.json()
        results = data.get("results", [])
        if not results:
            break

        total_candidates.extend(results)
        pagination = data.get("pagination", {})
        total_pages = pagination.get("pages", 1)

        logger.info(f"Fetched page {page} of {total_pages} ({len(results)} candidates)")
        import time
        time.sleep(0.5)
        if page >= total_pages:
            break
        page += 1

    logger.info(f"Total candidate records retrieved from OpenFEC: {len(total_candidates)}")

    # Step 2: Extract candidate IDs and batch-fetch principal committees
    logger.info("Extracting Candidate IDS and Website")
    all_cids = [c["candidate_id"] for c in total_candidates if "candidate_id" in c]
    committee_batch_map = {}

    # Process committee lookups in chunks of 100 candidate IDs
    for i in range(0, len(all_cids), 100):
        chunk = all_cids[i:i + 100]
        chunk_map = fetch_committee_details_batch(api_key, chunk)
        time.sleep(0.5)
        committee_batch_map.update(chunk_map)

    # Step 3: Construct cleaned records
    normalized_records = []
    for c in total_candidates:
        cid = c.get("candidate_id")
        cmte_info = committee_batch_map.get(cid, {})
        cmte_id = cmte_info.get("committee_id")
        campaign_url = cmte_info.get("website_url")

        # Fallback search if top-level committee website_url is None
        # if not campaign_url and cmte_id:
        #     campaign_url = get_candidate_campaign_url(api_key, cid, cmte_id)

        chamber = map_fec_office_to_chamber(c.get("office"))
        district = clean_district(c.get("district"))

        normalized_records.append({
            "candidate_id": cid,
            "name": c.get("name"),
            "state": c.get("state"),
            "chamber": chamber,
            "district": district,
            "party":  map_fec_party(c.get("party_full") or c.get("party")),
            "geoid_cd": None,  # Computed downstream in dbt staging if needed
            "campaign_url": campaign_url,
            "committee_id": cmte_id,
            "incumbent_challenge": c.get("incumbent_challenge"),
            "election_year": ELECTION_YEAR,
            "fec_update_date": c.get("load_date"),
        })

    logger.info("Writing to DuckDB")
    con = duckdb.connect(str(DB_PATH))
    try:
        init_fec_db(con)
        stats = load_candidates(con, normalized_records)
        logger.info(
            f"Ingestion complete. Total: {stats['total']} | "
            f"Inserted: {stats['inserted']} | Updated: {stats['updated']} | "
            f"Unchanged: {stats['unchanged']}"
        )
    finally:
        con.close()


if __name__ == "__main__":
    ingest_candidates()