import os
import requests
import duckdb
import logging
from pathlib import Path
from config import DB_PATH

BASE_DIR = Path(__file__).resolve().parents[1]

LOG_PATH = BASE_DIR / "logs" / "openfec_ingestion.log"

LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

FEC_API_KEY = os.getenv("FEC_API_KEY")
CYCLE = 2026

def init_db(db_path):
    """
    Initializes DuckDB schema and tables for elections and candidates.
    Safe to execute multiple times (idempotent).
    """
    logger.info(f"Initializing DuckDB schema at: {db_path}")

    conn = duckdb.connect(db_path)

    try:

        # 2. Table: Active Election Contests
        conn.execute("""
                     CREATE TABLE IF NOT EXISTS main.fct_elections (
                                                                           election_id VARCHAR PRIMARY KEY,     -- e.g. '2026_WA_SENATE' or '2026_WA_08'
                                                                           election_cycle INTEGER NOT NULL,     -- 2026
                                                                           state_code VARCHAR(2) NOT NULL,
                         office_sought VARCHAR(1) NOT NULL,   -- 'H' (House) or 'S' (Senate)
                         district VARCHAR,                    -- '08', '01', or NULL for Senate
                         election_type VARCHAR NOT NULL,      -- 'PRIMARY', 'GENERAL', 'SPECIAL'
                         election_date DATE,
                         is_active BOOLEAN DEFAULT TRUE
                         );
                     """)

        # 3. Table: Candidate Links per Contest
        conn.execute("""
                     CREATE TABLE IF NOT EXISTS main.fct_race_candidates (
                                                                                 race_candidate_id VARCHAR PRIMARY KEY, -- e.g. '2026_WA_08_H8WA08014'
                                                                                 election_id VARCHAR NOT NULL REFERENCES main_dbt.fct_elections(election_id),
                         politician_id VARCHAR NOT NULL,        -- Foreign key to your existing candidate IDs
                         party VARCHAR NOT NULL,
                         is_incumbent BOOLEAN DEFAULT FALSE,
                         ballot_status VARCHAR DEFAULT 'CERTIFIED', -- 'CERTIFIED', 'RUNOFF', 'WITHDRAWN'
                         votes_received INTEGER DEFAULT 0,
                         vote_share_pct DOUBLE DEFAULT 0.0,
                         is_winner BOOLEAN DEFAULT FALSE
                         );
                     """)

        logger.info("DuckDB tables initialized successfully.")

    except Exception as err:
        logger.error(f"Failed to initialize DuckDB tables: {err}")
        raise err
    finally:
        conn.close()

def ingest_2026_elections():
    url = "https://api.open.fec.gov/v1/candidates/totals/"
    params = {
        "api_key": FEC_API_KEY,
        "cycle": CYCLE,
        "candidate_status": "C", # Active certified candidates
        "min_receipts": 5000.00,
        "office": ["H", "S"],
        "per_page": 100
    }

    response = requests.get(url, params=params)
    results = response.json().get("results", [])

    conn = duckdb.connect(str(DB_PATH))

    for cand in results:
        cand_id = cand.get("candidate_id")
        office = cand.get("office")
        state = cand.get("state")
        district = cand.get("district") or "00"
        party = cand.get("party")
        is_incumbent = cand.get("incumbent_challenge_full") == "Incumbent"

        # Construct Election Contest ID
        election_id = f"{CYCLE}_{state}_{office}_{district}"

        # 1. Upsert Election Contest
        conn.execute("""
                     INSERT INTO main.fct_elections (election_id, election_cycle, state_code, office_sought, district, election_type)
                     VALUES (?, ?, ?, ?, ?, 'GENERAL')
                         ON CONFLICT (election_id) DO NOTHING;
                     """, [election_id, CYCLE, state, office, district])

        # 2. Upsert Candidate Slate Link
        race_candidate_id = f"{election_id}_{cand_id}"
        conn.execute("""
                     INSERT INTO main.fct_race_candidates (race_candidate_id, election_id, politician_id, party, is_incumbent)
                     VALUES (?, ?, ?, ?, ?)
                         ON CONFLICT (race_candidate_id) DO UPDATE SET
                         party = EXCLUDED.party,
                                                                is_incumbent = EXCLUDED.is_incumbent;
                     """, [race_candidate_id, election_id, cand_id, party, is_incumbent])

    conn.close()
    print("Ingested active 2026 elections and candidate slates into DuckDB.")

if __name__ == "__main__":
    ingest_2026_elections()