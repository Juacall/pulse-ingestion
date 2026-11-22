import logging
import os
from pathlib import Path
import tempfile
import time
import zipfile
import duckdb
import httpx
from api import fetch_zip_csv_text
from database import _restore_set_aside_rows, _set_aside_tables_without_primary_key

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "dev.duckdb"

DONOR_TABLE_KEYS = {
    "politician_fec_summary": ["politician_id"],
    "politician_top_donors": ["politician_id", "donor_name", "donor_type", "cycle"],
}

FEC_CYCLE = os.getenv("FEC_CYCLE", "26")  # 2025-2026 Cycle
FEC_CYCLE_YEAR = os.getenv("FEC_CYCLE_YEAR", "2026")
REFRESH_EMPLOYERS = os.getenv("REFRESH_EMPLOYERS", "true").lower() == "true"

URL_LINKAGES = f"https://www.fec.gov/files/bulk-downloads/20{FEC_CYCLE}/ccl{FEC_CYCLE}.zip"
URL_PAC_CONTRIBS = f"https://www.fec.gov/files/bulk-downloads/20{FEC_CYCLE}/pas2{FEC_CYCLE}.zip"
URL_ALL_CANDIDATE_SUMMARIES = f"https://www.fec.gov/files/bulk-downloads/20{FEC_CYCLE}/weball{FEC_CYCLE}.zip"
URL_HOUSE_SENATE_DETAILED = f"https://www.fec.gov/files/bulk-downloads/20{FEC_CYCLE}/webk{FEC_CYCLE}.zip"
URL_COMMITTEE_TRANSFERS = f"https://www.fec.gov/files/bulk-downloads/20{FEC_CYCLE}/oth{FEC_CYCLE}.zip"

EMPLOYER_STATUS_MAP = {
    "NULL": ("UNDISCLOSED EMPLOYER (INDIVIDUALS)", "Employer Not Listed on FEC Filing"),
    "NONE": ("UNDISCLOSED EMPLOYER (INDIVIDUALS)", "Employer Not Listed on FEC Filing"),
    "N/A": ("UNDISCLOSED EMPLOYER (INDIVIDUALS)", "Employer Not Listed on FEC Filing"),
    "NA": ("UNDISCLOSED EMPLOYER (INDIVIDUALS)", "Employer Not Listed on FEC Filing"),
    "UNKNOWN": ("UNDISCLOSED EMPLOYER (INDIVIDUALS)", "Employer Not Listed on FEC Filing"),
    "INFORMATION REQUESTED": ("UNDISCLOSED EMPLOYER (INDIVIDUALS)", "Employer Not Listed on FEC Filing"),
    "INFORMATION REQUESTED PER BEST EFFORTS": ("UNDISCLOSED EMPLOYER (INDIVIDUALS)", "Employer Not Listed on FEC Filing"),
    "RETIRED": ("RETIRED (INDIVIDUALS)", "Employment Status (Individual Donors)"),
    "NOT EMPLOYED": ("NOT EMPLOYED (INDIVIDUALS)", "Employment Status (Individual Donors)"),
    "UNEMPLOYED": ("NOT EMPLOYED (INDIVIDUALS)", "Employment Status (Individual Donors)"),
    "SELF-EMPLOYED": ("SELF-EMPLOYED (INDIVIDUALS)", "Employment Status (Individual Donors)"),
    "SELF EMPLOYED": ("SELF-EMPLOYED (INDIVIDUALS)", "Employment Status (Individual Donors)"),
    "SELF": ("SELF-EMPLOYED (INDIVIDUALS)", "Employment Status (Individual Donors)"),
    "HOMEMAKER": ("HOMEMAKER (INDIVIDUALS)", "Employment Status (Individual Donors)"),
    "STUDENT": ("STUDENT (INDIVIDUALS)", "Employment Status (Individual Donors)"),
}

logger = logging.getLogger(__name__)


def extract_zip_to_temp(zip_data: bytes | str, suffix: str = ".txt") -> Path:
    raw_bytes = zip_data.encode("utf-8") if isinstance(zip_data, str) else bytes(zip_data)

    tmp_zip = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
    try:
        tmp_zip.write(raw_bytes)
    finally:
        tmp_zip.close()

    out_path = Path(tempfile.mktemp(suffix=suffix))
    with zipfile.ZipFile(tmp_zip.name, "r") as zf:
        member_name = zf.namelist()[0]
        with zf.open(member_name) as src, open(out_path, "wb") as dst:
            while True:
                chunk = src.read(1024 * 1024 * 4)  # 4 MB chunks
                if not chunk:
                    break
                dst.write(chunk)

    Path(tmp_zip.name).unlink(missing_ok=True)
    return out_path

def write_pipe_text_to_temp(pipe_text: bytes | str, suffix: str = ".txt") -> Path:
    out_path = Path(tempfile.mktemp(suffix=suffix))
    if isinstance(pipe_text, bytes):
        out_path.write_bytes(pipe_text)
    else:
        out_path.write_text(pipe_text, encoding="utf-8", errors="replace")
    return out_path

def _register_pipe_file_view(conn: duckdb.DuckDBPyConnection, txt_path: Path, view_name: str):
    conn.execute(f"""
        CREATE OR REPLACE TEMP VIEW {view_name} AS
        SELECT *
        FROM read_csv(
            '{txt_path}',
            delim='|',
            header=false,
            auto_detect=true,
            null_padding=true,
            ignore_errors=true,
            all_varchar=true
        );
    """)


def ingest_top_employers_from_api(conn: duckdb.DuckDBPyConnection, cycle: str = "2026"):
    """Pre-computes top employer-grouped individual donors into main.politician_top_donors via OpenFEC API."""
    api_key = os.getenv("FEC_API_KEY")
    if not api_key:
        logger.warning("⚠️ FEC_API_KEY not set; skipping employer API ingestion.")
        return

    targets = conn.execute("""
                           SELECT DISTINCT
                               p.id AS politician_id,
                               ccl.committee_id
                           FROM main.dim_politicians p
                                    JOIN main.politician_fec_summary s
                                         ON p.id = s.politician_id
                                    JOIN (
                               SELECT DISTINCT TRIM(candidate_id) AS candidate_id, TRIM(committee_id) AS committee_id
                               FROM stg_fec_ccl
                               WHERE UPPER(TRIM(committee_designation)) IN ('P', 'A')
                               UNION
                               SELECT DISTINCT TRIM(fec_candidate_id) AS candidate_id, TRIM(committee_id) AS committee_id
                               FROM main.dim_politicians
                               WHERE fec_candidate_id IS NOT NULL AND committee_id IS NOT NULL
                           ) ccl
                                         ON TRIM(p.fec_candidate_id) = ccl.candidate_id
                           WHERE COALESCE(s.individual_contributions, 0) > 0
                           ORDER BY p.id, ccl.committee_id
                           """).fetchall()

    print(f"📥 Fetching pre-aggregated top employers via OpenFEC API for {len(targets)} active committees...")
    url = "https://api.open.fec.gov/v1/schedules/schedule_a/by_employer/"
    rows_to_insert = []
    failed_committees: list[tuple[str, str, str]] = []
    rate_limit_hits = 0

    with httpx.Client(timeout=10.0) as client:
        for idx, (politician_id, committee_id) in enumerate(targets, start=1):
            params = {
                "api_key": api_key,
                "committee_id": committee_id,
                "cycle": int(cycle),
                "sort": "-total",
                "per_page": 10,
            }

            succeeded = False
            last_error = "Unknown error"

            for attempt in range(1, 4):
                try:
                    res = client.get(url, params=params)

                    if res.status_code == 429:
                        rate_limit_hits += 1
                        wait_s = 5 * attempt
                        last_error = f"HTTP 429 Too Many Requests (attempt {attempt}/3)"
                        logger.warning(
                            f"[OpenFEC by_employer] Rate limited for {politician_id} ({committee_id}); "
                            f"sleeping {wait_s}s (attempt {attempt}/3)"
                        )
                        time.sleep(wait_s)
                        continue

                    if res.status_code in (401, 403):
                        body_preview = res.text[:200]
                        raise RuntimeError(
                            f"OpenFEC authentication/authorization failed (HTTP {res.status_code}): {body_preview}"
                        )

                    if res.status_code != 200:
                        last_error = f"HTTP {res.status_code}: {res.text[:200]}"
                        logger.warning(
                            f"[OpenFEC by_employer] Non-200 for {politician_id} ({committee_id}): {last_error}"
                        )
                        break

                    grouped_employers: dict[str, dict] = {}
                    for item in res.json().get("results", []):
                        raw_emp = (item.get("employer") or "").strip().upper()
                        total = float(item.get("total") or 0.0)
                        if total <= 0:
                            continue

                        if not raw_emp or raw_emp in EMPLOYER_STATUS_MAP:
                            clean_name, industry_label = EMPLOYER_STATUS_MAP.get(
                                raw_emp,
                                ("UNDISCLOSED EMPLOYER (INDIVIDUALS)", "Employer Not Listed on FEC Filing"),
                            )
                        else:
                            clean_name = raw_emp
                            industry_label = "Employer (Individual Donors)"

                        if clean_name not in grouped_employers:
                            grouped_employers[clean_name] = {"amount": 0.0, "industry": industry_label}
                        grouped_employers[clean_name]["amount"] += total

                    top_5 = sorted(grouped_employers.items(), key=lambda x: -x[1]["amount"])[:5]
                    for clean_name, meta in top_5:
                        rows_to_insert.append((
                            politician_id,
                            clean_name,
                            round(meta["amount"], 2),
                            "Employer",
                            meta["industry"],
                            str(cycle),
                        ))

                    succeeded = True
                    break

                except httpx.RequestError as exc:
                    last_error = f"{type(exc).__name__}: {exc}"
                    logger.warning(
                        f"[OpenFEC by_employer] Network error for {politician_id} ({committee_id}) "
                        f"(attempt {attempt}/3): {last_error}"
                    )
                    time.sleep(attempt)
                except ValueError as exc:
                    last_error = f"JSONDecodeError: {exc}"
                    logger.warning(
                        f"[OpenFEC by_employer] Invalid JSON for {politician_id} ({committee_id}): {last_error}"
                    )
                    break

            if not succeeded:
                failed_committees.append((politician_id, committee_id, last_error))

            if idx % 50 == 0:
                print(
                    f"   ...processed {idx}/{len(targets)} committees "
                    f"({len(rows_to_insert)} employer rows, {len(failed_committees)} failed)"
                )
            time.sleep(0.4)

    if rows_to_insert:
        conn.executemany("""
                         INSERT INTO main.politician_top_donors
                             (politician_id, donor_name, amount, donor_type, industry, cycle)
                         VALUES (?, ?, ?, ?, ?, ?)
                             ON CONFLICT (politician_id, donor_name, donor_type, cycle) DO UPDATE SET
                             amount = EXCLUDED.amount,
                                                                                               industry = EXCLUDED.industry
                         """, rows_to_insert)

    print(
        f"✅ Employer ingestion complete: {len(rows_to_insert)} rows inserted across "
        f"{len(targets) - len(failed_committees)}/{len(targets)} committees "
        f"(rate_limits={rate_limit_hits}, failures={len(failed_committees)})."
    )

    if failed_committees:
        sample = "; ".join(f"{pid}/{cid} -> {err}" for pid, cid, err in failed_committees[:10])
        logger.error(f"❌ Failed OpenFEC employer lookups ({len(failed_committees)} total). Sample: {sample}")


def ingest_independent_expenditures(conn: duckdb.DuckDBPyConnection):
    """Ingests FEC independent_expenditure_2026.csv into DuckDB using exact schema headers."""
    print("📥 Downloading & staging FEC Independent Expenditures...")
    ie_url = f"https://www.fec.gov/files/bulk-downloads/{FEC_CYCLE_YEAR}/independent_expenditure_{FEC_CYCLE_YEAR}.csv"

    conn.execute(f"""
        CREATE OR REPLACE TABLE main.stg_fec_independent_expenditures AS
        SELECT
            TRIM(spe_id) AS super_pac_id,
            REGEXP_REPLACE(UPPER(TRIM(spe_nam)), '\\s+', ' ', 'g') AS super_pac_name,
            TRIM(cand_id) AS fec_candidate_id,
            REGEXP_REPLACE(UPPER(TRIM(cand_name)), '\\s+', ' ', 'g') AS candidate_name,
            TRIM(can_office) AS candidate_office,
            TRIM(can_office_state) AS candidate_state,
            CAST(TRY_CAST(exp_amo AS DOUBLE) AS DOUBLE) AS amount,
            CASE
                WHEN UPPER(TRIM(sup_opp)) = 'S' THEN 'SUPPORT'
                WHEN UPPER(TRIM(sup_opp)) = 'O' THEN 'OPPOSE'
                ELSE 'UNKNOWN'
            END AS stance,
            TRY_CAST(exp_date AS DATE) AS expenditure_date,
            TRIM(pur) AS purpose,
            TRY_CAST(file_num AS BIGINT) AS file_num,
            '{FEC_CYCLE_YEAR}' AS cycle
        FROM read_csv_auto('{ie_url}', header=True)
        WHERE exp_amo IS NOT NULL
          AND spe_id LIKE 'C%'
          AND CAST(TRY_CAST(exp_amo AS DOUBLE) AS DOUBLE) BETWEEN 1.0 AND 50000000.0;
    """)

    conn.execute("""
                 CREATE OR REPLACE VIEW main.v_super_pac_impact AS
                 SELECT
                     super_pac_name,
                     cycle,
                     SUM(CASE WHEN stance = 'SUPPORT' THEN amount ELSE 0 END) AS total_support_spending,
                     SUM(CASE WHEN stance = 'OPPOSE' THEN amount ELSE 0 END) AS total_oppose_spending,
                     SUM(amount) AS total_independent_expenditures,
                     COUNT(DISTINCT fec_candidate_id) AS candidates_targeted,
                     MAX_BY(candidate_name, amount) AS top_target_candidate,
                     MAX(amount) AS top_target_amount
                 FROM main.stg_fec_independent_expenditures
                 GROUP BY super_pac_name, cycle
                 ORDER BY total_independent_expenditures DESC;
                 """)
    print("✅ Super PAC Independent Expenditure ingestion complete.")


def run_bulk_ingestion():
    print("🚀 Starting Complete FEC Bulk Data Ingestion (weball + webk) into DuckDB...")
    conn = duckdb.connect(str(DB_PATH))

    conn.execute("SET memory_limit = '1.5GB';")
    conn.execute("SET threads = 2;")
    conn.execute("SET preserve_insertion_order = false;")

    temp_files: list[Path] = []
    try:
        # 0. Committee Transfers (oth26)
        oth_path = write_pipe_text_to_temp(fetch_zip_csv_text(URL_COMMITTEE_TRANSFERS), suffix="_oth.txt")
        temp_files.append(oth_path)
        _register_pipe_file_view(conn, oth_path, "v_oth_raw")

        # 1. Candidate-Committee Linkages (ccl26)
        ccl_path = write_pipe_text_to_temp(fetch_zip_csv_text(URL_LINKAGES), suffix="_ccl.txt")
        temp_files.append(ccl_path)
        _register_pipe_file_view(conn, ccl_path, "v_ccl_raw")

        # 2. PAC & Committee Contributions (pas226)
        pas2_path = write_pipe_text_to_temp(fetch_zip_csv_text(URL_PAC_CONTRIBS), suffix="_pas2.txt")
        temp_files.append(pas2_path)
        _register_pipe_file_view(conn, pas2_path, "v_pas2_raw")

        # 3. All Candidate Financial Summaries (weball26)
        weball_path = write_pipe_text_to_temp(fetch_zip_csv_text(URL_ALL_CANDIDATE_SUMMARIES), suffix="_weball.txt")
        temp_files.append(weball_path)
        _register_pipe_file_view(conn, weball_path, "v_weball_raw")

        # 4. House/Senate Detailed Committee Summaries (webk26)
        webk_path = write_pipe_text_to_temp(fetch_zip_csv_text(URL_HOUSE_SENATE_DETAILED), suffix="_webk.txt")
        temp_files.append(webk_path)
        _register_pipe_file_view(conn, webk_path, "v_webk_raw")


        conn.execute("""
                     CREATE OR REPLACE TABLE stg_fec_oth AS
                     SELECT
                         TRIM(column00) AS recipient_committee_id,
                         UPPER(TRIM(column05)) AS transaction_type,
                         UPPER(TRIM(column06)) AS entity_type,
                         column07 AS contributor_name,
                         TRY_CAST(column14 AS DOUBLE) AS amount,
                         TRIM(column15) AS contributor_committee_id
                     FROM v_oth_raw
                     WHERE TRY_CAST(column14 AS DOUBLE) > 0
                       AND UPPER(TRIM(column05)) IN ('18K', '18G', '18H', '16C');
                     """)

        # 1. Candidate-Committee Linkages (ccl26)
        print("📥 Ingesting Candidate-Committee Linkages (ccl26)...")

        conn.execute("""
                     CREATE OR REPLACE TABLE stg_fec_ccl AS
                     SELECT DISTINCT
                         TRIM(column0) AS candidate_id,
                         TRIM(column3) AS committee_id,
                         UPPER(TRIM(column5)) AS committee_designation
                     FROM v_ccl_raw
                     WHERE column0 IS NOT NULL
                       AND TRIM(column0) != ''
              AND column3 IS NOT NULL
              AND TRIM(column3) != ''
              AND UPPER(TRIM(column5)) IN ('P', 'A');
                     """)

        # 2. PAC & Committee Contributions (pas226)
        print("📥 Ingesting PAC & Committee Contributions (pas2)...")


        conn.execute("""
                     CREATE OR REPLACE TABLE stg_fec_pas2 AS
                     SELECT
                         TRIM(column00) AS committee_id,
                         UPPER(TRIM(column05)) AS transaction_type,
                         column06 AS entity_type,
                         column07 AS contributor_name,
                         column13 AS transaction_date,
                         TRY_CAST(column14 AS DOUBLE) AS amount,
                         TRIM(column15) AS candidate_id,
                         TRIM(column16) AS other_id,
                         column17 AS transaction_id
                     FROM v_pas2_raw
                     WHERE TRY_CAST(column14 AS DOUBLE) > 0
                       AND UPPER(TRIM(column05)) IN ('24K', '24C', '24Z', '24F', '24H', '24N', '24P', '24R');
                     """)

        # 3. All Candidate Financial Summaries (weball26)
        print("📥 Ingesting Candidate Macro Summaries (weball26)...")

        conn.execute("""
                     CREATE OR REPLACE TABLE stg_fec_candidate_summaries AS
                     SELECT
                         column00 AS fec_candidate_id,
                         column01 AS candidate_name,
                         TRY_CAST(column05 AS DOUBLE) AS total_receipts,
                         TRY_CAST(column25 AS DOUBLE) AS pac_contributions,
                         TRY_CAST(column17 AS DOUBLE) AS individual_contributions,
                         TRY_CAST(column11 AS DOUBLE) AS candidate_self_contributions,
                         column27 AS coverage_end_date
                     FROM v_weball_raw;
                     """)

        # 4. House/Senate Detailed Committee Summaries (webk26)
        print("📥 Ingesting Detailed House/Senate Committee Summaries (webk26)...")


        conn.execute("""
                     CREATE OR REPLACE TABLE stg_fec_webo_details AS
                     SELECT
                         ccl.candidate_id AS fec_candidate_id,
                         SUM(TRY_CAST(column07 AS DOUBLE)) AS individual_itemized,
                         SUM(TRY_CAST(column08 AS DOUBLE)) AS individual_unitemized
                     FROM v_webk_raw w
                              JOIN stg_fec_ccl ccl ON TRIM(w.column00) = ccl.committee_id
                     GROUP BY ccl.candidate_id;
                     """)

        # 5. Upsert main.politician_fec_summary
        print("📊 Updating Candidate Financial Summaries in main.politician_fec_summary...")
        set_aside_summary = _set_aside_tables_without_primary_key(
            conn, {"politician_fec_summary": DONOR_TABLE_KEYS["politician_fec_summary"]}
        )
        conn.execute("""
                     CREATE TABLE IF NOT EXISTS main.politician_fec_summary (
                                                                                politician_id VARCHAR PRIMARY KEY,
                                                                                fec_candidate_id VARCHAR,
                                                                                total_receipts DOUBLE,
                                                                                pac_contributions DOUBLE,
                                                                                individual_contributions DOUBLE,
                                                                                individual_itemized DOUBLE,
                                                                                individual_unitemized DOUBLE,
                                                                                grassroots_percentage DOUBLE,
                                                                                coverage_end_date VARCHAR,
                                                                                summary_source VARCHAR
                     );

                     INSERT INTO main.politician_fec_summary (
                         politician_id,
                         fec_candidate_id,
                         total_receipts,
                         pac_contributions,
                         individual_contributions,
                         individual_itemized,
                         individual_unitemized,
                         grassroots_percentage,
                         coverage_end_date,
                         summary_source
                     )
                     SELECT
                         p.id AS politician_id,
                         p.fec_candidate_id,
                         COALESCE(s.total_receipts, 0.0) AS total_receipts,
                         COALESCE(s.pac_contributions, 0.0) AS pac_contributions,
                         COALESCE(s.individual_contributions, 0.0) AS individual_contributions,
                         CASE
                             WHEN w.individual_itemized IS NOT NULL THEN w.individual_itemized
                             ELSE ROUND(COALESCE(s.individual_contributions, 0.0) * 0.65, 2)
                             END AS individual_itemized,
                         CASE
                             WHEN w.individual_unitemized IS NOT NULL THEN w.individual_unitemized
                             ELSE ROUND(COALESCE(s.individual_contributions, 0.0) * 0.35, 2)
                             END AS individual_unitemized,
                         CASE
                             WHEN COALESCE(s.total_receipts, 0.0) > 0 THEN
                                 ROUND(
                                         (
                                             CASE
                                                 WHEN w.individual_unitemized IS NOT NULL THEN w.individual_unitemized
                                                 ELSE ROUND(COALESCE(s.individual_contributions, 0.0) * 0.35, 2)
                                                 END / s.total_receipts
                                             ) * 100.0, 2
                                 )
                             ELSE 0.0
                             END AS grassroots_percentage,
                         s.coverage_end_date,
                         'FEC_BULK_WEBALL_WEBK' AS summary_source
                     FROM main.dim_politicians p
                              LEFT JOIN (
                         SELECT
                             committee_id,
                             FIRST(candidate_id ORDER BY CASE WHEN committee_designation = 'P' THEN 0 ELSE 1 END, candidate_id) AS candidate_id
                         FROM stg_fec_ccl
                         GROUP BY committee_id
                     ) ccl ON TRIM(p.committee_id) = ccl.committee_id
                              JOIN stg_fec_candidate_summaries s
                                   ON COALESCE(NULLIF(TRIM(p.fec_candidate_id), ''), ccl.candidate_id) = TRIM(s.fec_candidate_id)
                              LEFT JOIN stg_fec_webo_details w
                                        ON COALESCE(NULLIF(TRIM(p.fec_candidate_id), ''), ccl.candidate_id) = TRIM(w.fec_candidate_id)
                     WHERE COALESCE(NULLIF(TRIM(p.fec_candidate_id), ''), ccl.candidate_id) IS NOT NULL
                         ON CONFLICT (politician_id) DO UPDATE SET
                         fec_candidate_id = EXCLUDED.fec_candidate_id,
                                                            total_receipts = EXCLUDED.total_receipts,
                                                            pac_contributions = EXCLUDED.pac_contributions,
                                                            individual_contributions = EXCLUDED.individual_contributions,
                                                            individual_itemized = EXCLUDED.individual_itemized,
                                                            individual_unitemized = EXCLUDED.individual_unitemized,
                                                            grassroots_percentage = EXCLUDED.grassroots_percentage,
                                                            coverage_end_date = EXCLUDED.coverage_end_date,
                                                            summary_source = EXCLUDED.summary_source;
                     """)
        _restore_set_aside_rows(conn, set_aside_summary, DONOR_TABLE_KEYS)

        # 6. Populate politician_top_donors for PACs
        print("📊 Aggregating Top PAC Donors across all politicians...")
        set_aside_donors = _set_aside_tables_without_primary_key(
            conn, {"politician_top_donors": DONOR_TABLE_KEYS["politician_top_donors"]}
        )
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS main.politician_top_donors (
                politician_id VARCHAR,
                donor_name VARCHAR,
                amount DOUBLE,
                donor_type VARCHAR,
                industry VARCHAR,
                cycle VARCHAR,
                PRIMARY KEY (politician_id, donor_name, donor_type, cycle)
            );

            WITH valid_ccl AS (
                SELECT DISTINCT
                    TRIM(candidate_id) AS candidate_id,
                    TRIM(committee_id) AS committee_id
                FROM stg_fec_ccl
                WHERE UPPER(TRIM(committee_designation)) IN ('P', 'A')
            ),
            pas2_grouped AS (
                SELECT
                    p.id AS politician_id,
                    COALESCE(
                        NULLIF(NULLIF(UPPER(TRIM(pas.contributor_name)), ''), 'NULL'),
                        'UNDISCLOSED COMMITTEE'
                    ) AS donor_name,
                    SUM(pas.amount) AS amount
                FROM stg_fec_pas2 pas
                LEFT JOIN valid_ccl ccl_cmte ON pas.other_id = ccl_cmte.committee_id
                JOIN main.dim_politicians p
                    ON TRIM(p.fec_candidate_id) = COALESCE(NULLIF(pas.candidate_id, ''), ccl_cmte.candidate_id)
                    OR TRIM(p.committee_id) = pas.other_id
                GROUP BY p.id, 2
            ),
            oth_grouped AS (
                SELECT
                    p.id AS politician_id,
                    COALESCE(
                        NULLIF(NULLIF(UPPER(TRIM(oth.contributor_name)), ''), 'NULL'),
                        'UNDISCLOSED COMMITTEE'
                    ) AS donor_name,
                    SUM(oth.amount) AS amount
                FROM stg_fec_oth oth
                LEFT JOIN valid_ccl ccl ON oth.recipient_committee_id = ccl.committee_id
                JOIN main.dim_politicians p
                    ON TRIM(p.fec_candidate_id) = ccl.candidate_id
                    OR TRIM(p.committee_id) = oth.recipient_committee_id
                GROUP BY p.id, 2
            ),
            combined_pacs AS (
                SELECT
                    COALESCE(a.politician_id, b.politician_id) AS politician_id,
                    COALESCE(a.donor_name, b.donor_name) AS donor_name,
                    GREATEST(COALESCE(a.amount, 0.0), COALESCE(b.amount, 0.0)) AS amount
                FROM pas2_grouped a
                FULL OUTER JOIN oth_grouped b
                    ON a.politician_id = b.politician_id
                    AND a.donor_name = b.donor_name
            )
            INSERT INTO main.politician_top_donors (politician_id, donor_name, amount, donor_type, industry, cycle)
            SELECT
                politician_id,
                donor_name,
                ROUND(amount, 2) AS amount,
                'PAC' AS donor_type,
                'PAC / Political Committee' AS industry,
                '{FEC_CYCLE_YEAR}' AS cycle
            FROM combined_pacs
            WHERE amount > 0
            ON CONFLICT (politician_id, donor_name, donor_type, cycle) DO UPDATE SET
                amount = EXCLUDED.amount,
                industry = EXCLUDED.industry;
        """)
        _restore_set_aside_rows(conn, set_aside_donors, DONOR_TABLE_KEYS)

        # 7. Global Leaderboard View
        conn.execute("""
                     CREATE OR REPLACE VIEW main.v_global_top_donors_leaderboard AS
                     SELECT
                         d.donor_name,
                         d.donor_type,
                         d.cycle,
                         SUM(d.amount) AS total_amount_contributed,
                         COUNT(DISTINCT d.politician_id) AS politicians_funded,
                         MAX_BY(p.name, d.amount) AS top_recipient_name,
                         MAX(d.amount) AS top_recipient_amount
                     FROM main.politician_top_donors d
                              JOIN main.dim_politicians p ON d.politician_id = p.id
                     GROUP BY d.donor_name, d.donor_type, d.cycle
                     ORDER BY total_amount_contributed DESC;
                     """)

        # 8. Print Financial Summary Audit Report
        summary = conn.execute("""
                               SELECT
                                   SUM(total_receipts) AS total_raised,
                                   SUM(pac_contributions) AS total_pac,
                                   SUM(individual_itemized) AS total_large_indiv,
                                   SUM(individual_unitemized) AS total_grassroots,
                                   ROUND(AVG(grassroots_percentage), 2) AS avg_grassroots_pct
                               FROM main.politician_fec_summary;
                               """).fetchone()

        ingest_independent_expenditures(conn)
        if REFRESH_EMPLOYERS:
            ingest_top_employers_from_api(conn, cycle=FEC_CYCLE_YEAR)

        # 9. Clean up temporary staging tables and views
        print("🧹 Cleaning up staging tables and raw views...")
        conn.execute("""
                     DROP VIEW IF EXISTS v_ccl_raw;
                     DROP VIEW IF EXISTS v_oth_raw;
                     DROP VIEW IF EXISTS v_pas2_raw;
                     DROP VIEW IF EXISTS v_weball_raw;
                     DROP VIEW IF EXISTS v_webk_raw;
                     DROP TABLE IF EXISTS stg_fec_ccl;
                     DROP TABLE IF EXISTS stg_fec_oth;
                     DROP TABLE IF EXISTS stg_fec_pas2;
                     DROP TABLE IF EXISTS stg_fec_candidate_summaries;
                     DROP TABLE IF EXISTS stg_fec_webo_details;
                     """)


    finally:
        for path in temp_files:
            path.unlink(missing_ok=True)
        conn.close()

    print("\n" + "=" * 50)
    print("📋 FEC BULK INGESTION AUDIT SUMMARY")
    print("=" * 50)
    print(f"Total Raised:       ${summary[0]:,.2f}" if summary[0] else "Total Raised:       $0.00")
    print(f"PAC Contributions:  ${summary[1]:,.2f}" if summary[1] else "PAC Contributions:  $0.00")
    print(f"Large Individual:   ${summary[2]:,.2f}" if summary[2] else "Large Individual:   $0.00")
    print(f"Grassroots (<=200): ${summary[3]:,.2f}" if summary[3] else "Grassroots (<=200): $0.00")
    print(f"Avg Grassroots %:   {summary[4]}%" if summary[4] is not None else "Avg Grassroots %:   0.0%")
    print("=" * 50)
    print("✅ FEC Bulk Ingestion Complete!")


if __name__ == "__main__":
    run_bulk_ingestion()