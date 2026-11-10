"""
scripts/ingestion.py

Data loading and idempotent upsert functions for DuckDB raw tables.
Includes MD5 checksum generation and timestamp tracking.
"""

import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import json

def _compute_hash(*values) -> str:
    """Compute an MD5 hash over given string/numeric values."""
    payload = "|".join("" if v is None else str(v) for v in values)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def extract_policy_area(raw_policy_area) -> str:
    """Safely extracts the policy area name from dicts, JSON strings, or None values."""
    if not raw_policy_area:
        return "Uncategorized"

    # Handle stringified JSON from raw DB loads
    if isinstance(raw_policy_area, str):
        try:
            raw_policy_area = json.loads(raw_policy_area)
        except (json.JSONDecodeError, TypeError):
            # Fallback if the field is just a plain string scalar
            return raw_policy_area.strip() or "Uncategorized"

    # Safely access dict key
    if isinstance(raw_policy_area, dict):
        return raw_policy_area.get("name") or "Uncategorized"

    return "Uncategorized"

def load_candidates(db, candidates: List[Dict[str, Any]]) -> Dict[str, int]:
    if not candidates:
        return {"total": 0, "inserted": 0, "updated": 0, "unchanged": 0}

    now_iso = datetime.now(timezone.utc).isoformat()
    rows = []

    for c in candidates:
        cid = c.get("candidate_id")
        if not cid:
            continue

        name = c.get("name")
        state = c.get("state")
        chamber = c.get("chamber")
        district = c.get("district")
        party = c.get("party")

        # Calculate Census GEOID standard (e.g. NC State FIPS 37 + District 07 = 3707)
        geoid_cd = c.get("geoid_cd")

        campaign_url = c.get("campaign_url")
        committee_id = c.get("committee_id")
        incumbent_challenge = c.get("incumbent_challenge")
        election_year = int(c.get("election_year", 2026))
        fec_update_date = c.get("fec_update_date")
        first_file_date = c.get("first_file_date")
        candidate_status = c.get("candidate_status")
        has_raised_funds = c.get("has_raised_funds",False)

        row_hash = _compute_hash(
            cid, name, state, chamber, district, party,
            geoid_cd, campaign_url, committee_id, incumbent_challenge, election_year, candidate_status
        )

        rows.append((
            cid, name, state, chamber, district, party, geoid_cd,
            campaign_url, committee_id, incumbent_challenge, election_year,
            fec_update_date,first_file_date,candidate_status, has_raised_funds, row_hash, now_iso
        ))

    if not rows:
        return {"total": 0, "inserted": 0, "updated": 0, "unchanged": 0}

    # Identify existing row hashes
    cids = [r[0] for r in rows]
    placeholders = ",".join("?" for _ in cids)
    existing_hashes = {
        cid: h
        for cid, h in db.execute(
            f"SELECT candidate_id, row_hash FROM main.raw_fec_candidates WHERE candidate_id IN ({placeholders})",
            cids,
        ).fetchall()
    }

    inserted = updated = unchanged = 0
    for r in rows:
        cid, new_hash = r[0], r[15]
        if cid not in existing_hashes:
            inserted += 1
        elif existing_hashes[cid] != new_hash:
            updated += 1
        else:
            unchanged += 1

    db.executemany("""
                   INSERT INTO main.raw_fec_candidates (
                       candidate_id, name, state, chamber, district, party, geoid_cd,
                       campaign_url, committee_id, incumbent_challenge, election_year,
                       fec_update_date, first_file_date, candidate_status,has_raised_funds ,row_hash, ingested_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT (candidate_id) DO UPDATE SET
                       name = EXCLUDED.name,
                                                         state = EXCLUDED.state,
                                                         chamber = EXCLUDED.chamber,
                                                         district = EXCLUDED.district,
                                                         party = EXCLUDED.party,
                                                         geoid_cd = EXCLUDED.geoid_cd,
                                                         campaign_url = EXCLUDED.campaign_url,
                                                         committee_id = EXCLUDED.committee_id,
                                                         incumbent_challenge = EXCLUDED.incumbent_challenge,
                                                         election_year = EXCLUDED.election_year,
                                                         fec_update_date = EXCLUDED.fec_update_date,
                                                         first_file_date = EXCLUDED.first_file_date,
                                                         candidate_status = EXCLUDED.candidate_status,
                                                         has_raised_funds = EXCLUDED.has_raised_funds,
                                                         row_hash = EXCLUDED.row_hash,
                                                         ingested_at = EXCLUDED.ingested_at
                                                     WHERE main.raw_fec_candidates.row_hash IS NULL
                                                        OR main.raw_fec_candidates.row_hash != EXCLUDED.row_hash
                   """, rows)

    return {
        "total": len(rows),
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
    }

def _safe_int(val):
    """Safely cast year values to int or None."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None

def load_members(db, members):
    """
    Idempotently upsert member rows into raw_members.
    Primary Key: (bioguide_id).

    Returns a stats dict describing the incremental outcome:
      {"total", "inserted", "updated", "unchanged"}
    """
    if not members:
        return {"total": 0, "inserted": 0, "updated": 0, "unchanged": 0}

    now_iso = datetime.now(timezone.utc).isoformat()
    rows = []
    for m in members:
        bid = m.get("bioguideId")
        name = m.get("name")
        state = m.get("state") or m.get("stateCode")
        chamber = m.get("chamber") or ("House" if m.get("district") is not None else "Senate")
        district = m.get("district")
        party = m.get("partyName") or m.get("party")
        geoid_cd = m.get("geoid_cd") or m.get("_geoid_cd")
        website_url = m.get("website_url")
        image_url = m.get("image_url")
        update_date = m.get("updateDate")

        # Extract start_year and end_year (handles direct keys or nested terms payloads)
        start_year = _safe_int(m.get("start_year") or m.get("startYear"))
        end_year = _safe_int(m.get("end_year") or m.get("endYear"))
        print(m)

        row_hash = _compute_hash(
            bid, name, state, chamber, district, party, geoid_cd,
            website_url, image_url, start_year, end_year
        )

        # Tuple Layout:
        # [0]: bid, [1]: name, [2]: state, [3]: chamber, [4]: district,
        # [5]: party, [6]: geoid_cd, [7]: website_url, [8]: image_url,
        # [9]: start_year, [10]: end_year, [11]: update_date, [12]: row_hash, [13]: now_iso
        rows.append((
            bid, name, state, chamber, district, party, geoid_cd,
            website_url, image_url, start_year, end_year, update_date,
            row_hash, now_iso
        ))

    # Classify each incoming row against stored records
    existing_hashes = {}
    bids = [r[0] for r in rows if r[0] is not None]
    if bids:
        placeholders = ",".join("?" for _ in bids)
        existing_hashes = {
            bid: h
            for bid, h in db.execute(
                f"SELECT bioguide_id, row_hash FROM main.raw_members WHERE bioguide_id IN ({placeholders})",
                bids,
            ).fetchall()
        }

    inserted = updated = unchanged = 0
    for r in rows:
        bid, new_hash = r[0], r[12]
        if bid not in existing_hashes:
            inserted += 1
        elif existing_hashes[bid] != new_hash:
            updated += 1
        else:
            unchanged += 1

    db.executemany("""
                   INSERT INTO main.raw_members (
                       bioguide_id, name, state, chamber, district, party, geoid_cd,
                       website_url, image_url, start_year, end_year, update_date,
                       row_hash, ingested_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT (bioguide_id) DO UPDATE SET
                       name = EXCLUDED.name,
                                                        state = EXCLUDED.state,
                                                        chamber = EXCLUDED.chamber,
                                                        district = EXCLUDED.district,
                                                        party = EXCLUDED.party,
                                                        geoid_cd = EXCLUDED.geoid_cd,
                                                        website_url = EXCLUDED.website_url,
                                                        image_url = EXCLUDED.image_url,
                                                        start_year = EXCLUDED.start_year,
                                                        end_year = EXCLUDED.end_year,
                                                        update_date = EXCLUDED.update_date,
                                                        row_hash = EXCLUDED.row_hash,
                                                        ingested_at = EXCLUDED.ingested_at
                                                    WHERE main.raw_members.row_hash IS NULL
                                                       OR main.raw_members.row_hash != EXCLUDED.row_hash
                   """, rows)

    return {
        "total": len(rows),
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
    }


def load_bills(db, bills, member_id, relationship):
    """
    Idempotently upsert bill rows into raw_bills.
    Primary Key: (congress, bill_type, bill_number, member_id, relationship).

    Returns counts for inserted, updated, and unchanged member-bill relationships.
    """
    if not bills:
        return {"total": 0, "inserted": 0, "updated": 0, "unchanged": 0}

    now_iso = datetime.now(timezone.utc).isoformat()
    rows = []

    for b in bills:
        congress = b.get("congress")
        bill_type = b.get("type")
        bill_number = str(b.get("number")) if b.get("number") is not None else ""
        title = b.get("title")

        # Safely extract policyArea string using extract_policy_area helper
        policy_area = extract_policy_area(b.get("policyArea"))

        # Option A: Extract CRS summary (if present in dict)
        summary = b.get("summary")

        introduced_date = b.get("introducedDate")
        update_date = b.get("updateDate") or b.get("updateDateIncludingText")

        # Extract latest action
        latest_action = b.get("latestAction") or {}
        latest_action_date = latest_action.get("actionDate")
        latest_action_text = latest_action.get("text")

        # Include summary and policy_area in hash calculation
        row_hash = _compute_hash(
            congress, bill_type, bill_number, title, policy_area,
            summary, member_id, relationship, introduced_date,
            latest_action_date, latest_action_text
        )

        rows.append((
            congress,           # [0]
            bill_type,          # [1]
            bill_number,        # [2]
            title,              # [3]
            policy_area,        # [4]
            summary,            # [5]
            introduced_date,    # [6]
            latest_action_date, # [7]
            latest_action_text, # [8]
            member_id,          # [9]
            relationship,       # [10]
            update_date,        # [11]
            row_hash,           # [12]
            now_iso             # [13]
        ))

    # 1. Efficient Hash Lookup (Scope to member_id & relationship)
    existing_hashes = {
        (c, bt, bn): h
        for c, bt, bn, h in db.execute(
            """
            SELECT congress, bill_type, bill_number, row_hash
            FROM main.raw_bills
            WHERE member_id = ? AND relationship = ?
            """,
            [member_id, relationship],
        ).fetchall()
    }

    # 2. Metric Calculation
    inserted = updated = unchanged = 0
    for row in rows:
        key = (row[0], row[1], row[2])  # (congress, bill_type, bill_number)
        row_hash = row[12]
        existing_hash = existing_hashes.get(key)

        if existing_hash is None:
            inserted += 1
        elif existing_hash != row_hash:
            updated += 1
        else:
            unchanged += 1

    # 3. Idempotent Upsert into DuckDB
    upsert_sql = """
                 INSERT INTO main.raw_bills (
                     congress, bill_type, bill_number, title, policy_area, summary,
                     introduced_date, latest_action_date, latest_action_text,
                     member_id, relationship, update_date, row_hash, ingested_at
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT (congress, bill_type, bill_number, member_id, relationship) DO UPDATE SET
                     title = EXCLUDED.title,
                                                                                                    policy_area = EXCLUDED.policy_area,
                                                                                                    summary = COALESCE(EXCLUDED.summary, main.raw_bills.summary),
                                                                                                    introduced_date = EXCLUDED.introduced_date,
                                                                                                    latest_action_date = EXCLUDED.latest_action_date,
                                                                                                    latest_action_text = EXCLUDED.latest_action_text,
                                                                                                    update_date = EXCLUDED.update_date,
                                                                                                    row_hash = EXCLUDED.row_hash,
                                                                                                    ingested_at = EXCLUDED.ingested_at
                                                                                                WHERE main.raw_bills.row_hash != EXCLUDED.row_hash \
                 """

    db.executemany(upsert_sql, rows)

    return {
        "total": len(rows),
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
    }


def load_amendments(db, amendments, congress, bill_type, bill_number):
    """
    Idempotently upsert amendment rows into raw_amendments for a parent bill.
    Primary Key: (congress, bill_type, bill_number, amendment_number).
    """
    if not amendments:
        return

    now_iso = datetime.now(timezone.utc).isoformat()
    rows = []
    for a in amendments:
        sponsor = a.get("sponsor")
        sponsor_id = sponsor.get("bioguideId") if isinstance(sponsor, dict) else None
        amendment_num = str(a.get("number", ""))
        amendment_type = a.get("type")
        description = a.get("description")
        purpose = a.get("purpose")
        update_date = a.get("updateDate")

        row_hash = _compute_hash(
            congress, bill_type, bill_number, amendment_num, amendment_type,
            description, purpose, sponsor_id
        )
        rows.append((
            congress, bill_type, bill_number, amendment_num, amendment_type,
            description, purpose, sponsor_id, update_date, row_hash, now_iso
        ))

    db.executemany("""
        INSERT INTO main.raw_amendments (
            congress, bill_type, bill_number, amendment_number, amendment_type,
            description, purpose, sponsor_id, update_date, row_hash, ingested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (congress, bill_type, bill_number, amendment_number, amendment_type) DO UPDATE SET
            amendment_type = EXCLUDED.amendment_type,
            description = EXCLUDED.description,
            purpose = EXCLUDED.purpose,
            sponsor_id = EXCLUDED.sponsor_id,
            update_date = EXCLUDED.update_date,
            row_hash = EXCLUDED.row_hash,
            ingested_at = EXCLUDED.ingested_at
        WHERE main.raw_amendments.row_hash != EXCLUDED.row_hash
    """, rows)
