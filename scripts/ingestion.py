"""
scripts/ingestion.py

Data loading and idempotent upsert functions for DuckDB raw tables.
Includes MD5 checksum generation and timestamp tracking.
"""

import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

def _compute_hash(*values) -> str:
    """Compute an MD5 hash over given string/numeric values."""
    payload = "|".join("" if v is None else str(v) for v in values)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


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
        website_url = m.get("website_url"),
        image_url = m.get("image_url")
        update_date = m.get("updateDate")

        row_hash = _compute_hash(bid, name, state, chamber, district, party, geoid_cd, website_url, image_url)

        # Tuple Order:
        # [0]: bid, [1]: name, [2]: state, [3]: chamber, [4]: district,
        # [5]: party, [6]: geoid_cd, [7]: website_url, [8]: update_date,
        # [9]: row_hash, [10]: now_iso
        rows.append((bid, name, state, chamber, district, party, geoid_cd, website_url,image_url, update_date, row_hash, now_iso))

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
        bid, new_hash = r[0], r[10]
        if bid not in existing_hashes:
            inserted += 1
        elif existing_hashes[bid] != new_hash:
            updated += 1
        else:
            unchanged += 1

    db.executemany("""
                   INSERT INTO main.raw_members (
                       bioguide_id, name, state, chamber, district, party, geoid_cd, website_url,  image_url, update_date, row_hash, ingested_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT (bioguide_id) DO UPDATE SET
                       name = EXCLUDED.name,
                                                        state = EXCLUDED.state,
                                                        chamber = EXCLUDED.chamber,
                                                        district = EXCLUDED.district,
                                                        party = EXCLUDED.party,
                                                        geoid_cd = EXCLUDED.geoid_cd,
                                                        website_url = EXCLUDED.website_url,
                                                        image_url = EXCLUDED.image_url,
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
        bill_number = str(b.get("number"))
        title = b.get("title")
        introduced_date = b.get("introducedDate")
        update_date = b.get("updateDate") or b.get("updateDateIncludingText")

        # Current bill status: the Congress API exposes the most-recent action
        # inline on the sponsored/cosponsored list response (no extra call). We
        # track current status only, so this is overwritten on each run.
        latest_action = b.get("latestAction") or {}
        latest_action_date = latest_action.get("actionDate")
        latest_action_text = latest_action.get("text")

        row_hash = _compute_hash(
            congress, bill_type, bill_number, title, member_id, relationship,
            introduced_date, latest_action_date, latest_action_text
        )
        rows.append((
            congress, bill_type, bill_number, title,
            introduced_date, latest_action_date, latest_action_text,
            member_id, relationship, update_date, row_hash, now_iso
        ))

    existing_hashes = {}
    chunk_size = 250
    for start in range(0, len(rows), chunk_size):
        chunk = rows[start:start + chunk_size]
        placeholders = ",".join("(?, ?, ?)" for _ in chunk)
        params = [member_id, relationship]
        for row in chunk:
            params.extend((row[0], row[1], row[2]))
        existing_hashes.update({
            (congress, bill_type, bill_number, existing_member_id, existing_relationship): row_hash
            for congress, bill_type, bill_number, existing_member_id, existing_relationship, row_hash
            in db.execute(
                f"""
                SELECT congress, bill_type, bill_number, member_id, relationship, row_hash
                FROM main.raw_bills
                WHERE member_id = ? AND relationship = ?
                  AND (congress, bill_type, bill_number) IN ({placeholders})
                """,
                params,
            ).fetchall()
        })

    inserted = updated = unchanged = 0
    for row in rows:
        key = (row[0], row[1], row[2], row[7], row[8])
        existing_hash = existing_hashes.get(key)
        if existing_hash is None:
            inserted += 1
        elif existing_hash != row[10]:
            updated += 1
        else:
            unchanged += 1

    db.executemany("""
        INSERT INTO main.raw_bills (
            congress, bill_type, bill_number, title, introduced_date,
            latest_action_date, latest_action_text, member_id, relationship,
            update_date, row_hash, ingested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (congress, bill_type, bill_number, member_id, relationship) DO UPDATE SET
            title = EXCLUDED.title,
            introduced_date = EXCLUDED.introduced_date,
            latest_action_date = EXCLUDED.latest_action_date,
            latest_action_text = EXCLUDED.latest_action_text,
            update_date = EXCLUDED.update_date,
            row_hash = EXCLUDED.row_hash,
            ingested_at = EXCLUDED.ingested_at
        WHERE main.raw_bills.row_hash != EXCLUDED.row_hash
    """, rows)

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
