import json
import sys
from pathlib import Path

import requests

# Add script folder to path if executed standalone
sys.path.insert(0, str(Path(__file__).resolve().parent))

from api import fetch_members_for_congress
from config import (
    API_KEY,
    DEFAULT_CHAMBER,
    MEMBER_LIMIT as DEFAULT_MEMBER_LIMIT,
)
from database import get_district_geoids, addColumn
from ingestion import load_members

STATE_NAME_TO_CODE = {
    "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR",
    "CALIFORNIA": "CA", "COLORADO": "CO", "CONNECTICUT": "CT", "DELAWARE": "DE",
    "FLORIDA": "FL", "GEORGIA": "GA", "HAWAII": "HI", "IDAHO": "ID",
    "ILLINOIS": "IL", "INDIANA": "IN", "IOWA": "IA", "KANSAS": "KS",
    "KENTUCKY": "KY", "LOUISIANA": "LA", "MAINE": "ME", "MARYLAND": "MD",
    "MASSACHUSETTS": "MA", "MICHIGAN": "MI", "MINNESOTA": "MN", "MISSISSIPPI": "MS",
    "MISSOURI": "MO", "MONTANA": "MT", "NEBRASKA": "NE", "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ", "NEW MEXICO": "NM", "NEW YORK": "NY",
    "NORTH CAROLINA": "NC", "NORTH DAKOTA": "ND", "OHIO": "OH", "OKLAHOMA": "OK",
    "OREGON": "OR", "PENNSYLVANIA": "PA", "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD", "TENNESSEE": "TN", "TEXAS": "TX", "UTAH": "UT",
    "VERMONT": "VT", "VIRGINIA": "VA", "WASHINGTON": "WA", "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI", "WYOMING": "WY", "DISTRICT OF COLUMBIA": "DC",
    "PUERTO RICO": "PR", "GUAM": "GU", "VIRGIN ISLANDS": "VI",
    "AMERICAN SAMOA": "AS", "NORTHERN MARIANA ISLANDS": "MP"
}

def resolve_state_code(raw_state: str | None) -> str | None:
    if not raw_state:
        return None
    clean_state = raw_state.strip().upper()
    # Return directly if already 2-letter postal code, else map from full name
    if len(clean_state) == 2:
        return clean_state
    return STATE_NAME_TO_CODE.get(clean_state)

def extract_image_url(depiction_data) -> str | None:
    """Safely extracts imageUrl whether depiction is a dict or string."""
    if not depiction_data:
        return None

    # Case 1: Already a dictionary (from response.json())
    if isinstance(depiction_data, dict):
        return depiction_data.get("imageUrl")

    # Case 2: String payload (JSON or Python dict string)
    if isinstance(depiction_data, str):
        try:
            # Attempt standard JSON parse first
            parsed = json.loads(depiction_data.replace("'", '"'))
            if isinstance(parsed, dict):
                return parsed.get("imageUrl")
        except (json.JSONDecodeError, TypeError):
            pass

    return None

def fetch_member_official_url(bioguide_id: str) -> str | None:
    """Fetches full member detail payload to retrieve officialUrl."""
    url = f"https://api.congress.gov/v3/member/{bioguide_id}?api_key={API_KEY}"
    res = requests.get(url)
    if res.status_code == 200:
        return res.json().get("member", {}).get("officialWebsiteUrl")
    return None

def ingest_members(
        db,
        member_limit: int | None = DEFAULT_MEMBER_LIMIT,
        random_sample: bool = False,
        chamber: str = DEFAULT_CHAMBER,
):
    """Fetch current Congress members and upsert the selected chamber to raw_members."""
    chamber = (chamber or DEFAULT_CHAMBER).lower()

    limit_desc = "ALL" if member_limit is None else str(member_limit)
    print(f"\nFetching {chamber} members from the Congress API (limit={limit_desc}, random={random_sample})...")
    members = fetch_members_for_congress(
        chamber=chamber,
        member_limit=member_limit,
        shuffle=random_sample,
    )

    district_geoids = get_district_geoids(db)
    count = 0
    for member in members:
        state = (member.get("state") or member.get("stateCode") or "").upper()
        district = member.get("district")
        state_code = resolve_state_code(state)
        member['stateCode'] = state_code
        try:
            district = int(district) if district is not None else None
        except (TypeError, ValueError):
            district = None
        member["district"] = district
        member["_geoid_cd"] = district_geoids.get((state_code, district)) if district is not None else None

        official_url = fetch_member_official_url(member.get("bioguideId"))
        member["website_url"] = official_url
        member["image_url"] = extract_image_url(member.get("depiction"))
        count+=1
        print(f'Member Processed: {member.get("bioguideId")} , {member.get("name")} : [{count}/{len(members)}] ')
    member_stats = load_members(db, members)
    return members, member_stats
