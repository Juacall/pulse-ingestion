"""
scripts/api.py

Congress API client handling HTTP requests, rate throttling, pagination,
and data fetching routines.
"""

from pathlib import Path
from urllib.parse import urlparse, parse_qs
import sys
import time

import requests

# Add script folder to path if executed standalone or as module
sys.path.insert(0, str(Path(__file__).resolve().parent))

try:
    from config import (
        API_KEY,
        BASE_URL,
        CONGRESS,
        HOUSE_BILL_TYPES,
        SENATE_BILL_TYPES,
        MEMBER_LIMIT,
    )
except ImportError:
    from scripts.config import (
        API_KEY,
        BASE_URL,
        CONGRESS,
        HOUSE_BILL_TYPES,
        SENATE_BILL_TYPES,
        MEMBER_LIMIT,
    )


def api_get(url: str, params: dict | None = None, retries: int = 5) -> dict:
    """
    Execute an HTTP GET request against the Congress API with throttling and
    retries for rate limits, server errors, and transient network failures.
    """
    query = {"api_key": API_KEY, "format": "json"}
    if params:
        query.update(params)

    last_network_error = None
    transient_errors = (
        requests.exceptions.ChunkedEncodingError,
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
    )
    for attempt in range(retries):
        time.sleep(0.1)  # throttle every request
        try:
            response = requests.get(url, params=query)
        except transient_errors as exc:
            last_network_error = exc
            if attempt + 1 == retries:
                break
            wait = 2 ** (attempt + 1)
            print(
                f"  Network error on attempt {attempt + 1}/{retries}: {exc}. "
                f"Retrying in {wait}s..."
            )
            time.sleep(wait)
            continue

        # 1. Handle Rate Limiting (429)
        if response.status_code == 429:
            # Respect the API's Retry-After header if provided, else default to a longer sleep (e.g., 60s)
            retry_after = int(response.headers.get("Retry-After", 60))
            print(f"Rate limited (429). Waiting {retry_after}s before retrying...")
            time.sleep(retry_after)
            continue
        #  Handle Server Errors
        if response.status_code >= 500:
            wait = 2 ** (attempt + 1)
            print(f"  {response.status_code} on attempt {attempt + 1}, retrying in {wait}s...")
            time.sleep(wait)
            continue
        response.raise_for_status()
        return response.json()

    error = RuntimeError(f"Failed after {retries} retries: {url}")
    if last_network_error is not None:
        raise error from last_network_error
    raise error


def paginate(url: str, result_key: str, params: dict | None = None) -> list:
    """
    Collect all pages of results for a Congress API endpoint.

    The API returns a pagination.next URL in the response body when more pages
    exist. We follow it until exhausted, accumulating all results into one list.
    We request 250 items per page (the API maximum) to minimise round trips.
    """
    first_page_params = {**(params or {}), "limit": 250}
    results = []
    next_url = url

    while next_url:
        # Parse any query params embedded in next_url by the API
        parsed_url = urlparse(next_url)
        url_without_params = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
        url_params = {k: v[0] for k, v in parse_qs(parsed_url.query).items()}

        merged_params = {**first_page_params, **url_params}
        data = api_get(url_without_params, params=merged_params)

        results.extend(data.get(result_key, []))
        next_url = data.get("pagination", {}).get("next")

    return results


def fetch_members_for_congress(
        chamber: str = "house",
        member_limit: int | None = MEMBER_LIMIT,
        shuffle: bool = False,
) -> list:
    """Fetch current Congress members directly, optionally filtered by chamber."""
    url = f"{BASE_URL}/member/congress/{CONGRESS}"
    raw_members = paginate(url, "members")
    members = []
    seen_ids = set()

    for member in raw_members:
        bid = member.get("bioguideId")
        if not bid or bid in seen_ids:
            continue

        terms = member.get("terms", {})
        if isinstance(terms, dict):
            items = terms.get("item", [])
            if isinstance(items, dict):
                term_items = [items]
            elif isinstance(items, list):
                term_items = items
            else:
                term_items = []
        elif isinstance(terms, list):
            term_items = terms
        else:
            term_items = []

        latest_term = term_items[-1] if term_items and isinstance(term_items[-1], dict) else {}
        latest_chamber = latest_term.get("chamber") or member.get("chamber")
        district = member.get("district")
        is_senate = latest_chamber == "Senate" or (
            district is None and latest_chamber != "House of Representatives"
        )
        member_chamber = "Senate" if is_senate else "House"

        if chamber.lower() not in ("both", "all", member_chamber.lower()):
            continue

        member["chamber"] = member_chamber
        if is_senate:
            member["district"] = None
            member["_geoid_cd"] = None
        seen_ids.add(bid)
        members.append(member)

    if shuffle:
        import random
        random.shuffle(members)
    if member_limit is not None:
        members = members[:member_limit]

    print(f"  Total: {len(members)} {chamber.title()} members fetched")
    return members


def fetch_legislation_for_member(bioguide_id: str, chamber: str = "house") -> tuple[list, list]:
    """
    Fetch all bills sponsored and cosponsored by a member in the target Congress.

    Endpoints:
      - Sponsored:   /member/{bioguide_id}/sponsored-legislation
      - Cosponsored: /member/{bioguide_id}/cosponsored-legislation

    Filters bill types based on chamber:
      - Senate: S, SRES, SJRES, SCONRES
      - House:  HR, HRES, HJRES, HCONRES
      - Both:   All above types

    Returns two separate lists (sponsored, cosponsored) with bill metadata.
    """
    chamber_normalized = (chamber or "house").lower()
    if chamber_normalized == "senate":
        valid_types = SENATE_BILL_TYPES
    elif chamber_normalized in ("both", "all"):
        valid_types = HOUSE_BILL_TYPES | SENATE_BILL_TYPES
    else:
        valid_types = HOUSE_BILL_TYPES

    def _fetch(endpoint, result_key):
        url = f"{BASE_URL}/member/{bioguide_id}/{endpoint}"
        bills = []
        for b in paginate(url, result_key):
            b_congress = b.get("congress")
            if b_congress is not None and int(b_congress) != CONGRESS:
                continue

            b_type = (b.get("type") or b.get("billType") or "").upper()
            if b_type in valid_types:
                b["type"] = b_type
                bills.append(b)
        return bills

    sponsored = _fetch("sponsored-legislation", "sponsoredLegislation")
    cosponsored = _fetch("cosponsored-legislation", "cosponsoredLegislation")
    return sponsored, cosponsored


def fetch_amendments_for_bill(congress: int, bill_type: str, bill_number: str) -> list:
    """
    Fetch all amendments to a bill, regardless of who sponsored them.
    Endpoints:
      - House:  f"{BASE_URL}/bill/{congress}/hr/{bill_number}/amendments" (or hres/hjres/hconres)
      - Senate: f"{BASE_URL}/bill/{congress}/s/{bill_number}/amendments" (or sres/sjres/sconres)
    Treats 404 as an empty list (bill has no amendments).
    """
    url = f"{BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}/amendments"
    try:
        return paginate(url, "amendments")
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return []
        raise
