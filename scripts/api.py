"""
scripts/api.py

Congress API client handling HTTP requests, rate throttling, pagination,
and data fetching routines.
"""

import json
import os
import re
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import sys
import time

import httpx
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
        lastest_start_year = latest_term.get("startYear")
        lastest_end_year = latest_term.get("endYear")
        member["start_year"] = lastest_start_year
        member["end_year"] = lastest_end_year
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



def fetch_summary_for_bill(congress: int, bill_type: str, bill_number: str) -> str | None:
    """
    Fetches CRS summary text for a specific bill from Congress.gov API.
    GET /v3/bill/{congress}/{billType}/{billNumber}/summaries
    """
    url = f"{BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}/summaries?api_key={API_KEY}"
    try:
        res = requests.get(url, timeout=10)
        if res.status_code == 200:
            data = res.json()
            summaries = data.get("summaries", [])
            if summaries:
                # Get the latest CRS summary text (usually the last entry in the list)
                latest = summaries[-1]
                return latest.get("text")
    except Exception as e:
        logger.warning(f"Failed to fetch summary for bill {congress}-{bill_type}-{bill_number}: {e}")
    return None

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


# -- OpenFEC -------------------------------------------------------------------

FEC_BASE_URL = "https://api.open.fec.gov/v1"
DONOR_CYCLE = 2026
FEC_REQUEST_INTERVAL_SECONDS = max(
    0.0, float(os.getenv("FEC_REQUEST_INTERVAL_SECONDS", "4"))
)
NON_COMPANY_EMPLOYERS = {
    "ENTREPRENEUR",
    "HOMEMAKER",
    "INFORMATIONREQUESTED",
    "INFORMATIONREQUESTEDPERBESTEFFORTS",
    "NA",
    "NONE",
    "NOTEMPLOYED",
    "NULL",
    "RETIRED",
    "SELF",
    "SELFEMPLOYED",
}
_last_fec_request_at = None

class OpenFECRateLimitError(RuntimeError):
    def __init__(self, endpoint: str, retry_after: str | None = None):
        super().__init__(f"OpenFEC rate limit reached at {endpoint}.")
        self.endpoint = endpoint
        self.retry_after = retry_after
        self.context = "context unavailable"


class OpenFECRequestTimeoutError(RuntimeError):
    def __init__(self, endpoint: str):
        super().__init__(f"OpenFEC request timed out at {endpoint}.")
        self.endpoint = endpoint
        self.context = "context unavailable"


def _pace_fec_request():
    """Keep successive OpenFEC request starts at least the configured interval apart."""
    global _last_fec_request_at
    now = time.monotonic()
    if _last_fec_request_at is not None:
        remaining = FEC_REQUEST_INTERVAL_SECONDS - (now - _last_fec_request_at)
        if remaining > 0:
            time.sleep(remaining)
            now = time.monotonic()
    _last_fec_request_at = now


def _get_fec_response(endpoint: str, params: dict):
    """Request an OpenFEC endpoint without exposing the API key in HTTP errors."""
    _pace_fec_request()
    try:
        response = httpx.get(
            f"{FEC_BASE_URL}/{endpoint.lstrip('/')}",
            params=params,
            timeout=60,
        )
    except httpx.TimeoutException as error:
        raise OpenFECRequestTimeoutError(endpoint) from error
    if response.is_error:
        if response.status_code == 429:
            raise OpenFECRateLimitError(
                endpoint, response.headers.get("Retry-After")
            )
        raise RuntimeError(
            f"OpenFEC request failed with HTTP {response.status_code} at {endpoint}."
        )
    return response


def _normalize_name(name: str | None) -> str:
    return re.sub(r"[^A-Z0-9]", "", (name or "").upper())


def _candidate_name_identity(name: str | None) -> tuple[str, str]:
    """Extract normalized surname and first name from FEC/member name formats."""
    parts = (name or "").upper().split(",", maxsplit=1)
    if len(parts) == 2:
        surname = _normalize_name(parts[0])
        given_names = re.findall(r"[A-Z]+", parts[1])
        first_name = given_names[0] if given_names else ""
        return surname, first_name

    parts = re.findall(r"[A-Z]+", (name or "").upper())
    if len(parts) < 2:
        return "", ""
    return _normalize_name(parts[-1]), parts[0]


def _normalize_district(district) -> int | None:
    if district is None or str(district).strip() == "":
        return None
    try:
        return int(district)
    except (TypeError, ValueError):
        return None


def fetch_fec_candidate_mapping(
    bioguide_id: str,
    name: str,
    state: str,
    chamber: str,
    district: int | None,
    api_key: str,
) -> dict:
    """Broaden candidate search and accept only a unique name/office/state match."""
    office = {"house": "H", "senate": "S"}.get(chamber.strip().casefold())
    if office is None:
        raise ValueError(f"Unsupported chamber for FEC mapping: {chamber}")

    name_variants = [name]
    if "," in name:
        surname, given_names = name.split(",", maxsplit=1)
        given_tokens = re.findall(r"[A-Za-z]+", given_names)
        if given_tokens:
            name_variants.append(f"{given_tokens[0]} {surname.strip()}")
        name_variants.append(surname.strip())

    candidates_by_id = {}
    expected_identity = _candidate_name_identity(name)
    for query in dict.fromkeys(variant.strip() for variant in name_variants if variant.strip()):
        response = _get_fec_response(
            "candidates/search/",
            [
                ("api_key", api_key),
                ("q", query),
                ("state", state.upper()),
                ("office", office),
                ("cycle", DONOR_CYCLE),
                ("per_page", 100),
                ("sort", "-receipts"),
            ],
        )
        for candidate in response.json().get("results", []):
            candidate_id = candidate.get("candidate_id")
            if candidate_id:
                candidates_by_id[candidate_id] = candidate

        identity_matches = [
            candidate
            for candidate in candidates_by_id.values()
            if _candidate_name_identity(candidate.get("name")) == expected_identity
            and str(candidate.get("office") or "").upper() == office
            and str(candidate.get("state") or "").upper() == state.upper()
        ]
        if identity_matches:
            break

    candidates = list(candidates_by_id.values())
    options = [
        {
            "candidate_id": candidate.get("candidate_id"),
            "name": candidate.get("name"),
            "office": candidate.get("office"),
            "state": candidate.get("state"),
            "district": candidate.get("district"),
        }
        for candidate in candidates
    ]
    identity_matches = [
        candidate
        for candidate in candidates
        if _candidate_name_identity(candidate.get("name")) == expected_identity
        and str(candidate.get("office") or "").upper() == office
        and str(candidate.get("state") or "").upper() == state.upper()
    ]
    district_matches = [
        candidate
        for candidate in identity_matches
        if office != "H"
        or _normalize_district(candidate.get("district")) == district
    ]
    matches = district_matches or identity_matches

    if len(matches) == 1:
        candidate = matches[0]
        status = "MATCHED"
        candidate_district = _normalize_district(candidate.get("district"))
        if office == "H" and candidate_district != district:
            reason = (
                "Unique normalized first/last name, office, and state match; "
                "candidate district differs or was changed by redistricting."
            )
        else:
            reason = "Unique normalized first/last name, office, state, and district match."
    elif len(matches) > 1:
        candidate = None
        status = "AMBIGUOUS"
        reason = "Multiple FEC candidates match the member's name, office, and state."
    else:
        candidate = None
        status = "NOT_FOUND"
        reason = (
            "No unique normalized first/last name, office, and state match."
            if candidates
            else "OpenFEC returned no candidate search results."
        )

    return {
        "bioguide_id": bioguide_id,
        "member_name": name,
        "state_code": state.upper(),
        "chamber": chamber,
        "district_number": district,
        "fec_candidate_id": candidate.get("candidate_id") if candidate else None,
        "fec_candidate_name": candidate.get("name") if candidate else None,
        "fec_office": candidate.get("office") if candidate else None,
        "fec_state": candidate.get("state") if candidate else None,
        "fec_district": candidate.get("district") if candidate else None,
        "mapping_status": status,
        "mapping_reason": reason,
        "candidate_options": json.dumps(options),
    }


def fetch_fec_candidates_by_ids(
    fec_candidate_ids: list[str], api_key: str, cycle: int = DONOR_CYCLE
) -> list[dict]:
    """Fetch current-cycle candidate records for IDs provided by the crosswalk."""
    if not fec_candidate_ids:
        return []

    response = _get_fec_response(
        "candidates/",
        [
            ("api_key", api_key),
            ("cycle", cycle),
            ("per_page", min(len(fec_candidate_ids), 100)),
            *(("candidate_id", candidate_id) for candidate_id in fec_candidate_ids),
        ],
    )
    return response.json().get("results", [])


def fetch_candidate_committees(
        fec_candidate_id: str, api_key: str, cycle: int = DONOR_CYCLE
) -> tuple[list[dict], dict | None]:
    """Fetch non-joint-fundraising committees and extract primary campaign metadata."""
    response = _get_fec_response(
        f"candidate/{fec_candidate_id}/committees/",
        {"api_key": api_key, "cycle": cycle, "per_page": 100},
    )

    committees = {}
    primary_info = None

    for row in response.json().get("results", []):
        committee_id = row.get("committee_id")
        designation = row.get("designation")

        # Capture non-joint-fundraising committees for donor aggregation
        if committee_id and designation != "J":
            committees[committee_id] = row

        # Capture principal campaign committee ('P') details
        if designation == "P" and primary_info is None:
            primary_info = {
                "committee_id": committee_id,
                "campaign_url": row.get("website"),
            }


    return list(committees.values()), primary_info


def fetch_candidate_committee_ids(
    fec_candidate_id: str, api_key: str, cycle: int = DONOR_CYCLE
) -> list[str]:
    """Fetch candidate committee IDs, excluding joint fundraising committees."""
    return [
        committee["committee_id"]
        for committee in fetch_candidate_committees(
            fec_candidate_id, api_key, cycle
        )
    ]


def fetch_candidate_committee_report_summary(
    fec_candidate_id: str, api_key: str, cycle: int = DONOR_CYCLE
) -> dict | None:
    """Build a fallback summary from each committee's latest cycle filing."""
    committees = fetch_candidate_committees(fec_candidate_id, api_key, cycle)
    committee_summaries = []
    for committee in committees:
        committee_id = committee["committee_id"]
        response = _get_fec_response(
            f"committee/{committee_id}/reports/",
            [
                ("api_key", api_key),
                ("cycle", cycle),
                ("per_page", 100),
                ("sort", "-receipt_date"),
            ],
        )
        reports = [
            report
            for report in response.json().get("results", [])
            if report.get("most_recent") is True
        ]
        if not reports:
            continue

        latest_report = max(
            reports,
            key=lambda report: (
                str(report.get("coverage_end_date") or ""),
                str(report.get("receipt_date") or ""),
            ),
        )
        if latest_report.get("total_receipts_ytd") is not None:
            committee_summaries.append(latest_report)

    if not committee_summaries:
        return None

    coverage_dates = [
        str(report["coverage_end_date"])[:10]
        for report in committee_summaries
        if report.get("coverage_end_date")
    ]
    return {
        "total_receipts": sum(
            float(report.get("total_receipts_ytd") or 0)
            for report in committee_summaries
        ),
        "pac_contributions": sum(
            float(report.get("other_political_committee_contributions_ytd") or 0)
            for report in committee_summaries
        ),
        "individual_contributions": sum(
            float(report.get("individual_itemized_contributions_ytd") or 0)
            for report in committee_summaries
        ),
        "coverage_end_date": min(coverage_dates) if coverage_dates else None,
        "summary_source": "committee_reports",
    }


def fetch_top_pac_contributors(
    fec_candidate_id: str, api_key: str, cycle: int = DONOR_CYCLE
) -> list[dict]:
    """Fetch top employer aggregates from the candidate's committees."""
    committee_ids, primary_committee_info = fetch_candidate_committee_ids(
        fec_candidate_id, api_key, cycle
    )
    if not committee_ids:
        return [], primary_committee_info

    response = _get_fec_response(
        "schedules/schedule_a/by_employer/",
        [
            ("api_key", api_key),
            ("cycle", cycle),
            ("sort", "-total"),
            ("per_page", 100),
            *(("committee_id", committee_id) for committee_id in committee_ids),
        ],
    )
    totals_by_employer = {}
    for row in response.json().get("results", []):
        employer = row.get("employer")
        donor_name = employer.strip() if isinstance(employer, str) else ""
        employer_category = re.sub(r"[^A-Z0-9]", "", donor_name.upper())
        if not donor_name or employer_category in NON_COMPANY_EMPLOYERS:
            continue
        donor_name = donor_name.title()
        amount = float(row.get("total") or 0.0)
        totals_by_employer[donor_name] = totals_by_employer.get(donor_name, 0.0) + amount

    top_employers = sorted(
        totals_by_employer.items(), key=lambda item: item[1], reverse=True
    )[:5]


    donors = [
        {
            "donor_name": donor_name,
            "amount": amount,
            "industry": "Employer aggregate",
        }
        for donor_name, amount in top_employers]
    return donors, primary_committee_info


def fetch_fec_candidate_summaries(
        fec_candidate_ids: list[str], api_key: str, cycle: int = DONOR_CYCLE
) -> dict[str, dict]:
    """Fetch summary totals for multiple candidates in a single OpenFEC request."""
    if not fec_candidate_ids:
        return {}

    response = _get_fec_response(
        "candidates/totals/",
        [
            ("api_key", api_key),
            ("cycle", cycle),
            ("per_page", max(len(fec_candidate_ids), 20)),
            *(("candidate_id", candidate_id) for candidate_id in fec_candidate_ids),
        ],
    )
    summaries = {}
    for row in response.json().get("results", []):
        candidate_id = row.get("candidate_id")
        if candidate_id:
            total_receipts = float(row.get("receipts") or row.get("total_receipts") or 0.0)

            # Line 11a(i): > $200 (Itemized)
            itemized = float(row.get("individual_itemized_contributions") or 0.0)

            # Line 11a Total: Total Individual Contributions
            total_indiv = float(row.get("individual_contributions") or 0.0)

            # Line 11a(ii): <= $200 (Grassroots Unitemized)
            # Derived as: Total Individual - Itemized Individual
            if "individual_unitemized_contributions" in row and row["individual_unitemized_contributions"] is not None:
                unitemized = float(row["individual_unitemized_contributions"])
            elif total_indiv >= itemized and total_indiv > 0:
                unitemized = total_indiv - itemized
            else:
                # Secondary fallback if individual_contributions is omitted in OpenFEC payload
                pac_contribs = float(row.get("other_political_committee_contributions") or 0.0)
                transfers = float(row.get("transfers_from_other_authorized_committee") or 0.0)
                unitemized = max(0.0, total_receipts - (itemized + pac_contribs + transfers))
                total_indiv = itemized + unitemized

            # Line 11c: PACs
            pac_contribs = float(row.get("other_political_committee_contributions") or 0.0)

            # Grassroots share percentage calculation
            grassroots_pct = round((unitemized / total_receipts * 100), 2) if total_receipts > 0 else 0.0

            summaries[candidate_id] = {
                "total_receipts": total_receipts,
                "pac_contributions": pac_contribs,
                "individual_contributions": total_indiv,
                "individual_itemized": itemized,       # > $200
                "individual_unitemized": unitemized,   # <= $200 (Grassroots)
                "grassroots_percentage": grassroots_pct,
                "coverage_end_date": row.get("coverage_end_date"),
                "summary_source": "candidate_totals",
            }
    return summaries
