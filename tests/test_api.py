from pathlib import Path
import sys
from unittest.mock import Mock, patch

import pytest
import requests

# Add scripts directory to path for direct module imports.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from api import api_get, fetch_members_for_congress


@patch("api.time.sleep")
@patch("api.requests.get")
def test_api_get_retries_chunked_encoding_error(mock_get, mock_sleep):
    response = Mock(status_code=200)
    response.json.return_value = {"ok": True}
    mock_get.side_effect = [
        requests.exceptions.ChunkedEncodingError("Response ended prematurely"),
        response,
    ]

    assert api_get("https://example.test/resource", retries=3) == {"ok": True}
    assert mock_get.call_count == 2
    mock_sleep.assert_any_call(2)


@patch("api.time.sleep")
@patch("api.requests.get")
def test_api_get_raises_after_transient_errors_exhaust_retries(mock_get, mock_sleep):
    network_error = requests.exceptions.ChunkedEncodingError("Connection dropped")
    mock_get.side_effect = network_error

    with pytest.raises(RuntimeError, match="Failed after 2 retries") as exc_info:
        api_get("https://example.test/resource", retries=2)

    assert exc_info.value.__cause__ is network_error
    assert mock_get.call_count == 2
    mock_sleep.assert_any_call(0.1)
    assert mock_sleep.call_count == 3


@patch("api.time.sleep")
@patch("api.requests.get")
def test_api_get_does_not_retry_http_client_errors(mock_get, mock_sleep):
    response = Mock(status_code=400)
    response.raise_for_status.side_effect = requests.exceptions.HTTPError("Bad request")
    mock_get.return_value = response

    with pytest.raises(requests.exceptions.HTTPError, match="Bad request"):
        api_get("https://example.test/resource", retries=3)

    assert mock_get.call_count == 1
    assert mock_sleep.call_count == 1


def test_fetch_members_for_congress_filters_chamber_and_limits():
    members = [
        {
            "bioguideId": "H000001",
            "district": 1,
            "terms": {"item": [{"chamber": "House of Representatives"}]},
        },
        {
            "bioguideId": "S000001",
            "district": None,
            "terms": {"item": [{"chamber": "Senate"}]},
        },
        {
            "bioguideId": "H000002",
            "district": 2,
            "terms": {"item": [{"chamber": "House of Representatives"}]},
        },
    ]

    with patch("api.paginate", return_value=members):
        house_members = fetch_members_for_congress("house", member_limit=1)
        senate_members = fetch_members_for_congress("senate", member_limit=None)

    assert [member["bioguideId"] for member in house_members] == ["H000001"]
    assert house_members[0]["chamber"] == "House"
    assert [member["bioguideId"] for member in senate_members] == ["S000001"]
    assert senate_members[0]["_geoid_cd"] is None
