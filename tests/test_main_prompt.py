"""
tests/test_main_prompt.py

Unit test verifying prompt validation behavior for table reset, interactive menu,
and refresh mode selection in scripts/main.py.
"""

from unittest.mock import patch
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from main import prompt_ingestion_menu, prompt_refresh_mode, prompt_reset_tables


def test_prompt_reset_tables_accepts_yes():
    with patch("builtins.input", side_effect=["y"]):
        assert prompt_reset_tables() is True

    with patch("builtins.input", side_effect=["YES"]):
        assert prompt_reset_tables() is True


def test_prompt_reset_tables_accepts_no():
    with patch("builtins.input", side_effect=["n"]):
        assert prompt_reset_tables() is False

    with patch("builtins.input", side_effect=["NO"]):
        assert prompt_reset_tables() is False


def test_prompt_reset_tables_rejects_invalid_then_accepts_valid():
    """
    Verifies that invalid input (e.g. 'maybe', '123', 'foo') prompts again
    until a valid 'y' or 'n' is entered.
    """
    with patch("builtins.input", side_effect=["maybe", "foo", "123", "y"]):
        assert prompt_reset_tables() is True

    with patch("builtins.input", side_effect=["invalid", "n"]):
        assert prompt_reset_tables() is False


def test_prompt_refresh_mode_incremental():
    """Verifies that selecting incremental refresh options returns False."""
    for val in ["1", "i", "incremental", "n", "no"]:
        with patch("builtins.input", side_effect=[val]):
            assert prompt_refresh_mode() is False


def test_prompt_refresh_mode_full():
    """Verifies that selecting full refresh options returns True."""
    for val in ["2", "f", "full", "y", "yes"]:
        with patch("builtins.input", side_effect=[val]):
            assert prompt_refresh_mode() is True


def test_prompt_refresh_mode_invalid_then_valid():
    """Verifies invalid inputs re-prompt until valid choice."""
    with patch("builtins.input", side_effect=["unknown", "999", "1"]):
        assert prompt_refresh_mode() is False


def test_prompt_ingestion_menu_options():
    """Verifies all interactive menu choices return correct configuration dicts."""
    # Option 1: House Members Only
    with patch("builtins.input", side_effect=["1"]):
        res = prompt_ingestion_menu()
        assert res["chamber"] == "house"
        assert res["ingestion_mode"] == "members"
        assert res["skip_members"] is False

    # Option 2: Senate Members Only
    with patch("builtins.input", side_effect=["2"]):
        res = prompt_ingestion_menu()
        assert res["chamber"] == "senate"
        assert res["ingestion_mode"] == "members"
        assert res["skip_members"] is False

    # Option 3: House Bills Only
    with patch("builtins.input", side_effect=["3"]):
        res = prompt_ingestion_menu()
        assert res["chamber"] == "house"
        assert res["ingestion_mode"] == "bills"
        assert res["skip_members"] is True

    # Option 4: House Amendments Only
    with patch("builtins.input", side_effect=["4"]):
        res = prompt_ingestion_menu()
        assert res["chamber"] == "house"
        assert res["ingestion_mode"] == "amendments"
        assert res["skip_members"] is True

    # Option 5: Senate Bills Only
    with patch("builtins.input", side_effect=["5"]):
        res = prompt_ingestion_menu()
        assert res["chamber"] == "senate"
        assert res["ingestion_mode"] == "bills"
        assert res["skip_members"] is True

    # Option 6: Senate Amendments Only
    with patch("builtins.input", side_effect=["6"]):
        res = prompt_ingestion_menu()
        assert res["chamber"] == "senate"
        assert res["ingestion_mode"] == "amendments"
        assert res["skip_members"] is True

    # Option 7: All (Both Chambers)
    with patch("builtins.input", side_effect=["7"]):
        res = prompt_ingestion_menu()
        assert res["chamber"] == "both"
        assert res["ingestion_mode"] == "all"
        assert res["skip_members"] is False

    # Option 8 / Exit
    with patch("builtins.input", side_effect=["8"]):
        assert prompt_ingestion_menu() is None

    with patch("builtins.input", side_effect=["q"]):
        assert prompt_ingestion_menu() is None

    # Invalid input retry
    with patch("builtins.input", side_effect=["invalid", "99", "1"]):
        res = prompt_ingestion_menu()
        assert res["chamber"] == "house"
        assert res["ingestion_mode"] == "members"


if __name__ == "__main__":
    test_prompt_reset_tables_accepts_yes()
    test_prompt_reset_tables_accepts_no()
    test_prompt_reset_tables_rejects_invalid_then_accepts_valid()
    test_prompt_refresh_mode_incremental()
    test_prompt_refresh_mode_full()
    test_prompt_refresh_mode_invalid_then_valid()
    test_prompt_ingestion_menu_options()
    print("All prompt validation tests passed successfully.")
