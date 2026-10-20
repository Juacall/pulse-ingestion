from datetime import datetime

def parse_api_date(date_str: str | None) -> datetime | None:
    """Safely converts heterogeneous Congress API date strings to Python datetime."""
    if not date_str:
        return None
    try:
        # Handles 'Z' suffixes and standard ISO variations
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except ValueError:
        return None