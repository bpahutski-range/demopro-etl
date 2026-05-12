import time
import logging
from datetime import datetime, timezone, timedelta
from requests.auth import HTTPBasicAuth
from requests import Session
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from requests.exceptions import RequestException

from config import (
    DEMOPRO_USERNAME,
    DEMOPRO_PASSWORD,
    BASE_URL,
    POLL_INTERVAL,
    MAX_WAIT,
    STALENESS_DAYS,
)

# -----------------------------------------------------------------
# 1. Logging
#    Rather than print(), we use Python's built-in logging module.
#    This gives us timestamps, severity levels (INFO, WARNING, ERROR),
#    and the ability to write to a file later — all for free.
#    getLogger(__name__) means the logger is named after this file
#    ("api"), which makes it easy to trace which file a log came from.
# -----------------------------------------------------------------
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------
# 2. Shared session
#    Auth is attached once here. Every call made through _session automatically includes it. You never pass credentials again.
# -----------------------------------------------------------------
_session = Session()
_session.auth = HTTPBasicAuth(DEMOPRO_USERNAME, DEMOPRO_PASSWORD)


# -----------------------------------------------------------------
# 3. Base GET and POST helpers with retry logic
#    The @retry decorator from tenacity wraps these functions.
#    If a RequestException is raised (network error, 500, etc.),
#    tenacity catches it, waits, and tries again automatically.
#    stop_after_attempt(3) — give up after 3 tries
#    wait_exponential(multiplier=1, min=1, max=10) — wait 1s, 2s, 4s
# -----------------------------------------------------------------
@retry(
    retry=retry_if_exception_type(RequestException),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    reraise=True
)
def _get(endpoint, params=None):
    """Internal GET — retries on failure, returns parsed JSON."""
    url = f"{BASE_URL}/{endpoint}"
    logger.info(f"GET {endpoint} | params={params}")
    resp = _session.get(url, params=params or {})
    resp.raise_for_status()
    return resp.json()


@retry(
    retry=retry_if_exception_type(RequestException),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    reraise=True
)
def _post(endpoint, params=None):
    """Internal POST — retries on failure, returns parsed JSON."""
    url = f"{BASE_URL}/{endpoint}"
    logger.info(f"POST {endpoint} | params={params}")
    resp = _session.post(url, params=params or {})
    resp.raise_for_status()
    return resp.json()


# -----------------------------------------------------------------
# 4. Account summary — called ONCE per pipeline run, not per handle.
#    Your notebook called this inside find_existing_request_id(),
#    which meant one API call per handle. This way, main.py fetches
#    it once and passes it into the functions that need it.
# -----------------------------------------------------------------
def get_account_summary(days=90):
    """Fetch the full account summary for the last N days."""
    logger.info(f"Fetching account summary for last {days} days.")
    return _get("get_account_summary", params={"days": days})


# -----------------------------------------------------------------
# 5. Stale check
#    Given a handle/network and the already-fetched account summary,
#    returns a request_id if a fresh analysis exists, or None if
#    the pipeline should trigger a new one.
#    "Fresh" means the analysis_date is within STALENESS_DAYS.
# -----------------------------------------------------------------
def find_fresh_request_id(handle, network, summary):
    """
    Check account summary for a recent completed analysis.
    Returns request_id if found and fresh, else None.
    """
    handle_norm  = handle.lower().strip()
    network_norm = network.lower().strip()
    cutoff       = datetime.now(timezone.utc) - timedelta(days=STALENESS_DAYS)

    matches = [
        entry for entry in summary.get("analyses_data", [])
        if entry.get("handle", "").lower().strip()  == handle_norm
        and entry.get("network", "").lower().strip() == network_norm
    ]

    if not matches:
        logger.info(f"No existing analysis found for @{handle}/{network}.")
        return None

    matches.sort(key=lambda e: e.get("date", ""), reverse=True)
    most_recent = matches[0]
    analysis_date = datetime.fromisoformat(most_recent.get("date")).replace(tzinfo=timezone.utc)

    if analysis_date < cutoff:
        logger.info(f"Existing analysis for @{handle}/{network} is stale (date: {analysis_date.date()}). Will refresh.")
        return None

    request_id = most_recent.get("request_id")
    logger.info(f"Fresh analysis found for @{handle}/{network} — request_id={request_id} (date: {analysis_date.date()}).")
    return request_id


# -----------------------------------------------------------------
# 6. Core analysis functions — identical logic to your notebook
#    but using the shared session, logging, and the stale check
#    instead of the old cache check.
# -----------------------------------------------------------------
def get_analysis_by_request_id(request_id):
    """Fetch a completed analysis directly by request_id."""
    return _post("get_aggregate_analysis", params={
        "using": "request_id",
        "data":  request_id,
    })


def request_fresh_analysis(handle, network):
    """
    Trigger a new analysis for a handle/network pair.
    Polls until complete or timeout.
    """
    logger.info(f"Requesting fresh analysis for @{handle}/{network}.")
    result  = _post("get_aggregate_analysis", params={
        "using":   "social_media_handle",
        "data":    handle,
        "network": network.lower(),
    })

    status  = result.get("status")
    elapsed = 0

    while status in ("Queued", "In progress") and elapsed < MAX_WAIT:
        pct = result.get("percent_complete", 0)
        logger.info(f"  Status: {status} ({pct}% complete) — waiting {POLL_INTERVAL}s...")
        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL
        result   = _post("get_aggregate_analysis", params={
            "using": "request_id",
            "data":  result.get("request_id"),
        })
        status = result.get("status")

    if status == "Complete":
        logger.info(f"Analysis complete for @{handle}/{network}.")
    else:
        logger.warning(f"Analysis did not complete for @{handle}/{network}. Final status: {status}")

    return result


def get_taxonomy(sections=None):
    """Fetch taxonomy codes. Uses TAXONOMY_SECTIONS from config by default."""
    from config import TAXONOMY_SECTIONS
    params = {"section": sections or TAXONOMY_SECTIONS} if (sections or TAXONOMY_SECTIONS) else {}
    logger.info(f"Fetching taxonomy: {'all sections' if not params.get('section') else params['section']}")
    return _get("get_codes", params=params)


# -----------------------------------------------------------------
# 7. The public extract function — what main.py actually calls.
#    Takes a handle, network, and the pre-fetched summary.
#    Decides whether to reuse or refresh. Returns raw JSON.
# -----------------------------------------------------------------
def extract(handle, network, summary):
    """
    Main entry point for api.py.
    Checks for a fresh existing analysis first.
    Falls back to requesting a new one if stale or missing.
    """
    request_id = find_fresh_request_id(handle, network, summary)

    if request_id:
        return get_analysis_by_request_id(request_id)

    return request_fresh_analysis(handle, network)