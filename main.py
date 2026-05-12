import logging
import pandas as pd
from datetime import datetime, timezone, timedelta

import config
from api import get_account_summary, get_taxonomy, extract
from transform import transform_taxonomy, transform_handle, transform_facts
from load import load_taxonomy, load_handles, load_facts

# -----------------------------------------------------------------
# 1. Logging setup
#    This is the only place logging gets configured.
#    Every other file calls logging.getLogger(__name__) and
#    inherits this configuration automatically.
#
#    We write to both the console (so you can watch it run)
#    and a log file (so you have a record when it runs unattended).
#
#    %(asctime)s   — timestamp
#    %(name)s      — which file the log came from (api, transform, etc.)
#    %(levelname)s — INFO, WARNING, ERROR
#    %(message)s   — the actual message
# -----------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),                  # console
        logging.FileHandler("pipeline.log"),      # file
    ]
)

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------
# 2. Run list builder
#
#    Returns a deduplicated list of (handle, network) tuples
#    that need to be processed in this run. Combines two sources:
#
#    a) New accounts — in the input table but not yet in dim_handles
#    b) Stale accounts — in dim_handles with analysis_date > 90 days ago
#
#    RIGHT NOW: both are stubbed with hardcoded values so the rest
#    of the pipeline can be built and tested immediately.
#
#    LATER: replace each stub with the appropriate Snowflake query.
#    The SQL is included in the comments so nothing needs to be
#    figured out when the time comes.
# -----------------------------------------------------------------
def get_new_accounts():
    """
    Returns (handle, network) pairs from the input table that
    have no existing entry in dim_handles.

    REPLACE THIS with a Snowflake query:

        SELECT i.handle, i.network
        FROM input_handles i
        LEFT JOIN dim_handles d
            ON  i.handle  = d.handle
            AND i.network = d.network
        WHERE d.handle IS NULL
    """
    logger.info("Fetching new accounts (stubbed — replace with Snowflake query).")
    return [
        ("sabrinacarpenter", "instagram"),
        ("sabrinaannlynn",   "twitter"),
    ]


def get_stale_accounts():
    """
    Returns (handle, network) pairs from dim_handles where
    analysis_date is older than STALENESS_DAYS.

    REPLACE THIS with a Snowflake query:

        SELECT handle, network
        FROM dim_handles
        WHERE analysis_date < DATEADD(day, -{staleness_days}, CURRENT_TIMESTAMP())
    """.format(staleness_days=config.STALENESS_DAYS)
    logger.info(f"Fetching stale accounts (stubbed — replace with Snowflake query).")
    return [
        ("tigerwoods",      "twitter"),
        ("ericstonestreet", "instagram"),
    ]


def build_run_list():
    """
    Combines new and stale accounts into a single deduplicated list.
    A (handle, network) pair could theoretically appear in both —
    deduplication via set ensures it only runs once.
    """
    new_accounts   = get_new_accounts()
    stale_accounts = get_stale_accounts()
    combined       = list(set(new_accounts + stale_accounts))
    logger.info(f"Run list: {len(combined)} (handle, network) pairs to process.")
    return combined


# -----------------------------------------------------------------
# 3. Main pipeline
# -----------------------------------------------------------------
def main():
    logger.info("=" * 60)
    logger.info("Pipeline run started.")
    logger.info("=" * 60)

    run_start = datetime.now(timezone.utc)

    # -- Taxonomy (run every time for now; optimize later if slow)
    logger.info("Fetching and loading taxonomy...")
    raw_codes    = get_taxonomy()
    dim_taxonomy = transform_taxonomy(raw_codes)
    load_taxonomy(dim_taxonomy)

    # -- Account summary (fetched once, passed into every extract call)
    summary = get_account_summary(days=config.STALENESS_DAYS)

    # -- Build run list
    run_list = build_run_list()

    if not run_list:
        logger.info("No accounts to process. Exiting.")
        return

    # -- Process each (handle, network) pair
    succeeded = []
    failed    = []

    handle_rows = []
    fact_frames = []

    for handle, network in run_list:
        logger.info(f"Processing @{handle}/{network}...")
        try:
            raw = extract(handle, network, summary)

            if raw.get("status") != "Complete":
                logger.warning(f"Skipping @{handle}/{network} — status: {raw.get('status')}")
                failed.append((handle, network))
                continue

            handle_row = transform_handle(handle, network, raw)
            handle_rows.append(handle_row)

            facts = transform_facts(handle, network, handle_row["request_id"], raw)
            fact_frames.append(facts)

            succeeded.append((handle, network))

        except Exception as e:
            logger.error(f"Failed @{handle}/{network} — {e}", exc_info=True)
            failed.append((handle, network))
            continue

    # -- Load everything in bulk after the loop
    if handle_rows:
        load_handles(pd.DataFrame(handle_rows))

    if fact_frames:
        load_facts(pd.concat(fact_frames, ignore_index=True))

    # -- Run summary
    run_end     = datetime.now(timezone.utc)
    duration    = (run_end - run_start).seconds

    logger.info("=" * 60)
    logger.info(f"Pipeline run complete in {duration}s.")
    logger.info(f"  Succeeded: {len(succeeded)} — {succeeded}")
    logger.info(f"  Failed:    {len(failed)} — {failed}")
    logger.info("=" * 60)


# -----------------------------------------------------------------
# 4. Entry point
#    This block means: only run main() if this file is executed
#    directly (e.g. `python main.py`). If another file imports
#    main.py, main() does NOT run automatically.
#    This is standard Python and you'll see it everywhere.
# -----------------------------------------------------------------
if __name__ == "__main__":
    main()