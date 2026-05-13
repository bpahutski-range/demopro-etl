import logging
import os
import pandas as pd
from datetime import datetime, timezone

import config
from api import extract, get_account_summary
from transform import transform_handle, transform_facts
from load import load_handles, load_facts

# -----------------------------------------------------------------
# 1. Logging
#    Separate log file from pipeline.log so backfill runs don't
#    pollute your regular pipeline history.
# -----------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("backfill.log"),
    ]
)

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------
# 2. File paths
# -----------------------------------------------------------------
BACKFILL_CSV      = "/mnt/user-data/uploads/demopro_handles.csv"
CHECKPOINT_FILE   = "backfill_checkpoint.csv"
QUARANTINE_FILE   = "output/full_audience_quarantine.csv"
BATCH_SIZE        = 100

# -----------------------------------------------------------------
# 3. Cleaning
#    Strips @, lowercases, normalizes whitespace.
#    Splits into two DataFrames: processable handles and quarantine.
# -----------------------------------------------------------------
def load_and_clean(filepath):
    """
    Reads the backfill CSV, cleans handles, and splits into:
        - clean_df:     rows with real platform handles
        - quarantine_df: rows with Network == 'Full Audience'
    """
    df = pd.read_csv(filepath)
    df.columns = df.columns.str.strip()

    # Separate Full Audience rows before any cleaning
    quarantine_df = df[df["Network"].str.strip() == "Full Audience"].copy()
    clean_df      = df[df["Network"].str.strip() != "Full Audience"].copy()

    # Clean handles and networks
    clean_df["Username"] = clean_df["Username"].str.lstrip("@").str.strip()
    clean_df["Network"]  = clean_df["Network"].str.lower().str.strip()

    # Rename to match pipeline conventions
    clean_df = clean_df.rename(columns={"Username": "handle", "Network": "network"})

    # Drop duplicates on composite key
    before = len(clean_df)
    clean_df = clean_df.drop_duplicates(subset=["handle", "network"])
    after  = len(clean_df)

    if before - after > 0:
        logger.info(f"Dropped {before - after} duplicate (handle, network) pairs.")

    logger.info(f"Clean handles: {len(clean_df)} | Quarantined: {len(quarantine_df)}")
    return clean_df, quarantine_df


# -----------------------------------------------------------------
# 4. Quarantine writer
#    Saves Full Audience rows for manual review.
# -----------------------------------------------------------------
def write_quarantine(quarantine_df):
    """Write Full Audience rows to a separate CSV for manual review."""
    os.makedirs("output", exist_ok=True)
    quarantine_df.to_csv(QUARANTINE_FILE, index=False)
    logger.info(f"Quarantined {len(quarantine_df)} Full Audience rows → {QUARANTINE_FILE}")


# -----------------------------------------------------------------
# 5. Checkpoint system
#    Tracks which (handle, network) pairs have been successfully
#    loaded. On resume, already-completed pairs are skipped.
#
#    A pair is only written to the checkpoint AFTER a successful
#    load — not after extract or transform. If the load fails,
#    it will be retried on the next run.
# -----------------------------------------------------------------
def load_checkpoint():
    """
    Returns a set of (handle, network) tuples already completed.
    Returns empty set if no checkpoint exists yet.
    """
    if not os.path.exists(CHECKPOINT_FILE):
        logger.info("No checkpoint file found. Starting from the beginning.")
        return set()

    checkpoint = pd.read_csv(CHECKPOINT_FILE)
    completed  = set(zip(checkpoint["handle"], checkpoint["network"]))
    logger.info(f"Checkpoint loaded — {len(completed)} handles already completed.")
    return completed


def write_checkpoint(handle, network):
    """
    Appends a single completed (handle, network) pair to the
    checkpoint file immediately after a successful load.
    Appending row by row means a crash mid-run loses at most
    one record, not the entire checkpoint.
    """
    row = pd.DataFrame([{
        "handle":       handle,
        "network":      network,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }])

    row.to_csv(
        CHECKPOINT_FILE,
        mode="a",
        header=not os.path.exists(CHECKPOINT_FILE),
        index=False,
    )


# -----------------------------------------------------------------
# 6. Batch flusher
#    Writes a batch of accumulated results to CSV in one shot,
#    then checkpoints every handle in the batch.
#    Called every BATCH_SIZE handles and once at the end of the run.
# -----------------------------------------------------------------
def flush_batch(handle_rows, fact_frames, succeeded):
    """Bulk-load one batch to CSV, then write checkpoints for each handle."""
    load_handles(pd.DataFrame(handle_rows))

    if fact_frames:
        load_facts(pd.concat(fact_frames, ignore_index=True))

    for handle, network in succeeded:
        write_checkpoint(handle, network)

    logger.info(f"Flushed batch of {len(succeeded)} handles.")


# -----------------------------------------------------------------
# 7. Main backfill
# -----------------------------------------------------------------
def main():
    logger.info("=" * 60)
    logger.info("Backfill started.")
    logger.info("=" * 60)

    run_start = datetime.now(timezone.utc)

    # -- Load, clean, quarantine
    clean_df, quarantine_df = load_and_clean(BACKFILL_CSV)
    write_quarantine(quarantine_df)

    # -- Load checkpoint and filter out already-completed handles
    completed  = load_checkpoint()
    remaining  = clean_df[
        ~clean_df.apply(lambda r: (r["handle"], r["network"]) in completed, axis=1)
    ]

    logger.info(f"Handles remaining: {len(remaining)} of {len(clean_df)} total.")

    if remaining.empty:
        logger.info("All handles already completed. Nothing to do.")
        return

    # -- Fetch account summary once for the entire run
    summary = get_account_summary(days=config.STALENESS_DAYS)

    # -- Process each handle in batches of BATCH_SIZE
    #    Results accumulate in memory; CSV writes and checkpoints happen
    #    once per batch. Worst-case crash loss = one batch (100 handles).
    succeeded = []
    failed    = []

    batch_handle_rows = []
    batch_fact_frames = []
    batch_succeeded   = []

    for _, row in remaining.iterrows():
        handle  = row["handle"]
        network = row["network"]

        logger.info(f"Processing @{handle}/{network}...")

        try:
            raw = extract(handle, network, summary)

            if raw.get("status") != "Complete":
                logger.warning(f"Skipping @{handle}/{network} — status: {raw.get('status')}")
                failed.append((handle, network, raw.get("status")))
                continue

            handle_row = transform_handle(handle, network, raw)
            facts      = transform_facts(handle, network, handle_row["request_id"], raw)

            batch_handle_rows.append(handle_row)
            if not facts.empty:
                batch_fact_frames.append(facts)
            batch_succeeded.append((handle, network))

        except Exception as e:
            logger.error(f"Failed @{handle}/{network} — {e}", exc_info=True)
            failed.append((handle, network, str(e)))
            continue

        # -- Flush to CSV and checkpoint every BATCH_SIZE successes
        if len(batch_succeeded) >= BATCH_SIZE:
            flush_batch(batch_handle_rows, batch_fact_frames, batch_succeeded)
            succeeded.extend(batch_succeeded)
            batch_handle_rows = []
            batch_fact_frames = []
            batch_succeeded   = []

    # -- Flush any remaining handles that didn't fill a full batch
    if batch_succeeded:
        flush_batch(batch_handle_rows, batch_fact_frames, batch_succeeded)
        succeeded.extend(batch_succeeded)

    # -- Run summary
    run_end  = datetime.now(timezone.utc)
    duration = (run_end - run_start).seconds

    logger.info("=" * 60)
    logger.info(f"Backfill complete in {duration}s.")
    logger.info(f"  Succeeded: {len(succeeded)}")
    logger.info(f"  Failed:    {len(failed)}")

    if failed:
        logger.info("Failed handles:")
        for item in failed:
            logger.info(f"  @{item[0]}/{item[1]} — {item[2]}")

    logger.info("=" * 60)


if __name__ == "__main__":
    main()