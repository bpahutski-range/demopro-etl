import logging
import os
import pandas as pd

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------
# The destination is configured here in one place.
# When Snowflake is ready, this is where the connection gets
# established and the functions below get rewritten to use it.
# For now, we write to local CSV files.
# -----------------------------------------------------------------
OUTPUT_DIR = "output"


def _ensure_output_dir():
    """Create the output directory if it doesn't exist."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def _upsert_csv(df, filename, key_cols):
    """
    Simulate an upsert against a local CSV file.

    key_cols: list of column names that together form the unique key.
              e.g. ["handle", "network"] for dim_handles
                   ["handle", "network", "code"] for fact_analysis

    Logic:
        1. If the file doesn't exist, write df as-is (first run).
        2. If it does exist, load it, drop any rows whose key matches
           an incoming row, then append the incoming rows.
           This replicates Snowflake MERGE behavior locally.
    """
    _ensure_output_dir()
    filepath = os.path.join(OUTPUT_DIR, filename)

    if not os.path.exists(filepath):
        df.to_csv(filepath, index=False)
        logger.info(f"Created {filename} with {len(df)} rows.")
        return

    existing = pd.read_csv(filepath)

    # Build a temporary merge key by concatenating the key columns
    # into a single string for comparison. This avoids needing SQL.
    def make_key(frame):
        return frame[key_cols].astype(str).agg("|".join, axis=1)

    incoming_keys = set(make_key(df))
    mask          = ~make_key(existing).isin(incoming_keys)
    retained      = existing[mask]
    updated       = pd.concat([retained, df], ignore_index=True)

    updated.to_csv(filepath, index=False)
    logger.info(
        f"Upserted {filename}: "
        f"{len(existing) - len(retained)} rows replaced, "
        f"{len(df)} rows written, "
        f"{len(updated)} total rows."
    )


# -----------------------------------------------------------------
# Public functions — these are what main.py calls.
# Each one knows its own key and filename.
# When migrating to Snowflake, rewrite these three functions.
# Everything above this line stays or gets deleted entirely.
# -----------------------------------------------------------------

def load_taxonomy(df):
    """Write dim_taxonomy. Keyed on (code, taxonomy_version)."""
    logger.info("Loading dim_taxonomy...")
    _upsert_csv(df, "dim_taxonomy.csv", key_cols=["code", "taxonomy_version"])


def load_handles(df):
    """Write dim_handles. Keyed on (handle, network)."""
    logger.info("Loading dim_handles...")
    _upsert_csv(df, "dim_handles.csv", key_cols=["handle", "network"])


def load_facts(df):
    """Write fact_analysis. Keyed on (handle, network, code)."""
    logger.info("Loading fact_analysis...")
    _upsert_csv(df, "fact_analysis.csv", key_cols=["handle", "network", "code"])