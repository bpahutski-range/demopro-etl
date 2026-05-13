import logging
import pandas as pd
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def transform_taxonomy(raw_codes):
    """
    Transforms raw taxonomy JSON into a flat DataFrame.
    Run once per pipeline run, or only when taxonomy_version changes.
    Output: dim_taxonomy
    """
    rows = []
    for section in raw_codes.get("sections_list", []):
        for item in section.get("section_content", []):
            rows.append({
                "code":             item.get("code"),
                "label":            item.get("name"),
                "subcategory":      item.get("subcategory"),
                "section_name":     section.get("section_name"),
                "section_count":    section.get("section_count"),
                "taxonomy_version": raw_codes.get("taxonomy_version"),
                "from":             item.get("from"),
                "to":               item.get("to"),
                "mean":             item.get("mean"),
                "average":          item.get("average"),
                "median":           item.get("median"),
                "code_status":      item.get("code_status"),
            })

    df = pd.DataFrame(rows)
    logger.info(f"transform_taxonomy: {len(df)} codes across {df['section_name'].nunique()} sections.")
    return df


def transform_handle(handle, network, raw_analysis):
    """
    Transforms raw analysis JSON into a single handle record.
    Output: one row for dim_handles, keyed on (handle, network).
    """
    analysis = raw_analysis.get("analysis", {})
    record = {
        "handle":              handle.lower().strip(),
        "network":             network.lower().strip(),
        "request_id":          raw_analysis.get("request_id"),
        "followers_submitted": analysis.get("number_submitted"),
        "followers_processed": analysis.get("number_processed"),
        "analysis_date":       analysis.get("analysis_date"),
        "taxonomy_version":    analysis.get("taxonomy_version"),
        "pulled_at":           datetime.now(timezone.utc).isoformat(),
    }
    logger.info(f"transform_handle: @{handle}/{network} — request_id={record['request_id']}, analysis_date={record['analysis_date']}.")
    return record


def transform_facts(handle, network, request_id, raw_analysis):
    """
    Flattens the two-level analysis_data structure into a long-format DataFrame.
    Each row is one demographic code for one handle/network pair.
    Output: rows for fact_analysis, keyed on (handle, network, code).
    """
    rows = []
    for section in raw_analysis.get("analysis", {}).get("analysis_data", []):
        section_name = section.get("section_name")
        for row in section.get("section_rows", []):
            rows.append({
                "handle":         handle.lower().strip(),
                "network":        network.lower().strip(),
                "request_id":     request_id,
                "section_name":   section_name,
                "code":           row.get("code"),
                "pct":            row.get("pct"),
                "estimated_size": row.get("estimated_size"),
                "pulled_at":      datetime.now(timezone.utc).isoformat(),
            })

    df = pd.DataFrame(rows)

    if df.empty:
        logger.warning(f"transform_facts: @{handle}/{network} — no fact rows returned (analysis_data missing or empty).")
        return df

    logger.info(f"transform_facts: @{handle}/{network} — {len(df)} fact rows across {df['section_name'].nunique()} sections.")
    return df