import json
from api import extract, get_account_summary, get_taxonomy
import config

# Fetch taxonomy and pull out Family status section
raw_codes = get_taxonomy()
family_status = [
    section for section in raw_codes.get("sections_list", [])
    if section.get("section_name") == "Family status"
]
print("=== get_codes: Family status ===")
print(json.dumps(family_status, indent=2))

# Fetch a known analysis and pull out Marital/Parental status sections
summary = get_account_summary(days=config.STALENESS_DAYS)
raw     = extract("ericstonestreet", "instagram", summary)

family_sections = [
    section for section in raw.get("analysis", {}).get("analysis_data", [])
    if section.get("section_name") in ("Marital status", "Parental status")
]
print("\n=== get_aggregate_analysis: Marital status + Parental status ===")
print(json.dumps(family_sections, indent=2))