import pandas as pd
from api import extract, get_account_summary
from transform import transform_handle, transform_facts
import config

# -----------------------------------------------------------------
# Load and clean the backfill CSV
# Filter out Full Audience rows, strip @ symbols, normalize casing
# -----------------------------------------------------------------
df = pd.read_csv("/Users/billpahutski/Downloads/demopro_handles.csv")

df = df[df["Network"] != "Full Audience"].copy()
df["Username"] = df["Username"].str.lstrip("@").str.strip()
df["Network"]  = df["Network"].str.lower().str.strip()

# -----------------------------------------------------------------
# Sample 5 handles per network
# -----------------------------------------------------------------
test_handles = pd.concat([
    group.sample(min(5, len(group)), random_state=42)
    for _, group in df.groupby("Network")
]).reset_index(drop=True)
print(f'HERE: {test_handles.columns}')

print("Test handles selected:")
print(test_handles.to_string())
print(f"\nTotal: {len(test_handles)} handles across {test_handles['Network'].nunique()} platforms\n")

# -----------------------------------------------------------------
# Run the pipeline against each test handle
# -----------------------------------------------------------------
summary   = get_account_summary(days=config.STALENESS_DAYS)
succeeded = []
failed    = []

for _, row in test_handles.iterrows():
    handle  = row["Username"]
    network = row["Network"]
    print(f"Testing @{handle}/{network}...")
    try:
        raw = extract(handle, network, summary)
        if raw.get("status") != "Complete":
            print(f"  SKIP — status: {raw.get('status')}")
            failed.append((handle, network, raw.get("status")))
            continue
        handle_row = transform_handle(handle, network, raw)
        facts      = transform_facts(handle, network, handle_row["request_id"], raw)
        print(f"  OK — {len(facts)} fact rows")
        succeeded.append((handle, network))
    except Exception as e:
        print(f"  FAIL — {e}")
        failed.append((handle, network, str(e)))

# -----------------------------------------------------------------
# Summary
# -----------------------------------------------------------------
print("\n" + "=" * 50)
print(f"Succeeded: {len(succeeded)}")
print(f"Failed:    {len(failed)}")

if failed:
    print("\nFailed handles:")
    for item in failed:
        print(f"  @{item[0]}/{item[1]} — {item[2]}")