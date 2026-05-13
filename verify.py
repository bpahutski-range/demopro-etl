import pandas as pd

taxonomy = pd.read_csv("output/dim_taxonomy.csv")
handles  = pd.read_csv("output/dim_handles.csv")
facts    = pd.read_csv("output/fact_analysis.csv")

print("TAXONOMY")
print(f"  Rows:     {len(taxonomy)}")
print(f"  Sections: {taxonomy['section_name'].nunique()}")
print(f"  Sections: {taxonomy['section_name'].unique().tolist()}")

print("\nHANDLES")
print(f"  Rows: {len(handles)}")
print(handles[["handle", "network", "analysis_date"]].to_string())

print("\nFACTS")
print(f"  Rows:     {len(facts)}")
print(f"  Handles:  {facts['handle'].nunique()}")
print(f"  Sections: {facts['section_name'].nunique()}")

# Check for nulls in key columns
print("\nNULL CHECKS")
print(f"  facts — null codes:    {facts['code'].isna().sum()}")
print(f"  facts — null pct:      {facts['pct'].isna().sum()}")
print(f"  handles — null dates:  {handles['analysis_date'].isna().sum()}")


print('------------------------------------')
print('------------------------------------')
print('------------------------------------')
print('------------------------------------')

handles = pd.read_csv("output/dim_handles.csv")
facts   = pd.read_csv("output/fact_analysis.csv")

print(f"dim_handles rows: {len(handles)}")
print(f"fact_analysis rows: {len(facts)}")

# Check for duplicates on the composite key
handle_dupes = handles.duplicated(subset=["handle", "network"]).sum()
fact_dupes   = facts.duplicated(subset=["handle", "network", "code"]).sum()

print(f"dim_handles duplicates: {handle_dupes}")
print(f"fact_analysis duplicates: {fact_dupes}")