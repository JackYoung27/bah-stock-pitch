import requests
import pandas as pd
import time

# ── Company names + known subsidiaries/acquisitions ──────────────────────────
COMPANY_ENTITIES = {

    "Booz Allen Hamilton": [
        "Booz Allen Hamilton",
        "Booz Allen Hamilton Inc",
        "Booz Allen Cyber Solutions",
        "Aquilent",            # acquired 2016 — digital services
        "Epidemico",           # acquired 2014 — health data analytics
        "Morphick",            # acquired 2017 — cybersecurity
        "SDI Technology",      # acquired 2018 — defense IT
        "Liberty IT Solutions",# acquired 2021 — federal health IT
        "EverWatch",           # acquired 2022 — signals intelligence
        "PAR Government",      # acquired 2024 — cloud/gov software
        "Defy Security",       # acquired 2025 — commercial cyber
    ],

    "Leidos": [
        "Leidos",
        "Leidos Inc",
        "1901 Group",          # acquired 2021 — managed IT services
        "Dynetics",            # acquired 2020 — defense R&D
        "Gibbs & Cox",         # acquired 2021 — naval architecture
        "Kudu Dynamics",       # acquired 2025 — AI/cyber
        "Leidos Biomedical Research",  # formerly SAIC-Frederick
    ],

    "SAIC": [
        "Science Applications International Corporation",
        "SAIC",
        "Engility",            # acquired 2019 — defense services
        "Unisys Federal",      # acquired 2020 — federal IT
        "Halfaker and Associates",  # acquired 2021 — veteran-led tech
        "Koverse",             # acquired 2021 — big data analytics
        "Scitor",              # acquired 2015 — intelligence services
        "TASC",                # legacy name
        "SilverEdge",          # acquired 2025 — national security tech
    ],

    "CACI": [
        "CACI International",
        "CACI Inc",
        "CACI Federal",
        "CACI Technologies",
        "CACI-WGI",
        "CACI-ISS",
        "CACI-NSR",
        "Six3 Systems",        # acquired 2013 — intelligence/cyber
        "Bluestone",           # acquired 2014 — IT services
        "Apogee",              # acquired 2015 — intel/defense analytics
        "Syntex Management Systems",
        "LGS Innovations",     # acquired 2018 — wireless/signals
        "SI International",    # earlier acquisition — IT services
    ],
}

# ── Search configuration ──────────────────────────────────────────────────────
AI_KEYWORDS = [
    "artificial intelligence",
    "machine learning",
    "large language model",
    "generative AI",
    "deep learning",
    "natural language processing",
    "computer vision",
    "autonomous systems",
    "predictive analytics",
    "cognitive computing",
]

# NAICS codes for IT/software services — catches contracts not labeled "AI"
AI_NAICS = ["541511", "541512", "541715", "541519"]

START_DATE = "2019-01-01"
END_DATE   = "2025-12-31"
MAX_PAGES  = 10  # 100 results/page → up to 1,000 per entity name

API_URL = "https://api.usaspending.gov/api/v2/search/spending_by_award/"

FIELDS = [
    "Award ID", "Recipient Name", "Award Amount", "Description",
    "Awarding Agency", "Awarding Sub Agency",
    "Period of Performance Start Date",
    "Period of Performance Current End Date",
    "NAICS Code", "NAICS Description", "PSC Code",
]


# ── Fetch by keyword search ───────────────────────────────────────────────────
def fetch_by_keyword(entity_name, keywords, start_date, end_date, max_pages):
    results, page = [], 1
    while True:
        payload = {
            "filters": {
                "recipient_search_text": [entity_name],
                "award_type_codes": ["A", "B", "C", "D"],
                "keywords": keywords,
                "time_period": [{"start_date": start_date, "end_date": end_date}],
            },
            "fields": FIELDS,
            "page": page, "limit": 100,
            "sort": "Award Amount", "order": "desc",
        }
        try:
            r = requests.post(API_URL, json=payload, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"  Warning ({entity_name}, page {page}): {e}")
            break
        batch = data.get("results", [])
        if not batch:
            break
        results.extend(batch)
        if not data.get("page_metadata", {}).get("hasNext", False) or page >= max_pages:
            break
        page += 1
        time.sleep(0.4)
    return results


# ── Fetch by NAICS code ───────────────────────────────────────────────────────
def fetch_by_naics(entity_name, naics_codes, start_date, end_date, max_pages):
    results, page = [], 1
    while True:
        payload = {
            "filters": {
                "recipient_search_text": [entity_name],
                "award_type_codes": ["A", "B", "C", "D"],
                "naics_codes": {"require": naics_codes},
                "time_period": [{"start_date": start_date, "end_date": end_date}],
            },
            "fields": FIELDS,
            "page": page, "limit": 100,
            "sort": "Award Amount", "order": "desc",
        }
        try:
            r = requests.post(API_URL, json=payload, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"  Warning ({entity_name}, page {page}): {e}")
            break
        batch = data.get("results", [])
        if not batch:
            break
        results.extend(batch)
        if not data.get("page_metadata", {}).get("hasNext", False) or page >= max_pages:
            break
        page += 1
        time.sleep(0.4)
    return results


# ── Run all companies (keyword search) ───────────────────────────────────────
keyword_dfs = {}

for company, entity_names in COMPANY_ENTITIES.items():
    print(f"\n{company} ({len(entity_names)} entity names)")
    company_results = []

    for name in entity_names:
        raw = fetch_by_keyword(name, AI_KEYWORDS, START_DATE, END_DATE, MAX_PAGES)
        if raw:
            for r in raw:
                r["_searched_as"] = name  # track which name matched
            company_results.extend(raw)
            print(f"  {name:45s} -> {len(raw):>4} records")
        else:
            print(f"  {name:45s} -> no results")
        time.sleep(0.3)

    if company_results:
        df = pd.DataFrame(company_results)
        df["Award Amount"] = pd.to_numeric(df["Award Amount"], errors="coerce")
        df["Company"] = company
        before = len(df)
        # Deduplicate by Award ID, keep highest-value version
        df = df.sort_values("Award Amount", ascending=False).drop_duplicates(subset="Award ID")
        print(f"  -> {len(df)} unique contracts | ${df['Award Amount'].sum():,.0f} total | {before - len(df)} dupes removed")
        keyword_dfs[company] = df


# ── Run all companies (NAICS search) ─────────────────────────────────────────
naics_dfs = {}

for company, entity_names in COMPANY_ENTITIES.items():
    print(f"\n{company} — NAICS search")
    company_results = []

    for name in entity_names:
        raw = fetch_by_naics(name, AI_NAICS, START_DATE, END_DATE, MAX_PAGES)
        if raw:
            for r in raw:
                r["_searched_as"] = name
            company_results.extend(raw)
            print(f"  {name:45s} -> {len(raw):>4} records")
        else:
            print(f"  {name:45s} -> no results")
        time.sleep(0.3)

    if company_results:
        df = pd.DataFrame(company_results)
        df["Award Amount"] = pd.to_numeric(df["Award Amount"], errors="coerce")
        df["Company"] = company
        df = df.sort_values("Award Amount", ascending=False).drop_duplicates(subset="Award ID")
        print(f"  -> {len(df)} unique contracts | ${df['Award Amount'].sum():,.0f} total")
        naics_dfs[company] = df


# ── Merge keyword + NAICS, deduplicate, save ──────────────────────────────────
all_merged = {}

for company in COMPANY_ENTITIES.keys():
    frames = []
    if company in keyword_dfs:
        frames.append(keyword_dfs[company])
    if company in naics_dfs:
        frames.append(naics_dfs[company])

    if frames:
        merged = pd.concat(frames, ignore_index=True)
        merged = merged.sort_values("Award Amount", ascending=False).drop_duplicates(subset="Award ID")
        all_merged[company] = merged

        fname = f"{company.replace(' ', '_')}_AI_contracts_final.csv"
        merged.to_csv(fname, index=False)
        print(f"Saved: {fname}  ({len(merged)} contracts, ${merged['Award Amount'].sum():,.0f})")

# Combined file — all four companies
if all_merged:
    grand = pd.concat(all_merged.values(), ignore_index=True)
    grand.to_csv("ALL_contractors_AI_contracts_final.csv", index=False)
    print(f"\nSaved: ALL_contractors_AI_contracts_final.csv  ({len(grand)} total contracts)")


# ── Summary table ─────────────────────────────────────────────────────────────
rows = []
for company, df in all_merged.items():
    rows.append({
        "Company":          company,
        "# Contracts":      len(df),
        "Total Value":      df["Award Amount"].sum(),
        "Largest Contract": df["Award Amount"].max(),
        "Avg Contract":     df["Award Amount"].mean(),
    })

summary = pd.DataFrame(rows).set_index("Company").sort_values("Total Value", ascending=False)
print("\nSummary — AI-Related Contracts 2019–2025 (incl. subsidiaries)")
print(summary.to_string())
