from __future__ import annotations

import json
from datetime import datetime, timedelta
from urllib.request import Request, urlopen

from _paths import CHART_DIR, PROCESSED_DIR, ensure_dirs


API_URL = "https://api.usaspending.gov/api/v2/search/spending_by_transaction/"
FIELDS = [
    "Award ID",
    "Recipient Name",
    "Action Date",
    "Transaction Amount",
    "Awarding Agency",
    "Awarding Sub Agency",
    "Award Type",
    "Transaction Description",
]
PAGE_LIMIT = 100
MAX_PAGES = 250
DOD = "Department of Defense"

ensure_dirs(PROCESSED_DIR, CHART_DIR)


def fetch_actions(start_date: str, end_date: str) -> list[dict]:
    payload = {
        "filters": {
            "recipient_search_text": ["BOOZ ALLEN HAMILTON"],
            "time_period": [{"start_date": start_date, "end_date": end_date}],
            "award_type_codes": ["A", "B", "C", "D"],
        },
        "fields": FIELDS,
        "page": 1,
        "limit": PAGE_LIMIT,
        "sort": "Action Date",
        "order": "desc",
    }
    results: list[dict] = []
    page = 1
    while page <= MAX_PAGES:
        payload["page"] = page
        request = Request(API_URL, data=json.dumps(payload).encode("utf-8"), headers={"Content-Type": "application/json"})
        with urlopen(request, timeout=30) as response:
            body = json.loads(response.read().decode())
        rows = body.get("results", [])
        results.extend(rows)
        meta = body.get("page_metadata", {})
        if not meta.get("hasNext", False):
            break
        page = meta.get("next") or page + 1
    return results


def analyze(actions: list[dict], start_date: str, end_date: str) -> dict | None:
    import pandas as pd

    if not actions:
        return None
    df = pd.DataFrame(actions)
    df["transaction_amount"] = pd.to_numeric(df.get("Transaction Amount", 0), errors="coerce").fillna(0)
    df["gross_transaction_amount"] = df["transaction_amount"].clip(lower=0)
    df["action_date"] = pd.to_datetime(df.get("Action Date"), errors="coerce")
    df["agency"] = df.get("Awarding Agency", "Unknown").fillna("Unknown")
    df["award_id"] = df.get("Award ID", "Unknown").fillna("Unknown")
    df["sector"] = df["agency"].apply(lambda value: "DoD" if value == DOD else "Civilian")
    start_ts, end_ts = pd.Timestamp(start_date), pd.Timestamp(end_date)
    df = df.dropna(subset=["action_date"])
    df = df[(df["action_date"] >= start_ts) & (df["action_date"] <= end_ts)].copy()
    if df.empty:
        return None

    df["month"] = df["action_date"].dt.to_period("M")
    monthly = df.groupby("month").agg(
        gross_obligated=("gross_transaction_amount", "sum"),
        net_obligated=("transaction_amount", "sum"),
        num_actions=("award_id", "size"),
        unique_awards=("award_id", "nunique"),
    ).reset_index().sort_values("month").reset_index(drop=True)
    agency = (
        df.groupby("agency")
        .agg(
            gross_obligated=("gross_transaction_amount", "sum"),
            net_obligated=("transaction_amount", "sum"),
            num_actions=("award_id", "size"),
            unique_awards=("award_id", "nunique"),
        )
        .sort_values("gross_obligated", ascending=False)
        .head(10)
    )
    sector = (
        df.groupby("sector")
        .agg(
            gross_obligated=("gross_transaction_amount", "sum"),
            net_obligated=("transaction_amount", "sum"),
            num_actions=("award_id", "size"),
            unique_awards=("award_id", "nunique"),
        )
        .reindex(["DoD", "Civilian"], fill_value=0)
    )

    monthly.to_csv(PROCESSED_DIR / "bah_monthly_contract_actions.csv", index=False)
    agency.to_csv(PROCESSED_DIR / "bah_agency_breakdown.csv")
    sector.to_csv(PROCESSED_DIR / "bah_sector_breakdown.csv")
    df.to_csv(PROCESSED_DIR / "bah_contract_actions_raw.csv", index=False)
    return {"monthly": monthly, "sector": sector, "rows": len(df)}


def plot(results: dict | None) -> None:
    if results is None:
        return
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return

    monthly = results["monthly"]
    sector = results["sector"]
    month_labels = monthly["month"].astype(str)
    fig, axes = plt.subplots(3, 1, figsize=(12, 10), gridspec_kw={"height_ratios": [2, 2, 1.2]})
    axes[0].bar(range(len(monthly)), monthly["gross_obligated"] / 1e6, color="steelblue", alpha=0.8)
    axes[1].bar(range(len(monthly)), monthly["num_actions"], color="darkorange", alpha=0.8)
    axes[2].barh(list(sector.index), (sector["gross_obligated"] / 1e6).tolist(), color=["#234f7d", "#6d9f71"])
    for axis in axes[:2]:
        axis.set_xticks(range(len(monthly)))
        axis.set_xticklabels(month_labels, rotation=45, ha="right", fontsize=8)
        axis.grid(alpha=0.3, axis="y")
    axes[2].grid(alpha=0.3, axis="x")
    plt.tight_layout()
    plt.savefig(CHART_DIR / "contract_award_tracker.png", dpi=150)
    plt.close()


def main() -> None:
    end = datetime.now()
    start = end - timedelta(days=365)
    start_date = start.strftime("%Y-%m-%d")
    end_date = end.strftime("%Y-%m-%d")
    results = analyze(fetch_actions(start_date, end_date), start_date, end_date)
    plot(results)
    if results is not None:
        print(f"rows: {results['rows']}")


if __name__ == "__main__":
    main()
