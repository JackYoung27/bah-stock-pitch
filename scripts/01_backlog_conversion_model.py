from __future__ import annotations

import numpy as np
import pandas as pd

from _paths import CHART_DIR, PROCESSED_DIR, RAW_DIR, ensure_dirs


RAW_CSV = RAW_DIR / "bah_quarterly_financials.csv"
RESULTS_CSV = PROCESSED_DIR / "backlog_conversion_results.csv"
CHART_PATH = CHART_DIR / "backlog_conversion_chart.png"
REQUIRED = ["quarter", "date", "revenue_m", "backlog_m", "book_to_bill", "funded_backlog_m"]

ensure_dirs(PROCESSED_DIR, CHART_DIR)


def load_data() -> pd.DataFrame:
    df = pd.read_csv(RAW_CSV)
    missing = [column for column in REQUIRED if column not in df.columns]
    if missing:
        raise ValueError(", ".join(missing))
    for column in REQUIRED[2:]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)


def add_forward_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["next_1q_rev_m"] = df["revenue_m"].shift(-1)
    df["next_4q_rev_m"] = sum(df["revenue_m"].shift(-step) for step in range(1, 5))
    df["conv_1q_fwd"] = df["next_1q_rev_m"] / df["backlog_m"]
    df["conv_4q_fwd"] = df["next_4q_rev_m"] / df["backlog_m"]
    df["conv_1q_funded_fwd"] = df["next_1q_rev_m"] / df["funded_backlog_m"]
    return df


def print_summary(df: pd.DataFrame) -> None:
    valid_1q = df["conv_1q_fwd"].dropna()
    valid_4q = df["conv_4q_fwd"].dropna()
    latest = df.iloc[-1]
    print(f"rows: {len(df)}")
    if len(valid_1q):
        print(f"conv_1q_mean: {valid_1q.mean():.3f}")
    if len(valid_4q):
        print(f"conv_4q_mean: {valid_4q.mean():.3f}")
    print(f"latest_backlog_m: {latest['backlog_m']:,.0f}")
    print(f"latest_funded_backlog_m: {latest['funded_backlog_m']:,.0f}")
    if len(valid_4q):
        slope, intercept = np.polyfit(df.dropna(subset=['next_4q_rev_m'])["backlog_m"], df.dropna(subset=['next_4q_rev_m'])["next_4q_rev_m"], 1)
        print(f"latest_regression_next_4q_rev_m: {slope * latest['backlog_m'] + intercept:,.0f}")


def plot(df: pd.DataFrame) -> None:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    axes[0, 0].bar(df["date"], df["backlog_m"], width=60, alpha=0.4, color="steelblue", label="Backlog")
    axes[0, 0].plot(df["date"], df["revenue_m"], color="darkred", linewidth=2, label="Revenue")
    axes[0, 0].legend()
    axes[0, 0].grid(alpha=0.3)

    axes[0, 1].plot(df["date"], df["conv_1q_fwd"], color="teal", linewidth=1.5, label="1Q")
    axes[0, 1].plot(df["date"], df["conv_4q_fwd"], color="darkorange", linewidth=2, label="4Q")
    if df["conv_4q_fwd"].dropna().size:
        axes[0, 1].axhline(df["conv_4q_fwd"].dropna().mean(), color="darkorange", linestyle="--", alpha=0.5)
    axes[0, 1].legend()
    axes[0, 1].grid(alpha=0.3)

    valid_4q = df.dropna(subset=["next_4q_rev_m"]).copy()
    axes[1, 0].scatter(valid_4q["backlog_m"], valid_4q["next_4q_rev_m"], color="steelblue", alpha=0.8)
    if len(valid_4q) >= 2:
        slope, intercept = np.polyfit(valid_4q["backlog_m"], valid_4q["next_4q_rev_m"], 1)
        x_vals = np.linspace(valid_4q["backlog_m"].min(), valid_4q["backlog_m"].max(), 100)
        axes[1, 0].plot(x_vals, slope * x_vals + intercept, color="black", linestyle="--", linewidth=1.5)
    axes[1, 0].grid(alpha=0.3)

    valid_1q = df.dropna(subset=["next_1q_rev_m", "funded_backlog_m"]).copy()
    axes[1, 1].scatter(valid_1q["funded_backlog_m"], valid_1q["next_1q_rev_m"], color="darkgreen", alpha=0.8)
    if len(valid_1q) >= 2:
        slope, intercept = np.polyfit(valid_1q["funded_backlog_m"], valid_1q["next_1q_rev_m"], 1)
        x_vals = np.linspace(valid_1q["funded_backlog_m"].min(), valid_1q["funded_backlog_m"].max(), 100)
        axes[1, 1].plot(x_vals, slope * x_vals + intercept, color="black", linestyle="--", linewidth=1.5)
    axes[1, 1].grid(alpha=0.3)

    plt.tight_layout()
    plt.savefig(CHART_PATH, dpi=150)
    plt.close()


def main() -> None:
    df = add_forward_metrics(load_data())
    df.to_csv(RESULTS_CSV, index=False)
    plot(df)
    print_summary(df)


if __name__ == "__main__":
    main()
