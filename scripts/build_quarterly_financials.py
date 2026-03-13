from __future__ import annotations

import html
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd

from _paths import PROCESSED_DIR, RAW_DIR, ensure_dirs

CIK = "1443646"
SEC_SUBMISSIONS_URL = f"https://data.sec.gov/submissions/CIK{CIK.zfill(10)}.json"
SEC_COMPANYFACTS_URL = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{CIK.zfill(10)}.json"
USER_AGENT = "Codex research assistant jack@example.com"

RAW_CSV = RAW_DIR / "bah_quarterly_financials.csv"
SOURCE_CSV = PROCESSED_DIR / "bah_quarterly_financial_sources.csv"
START_DATE = "2015-03-31"


@dataclass
class QuarterlyReport:
    filing_date: str
    form: str
    accession_number: str
    primary_document: str
    period_end: str
    quarter: str
    report_url: str


@dataclass
class EarningsSource:
    filing_date: str
    accession_number: str
    filing_url: str
    exhibit_doc: Optional[str]
    exhibit_url: Optional[str]
    book_to_bill: Optional[float]


def fetch_text(url: str) -> str:
    for attempt in range(4):
        req = Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urlopen(req, timeout=30) as resp:
                return resp.read().decode("utf-8", errors="ignore")
        except HTTPError as exc:
            if exc.code not in {429, 500, 502, 503, 504} or attempt == 3:
                raise
        except URLError:
            if attempt == 3:
                raise
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"Failed to fetch {url}")


def fetch_json(url: str) -> dict:
    return json.loads(fetch_text(url))


def collapse_html(raw_html: str) -> str:
    stripped = re.sub(r"<[^>]+>", " ", raw_html)
    unescaped = html.unescape(stripped).replace("\xa0", " ")
    return " ".join(unescaped.split())


def fiscal_quarter_label(period_end: str) -> str:
    dt = datetime.strptime(period_end, "%Y-%m-%d")
    month_to_quarter = {3: ("Q4", 0), 6: ("Q1", 1), 9: ("Q2", 1), 12: ("Q3", 1)}
    if dt.month not in month_to_quarter:
        raise ValueError(f"Unexpected quarter-end month in {period_end}")
    quarter, year_offset = month_to_quarter[dt.month]
    return f"FY{dt.year + year_offset} {quarter}"


def archive_url(accession_number: str, document: str) -> str:
    accession_nodash = accession_number.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{CIK}/{accession_nodash}/{document}"


def load_quarterly_reports() -> list[QuarterlyReport]:
    submissions = fetch_json(SEC_SUBMISSIONS_URL)
    recent = submissions["filings"]["recent"]
    reports: list[QuarterlyReport] = []

    for filing_date, form, accession_number, primary_document in zip(
        recent["filingDate"],
        recent["form"],
        recent["accessionNumber"],
        recent["primaryDocument"],
    ):
        if form not in {"10-Q", "10-K"} or filing_date < "2015-01-01":
            continue

        report_url = archive_url(accession_number, primary_document)
        period_end_match = re.search(r"bah-(\d{8})", primary_document)
        if period_end_match:
            period_end = datetime.strptime(period_end_match.group(1), "%Y%m%d").strftime("%Y-%m-%d")
        else:
            report_html = fetch_text(report_url)
            clean = collapse_html(report_html)
            period_end_match = None
            for pattern in [
                r"Document Period End Date[: ]+([A-Za-z]+ \d{1,2}, \d{4})",
                r"For the quarterly period ended ([A-Za-z]+ \d{1,2}, \d{4})",
                r"For the fiscal year ended ([A-Za-z]+ \d{1,2}, \d{4})",
            ]:
                period_end_match = re.search(pattern, clean, re.IGNORECASE)
                if period_end_match:
                    break
            if not period_end_match:
                raise ValueError(f"Could not infer period end for {report_url}")
            period_end = datetime.strptime(period_end_match.group(1), "%B %d, %Y").strftime("%Y-%m-%d")

        if period_end < START_DATE:
            continue

        reports.append(
            QuarterlyReport(
                filing_date=filing_date,
                form=form,
                accession_number=accession_number,
                primary_document=primary_document,
                period_end=period_end,
                quarter=fiscal_quarter_label(period_end),
                report_url=report_url,
            )
        )

    reports.sort(key=lambda r: r.period_end)
    return reports


def build_revenue_lookup() -> dict[str, float]:
    companyfacts = fetch_json(SEC_COMPANYFACTS_URL)
    revenue_lookup: dict[str, float] = {}

    for fact_name in [
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "SalesRevenueServicesNet",
    ]:
        revenue_facts = (
            companyfacts["facts"]["us-gaap"]
            .get(fact_name, {})
            .get("units", {})
            .get("USD", [])
        )
        if not revenue_facts:
            continue

        df = pd.DataFrame(revenue_facts)
        df = df[df["form"].isin(["10-Q", "10-K"])].copy()
        df["start"] = pd.to_datetime(df["start"])
        df["end"] = pd.to_datetime(df["end"])
        df["filed"] = pd.to_datetime(df["filed"])
        df["days"] = (df["end"] - df["start"]).dt.days + 1

        discrete_quarters = (
            df[(df["days"] >= 80) & (df["days"] <= 100)]
            .sort_values(["end", "filed"])
            .drop_duplicates(subset=["end"], keep="last")
        )

        annual = (
            df[(df["form"] == "10-K") & (df["days"] >= 360) & (df["days"] <= 370)]
            .sort_values(["end", "filed"])
            .drop_duplicates(subset=["end"], keep="last")
            .set_index("end")["val"]
        )

        q3_ytd = (
            df[(df["form"] == "10-Q") & (df["fp"] == "Q3") & (df["days"] >= 260) & (df["days"] <= 280)]
            .sort_values(["end", "filed"])
            .drop_duplicates(subset=["end"], keep="last")
            .set_index("end")["val"]
        )

        for _, row in discrete_quarters.iterrows():
            end_key = row["end"].strftime("%Y-%m-%d")
            revenue_lookup.setdefault(end_key, float(row["val"]) / 1_000_000)

        for annual_end, annual_value in annual.items():
            if annual_end.strftime("%m-%d") != "03-31":
                continue
            q3_end = annual_end.replace(month=12, day=31, year=annual_end.year - 1)
            if q3_end not in q3_ytd:
                continue
            q4_revenue = float(annual_value) - float(q3_ytd[q3_end])
            revenue_lookup.setdefault(annual_end.strftime("%Y-%m-%d"), q4_revenue / 1_000_000)

    return revenue_lookup


def parse_backlog(report_html: str) -> tuple[float, float, float, float]:
    clean = collapse_html(report_html)
    patterns = [
        re.compile(
            r"Funded(?:\s+Backlog)?\s*\$?\s*([0-9,]+)\s*\$?\s*([0-9,]+)\s*"
            r"Unfunded(?:\s*\(\d+\))?\s*([0-9,]+)\s*([0-9,]+)\s*"
            r"Priced options\s*([0-9,]+)\s*([0-9,]+)\s*"
            r"Total backlog(?:\s*\(\d+\))?\s*\$?\s*([0-9,]+)\s*\$?\s*([0-9,]+)",
            re.IGNORECASE,
        ),
        re.compile(
            r"Funded\s*\$?\s*([0-9,]+)\s*\$?\s*([0-9,]+)\s*"
            r"Unfunded(?:\s*\(\d+\))?\s*([0-9,]+)\s*([0-9,]+)\s*"
            r"Priced options\s*([0-9,]+)\s*([0-9,]+)\s*"
            r"Total backlog\s*\$?\s*([0-9,]+)\s*\$?\s*([0-9,]+)",
            re.IGNORECASE,
        ),
    ]
    for pattern in patterns:
        match = pattern.search(clean)
        if match:
            funded = float(match.group(1).replace(",", ""))
            unfunded = float(match.group(3).replace(",", ""))
            priced_options = float(match.group(5).replace(",", ""))
            total = float(match.group(7).replace(",", ""))
            return funded, unfunded, priced_options, total
    raise ValueError("Could not parse backlog table from filing.")


def filing_index_url(accession_number: str) -> str:
    accession_nodash = accession_number.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{CIK}/{accession_nodash}/{accession_number}-index.htm"


def parse_exhibit_99_1_doc(index_html: str) -> Optional[str]:
    clean = collapse_html(index_html)
    for pattern in [
        r"EXHIBIT\s+99\.1\s+([A-Za-z0-9_.-]+\.(?:htm|html|pdf))\s+EX-99\.1",
        r"EX-99\.1\s+([A-Za-z0-9_.-]+\.(?:htm|html|pdf))\s+EX-99\.1",
    ]:
        match = re.search(pattern, clean, re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def parse_book_to_bill(doc_html: str) -> Optional[float]:
    clean = collapse_html(doc_html)
    patterns = [
        r"Book\s*-?\s*to\s*-?\s*Bill\s*\*?\s*([0-9]+(?:\.[0-9]+)?)\s*[0-9]+(?:\.[0-9]+)?(?:\s*[0-9]+(?:\.[0-9]+)?\s*[0-9]+(?:\.[0-9]+)?)?\s*\*\s*Book",
        r"quarterly\s+book\s*-?\s*to\s*-?\s*bill\s+ratio\s+(?:of|was)\s*([0-9]+(?:\.[0-9]+)?)x",
        r"book\s*-?\s*to\s*-?\s*bill\s+for\s+the\s+fourth\s+quarter[^0-9]{0,40}([0-9]+(?:\.[0-9]+)?)x",
        r"Book\s*-?\s*to\s*-?\s*Bill\s+of\s+([0-9]+(?:\.[0-9]+)?)",
        r"book\s*-?\s*to\s*-?\s*bill\s+ratio\s+(?:for\s+the\s+first\s+quarter\s+was|for\s+the\s+second\s+quarter\s+was|for\s+the\s+third\s+quarter\s+was|of|was)\s*([0-9]+(?:\.[0-9]+)?)\s*x",
    ]
    for pattern in patterns:
        match = re.search(pattern, clean, re.IGNORECASE)
        if match:
            return float(match.group(1))
    return None


def load_earnings_sources() -> dict[str, EarningsSource]:
    submissions = fetch_json(SEC_SUBMISSIONS_URL)
    recent = submissions["filings"]["recent"]
    earnings_8ks: list[dict] = []

    for filing_date, form, accession_number, primary_document in zip(
        recent["filingDate"],
        recent["form"],
        recent["accessionNumber"],
        recent["primaryDocument"],
    ):
        if form != "8-K" or filing_date < "2015-01-01":
            continue
        earnings_8ks.append({
            "filing_date": filing_date,
            "accession_number": accession_number,
            "primary_document": primary_document,
            "filing_url": archive_url(accession_number, primary_document),
        })

    sources: dict[str, EarningsSource] = {}
    report_dates = sorted(
        fd for fd, fm in zip(recent["filingDate"], recent["form"])
        if fm in {"10-Q", "10-K"} and fd >= "2015-01-01"
    )

    for report_date in report_dates:
        date_obj = datetime.strptime(report_date, "%Y-%m-%d")
        candidates = [
            (abs((datetime.strptime(f["filing_date"], "%Y-%m-%d") - date_obj).days), f)
            for f in earnings_8ks
            if abs((datetime.strptime(f["filing_date"], "%Y-%m-%d") - date_obj).days) <= 3
        ]
        candidates.sort(key=lambda item: (
            item[0],
            0 if item[1]["accession_number"].startswith("0001443646") else 1,
            0 if ("bah" in item[1]["primary_document"].lower() or "fy" in item[1]["primary_document"].lower()) else 1,
            item[1]["accession_number"],
        ))

        selected: Optional[EarningsSource] = None
        for _, filing in candidates:
            index_html = fetch_text(filing_index_url(filing["accession_number"]))
            exhibit_doc = parse_exhibit_99_1_doc(index_html)
            doc_candidates = [exhibit_doc] if exhibit_doc else []
            doc_candidates.append(filing["primary_document"])

            for doc in doc_candidates:
                exhibit_url = archive_url(filing["accession_number"], doc)
                btb = parse_book_to_bill(fetch_text(exhibit_url))
                if btb is not None:
                    selected = EarningsSource(
                        filing_date=filing["filing_date"],
                        accession_number=filing["accession_number"],
                        filing_url=filing["filing_url"],
                        exhibit_doc=doc,
                        exhibit_url=exhibit_url,
                        book_to_bill=btb,
                    )
                    break
            if selected:
                break

        sources[report_date] = selected or EarningsSource(
            filing_date="", accession_number="", filing_url="",
            exhibit_doc=None, exhibit_url=None, book_to_bill=None,
        )

    return sources


def build_dataset() -> tuple[pd.DataFrame, pd.DataFrame]:
    reports = load_quarterly_reports()
    revenue_lookup = build_revenue_lookup()
    earnings_sources = load_earnings_sources()

    rows, source_rows = [], []

    for report in reports:
        report_html = fetch_text(report.report_url)
        funded_backlog_m, unfunded_backlog_m, priced_options_backlog_m, backlog_m = parse_backlog(report_html)
        src = earnings_sources.get(report.filing_date)
        revenue_m = revenue_lookup.get(report.period_end)

        rows.append({
            "quarter": report.quarter,
            "date": report.period_end,
            "revenue_m": revenue_m,
            "backlog_m": backlog_m,
            "book_to_bill": src.book_to_bill if src else None,
            "funded_backlog_m": funded_backlog_m,
            "unfunded_backlog_m": unfunded_backlog_m,
            "priced_options_backlog_m": priced_options_backlog_m,
        })
        source_rows.append({
            "quarter": report.quarter,
            "date": report.period_end,
            "report_filing_date": report.filing_date,
            "earnings_filing_date": src.filing_date if src else "",
        })

    dataset = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    sources = pd.DataFrame(source_rows).sort_values("date").reset_index(drop=True)
    return dataset, sources


if __name__ == "__main__":
    ensure_dirs(RAW_DIR, PROCESSED_DIR)

    dataset, sources = build_dataset()
    dataset.to_csv(RAW_CSV, index=False)
    sources.to_csv(SOURCE_CSV, index=False)

    print(f"Wrote {len(dataset)} quarterly rows to {RAW_CSV}")
    print(f"Wrote source map to {SOURCE_CSV}")
    print(f"\nMissing values:\n{dataset.isna().sum().to_string()}")
