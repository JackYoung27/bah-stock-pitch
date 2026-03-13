# BAH Stock Pitch

Research repo for a long-only pitch on Booz Allen Hamilton (BAH).

## Scripts

- `scripts/build_quarterly_financials.py`: pulls quarterly revenue, backlog, funded backlog, unfunded backlog, priced options backlog, and book-to-bill from SEC filings.
- `scripts/01_backlog_conversion_model.py`: computes forward backlog conversion metrics and writes the backlog conversion output.
- `scripts/federal_contract_tracker.py`: pulls recent BAH federal contract actions from USAspending and writes contract summary files.

## Data Files

- `data/raw/bah_quarterly_financials.csv`: base quarterly history used by the backlog work.
- `data/processed/bah_quarterly_financial_sources.csv`: compact filing-date crosswalk for the quarterly history.
- `data/processed/backlog_conversion_results.csv`: quarter-by-quarter backlog conversion output, including next-quarter and next-four-quarter revenue conversion rates.
- `data/processed/bah_monthly_contract_actions.csv`: monthly BAH contract activity summary with gross obligations, net obligations, action counts, and unique awards.
- `data/processed/bah_agency_breakdown.csv`: top agency exposure from the contract pull.
- `data/processed/bah_sector_breakdown.csv`: DoD versus Civilian split from the contract pull.
- `data/processed/cleared_workforce_data.csv`: compact peer table for cleared employees by company.

## Run

- `python scripts/build_quarterly_financials.py`
- `python scripts/01_backlog_conversion_model.py`
- `python scripts/federal_contract_tracker.py`

## Dependencies

```bash
pip install pandas numpy matplotlib scikit-learn
```
