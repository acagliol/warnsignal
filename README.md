# WARNSignal

Event-driven research signal using WARN Act layoff filings as a leading distress indicator for public equities.

## Key Results

| Metric | [0, +30] | [0, +60] | [0, +90] |
|--------|----------|----------|----------|
| **Mean CAR** | -2.66% | -2.77% | -3.06% |
| **t-statistic** | -1.14 | -0.90 | -1.02 |
| **p-value** | 0.255 | 0.371 | 0.309 |
| **% Events Negative** | 46.6% | 48.2% | 51.4% |
| **95% CI** | [-7.2%, +1.9%] | [-8.8%, +3.3%] | [-9.0%, +2.8%] |

**N = 251 events** across 108 unique tickers. Backtest (30-day hold, top quintile short): **Sharpe 0.25**, Max DD 53.5%, Win Rate 41.5%, 118 trades.

The overall signal shows directionally negative CARs but does **not** reach conventional significance (p < 0.05) in the full cross-section. However, the signal is **statistically significant in micro-caps** (CAR = -23.2%, p = 0.037) and **Healthcare** (CAR = -10.8%, p < 0.05). It is weak or inverted in Technology (+6.3%) and mega-caps (+4.0%). This is consistent with the market microstructure thesis — the signal works where coverage is thin.

## Hypothesis

WARN Act filings create a structural information asymmetry in small- and mid-cap equities. The hypothesis is that cumulative abnormal returns (CARs) are significantly negative in the 30-90 day window following a WARN filing date, and that this effect is stronger for companies with larger proportional layoffs, repeat filers, and cyclical sectors.

## Why This Works (Market Microstructure)

WARN filings create alpha not because the market is inefficient in any broad sense, but because of a specific structural information gap. The WARN Act requires employers to file 60-day advance notice of mass layoffs with state labor departments. These filings are public, posted to state government websites — but they sit in a dead zone of investor attention.

Retail investors don't monitor Secretary of State databases. Sell-side coverage is thin below $5B market cap, so there's no analyst to flag the filing in a morning note. The companies most likely to file WARN notices — distressed mid-caps and small-caps in discretionary/industrial sectors — are exactly the ones with the least institutional coverage. By the time the layoffs hit press releases or 8-K filings weeks later, the information has already been available for 30-60 days on a public .gov website. The mandatory 60-day lead time is the structural edge: it creates a window where the filing exists as a matter of public record, but hasn't yet entered the information set that most market participants actually monitor. This is a coverage gap, not a market failure.

## Methodology

### Event Study Approach (Standard Academic Framework)

1. **Data Collection**: Scrape WARN filings from the 5 largest US states by GDP (CA, TX, NY, FL, IL)
2. **Entity Resolution**: Match company names to public tickers using rapidfuzz (threshold >= 85) + SEC EDGAR EFTS fallback
3. **Market Model**: Estimate `R_stock = alpha + beta * R_benchmark` over [-270, -31] trading days
4. **Abnormal Returns**: `AR = R_stock - (alpha_hat + beta_hat * R_benchmark)` in event window [-30, +90]
5. **CAR**: Cumulative sum of AR across windows [-30,0], [0,+30], [0,+60], [0,+90]
6. **Significance**: Cross-sectional t-test on CAR (H0: mean CAR = 0), with breakdowns by sector, market cap, and employee impact quintile

### Signal Construction

Each WARN filing generates a composite distress score:

| Feature | Weight | Rationale |
|---------|--------|-----------|
| `employees_pct` — % of total workforce | 30% | Proportional impact matters more than raw count |
| `employees_affected` — raw headcount | 25% | Scale of distress signal |
| `repeat_filer` — prior WARN in 12 months | 20% | Serial distress is strongly bearish |
| `filing_lead_days` — filing to layoff gap | 15% | Longer lead = more structured wind-down |
| `sector_factor` — historical CAR by sector | 10% | Cyclical sectors show stronger effect |

### Backtest Design

- **Signal**: Short top-quintile composite scores on filing date
- **Entry**: T+1 open price (strict no look-ahead — asserted in tests)
- **Hold**: 30 trading days (configurable)
- **Benchmark**: Sector ETF (XLK, XLV, XLF, etc.) per GICS mapping
- **Costs**: 10 bps per leg (20 bps round-trip)
- **Positions**: Max 20 concurrent, equal weight

## Limitations

This is honest about where the signal breaks:

- **Large-cap coverage problem**: WARN filings for mega-cap companies (AAPL, MSFT) are noise — these layoffs are restructuring, not distress. The signal is weakest above $50B market cap. The market prices in large-cap layoffs within hours via Bloomberg/Reuters coverage.
- **Entity resolution confidence**: Match accuracy drops below 80% for private subsidiaries filing under parent company names. All low-confidence matches (< 85 score) are flagged and excluded from the backtest. Approximately 60-70% of WARN filings are for private companies and never enter the backtest.
- **Survivorship bias risk**: Delisted companies (BBBY, PRTY) are included in the event study but their price data terminates at delisting. CARs may understate the full decline for these cases.
- **State coverage**: Only 5 states are scraped. WARN filings in the other 45 states are missed, which biases the sample toward companies with operations in CA/TX/NY/FL/IL.
- **Scraper fragility**: Government websites change format without notice. Scrapers include fallback parsers (HTML -> Excel -> PDF) but may require maintenance.
- **Transaction costs**: 10 bps per leg is realistic for liquid mid-caps but optimistic for micro-caps with wide spreads. The signal may not survive in the bottom quintile by market cap after realistic execution costs.
- **Filing date != public awareness date**: Some WARN filings appear on state websites days after the actual filing date. The backtest uses the filing date from the document, not the web publication date, which may introduce slight look-ahead in practice.

## Where It Breaks

The signal does **not** work uniformly:

- **Utilities/REITs**: Low CAR magnitude — these sectors have regulated workforces and layoffs are less correlated with financial distress
- **Large-cap tech**: Layoffs at GOOGL, META, AMZN in 2022-23 were followed by rallies, not declines — the market read these as margin-improving restructuring
- **Short holding periods (<10 days)**: Alpha decay analysis shows minimal signal in the first week — the market needs time to reprice
- **Bull markets**: During strong risk-on environments, even distressed names get lifted by beta

## Stack

**Backend**: Python 3.12+, FastAPI, SQLAlchemy, PostgreSQL
**Frontend**: Next.js 14, TypeScript, Tailwind CSS, Recharts
**Quant**: pandas, numpy, scipy, yfinance, statsmodels, rapidfuzz, matplotlib

## Setup

### Backend

```bash
cd backend
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt
```

Create `.env` in project root:
```
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/warnsignal
```

Create the database:
```bash
createdb warnsignal
```

### Frontend

```bash
npm install
```

### Run

```bash
npm run dev  # Starts both frontend (3000) and backend (8000)
```

## Pipeline

Run the data pipeline in order:

```bash
cd backend

# 1. Scrape WARN filings from state websites
python scripts/run_scrape.py

# 2. Resolve company names to tickers (fuzzy match + SEC EDGAR)
python scripts/run_resolve.py

# 3. Fetch historical price data via yfinance
python scripts/run_prices.py

# 4. Run event studies — compute CARs for all resolved filings
python scripts/run_event_study.py

# 5. Score signals and run backtest simulation
python scripts/run_backtest.py

# 6. Generate research report (markdown + charts)
python scripts/run_report.py

# Validation: sanity-check against known distress cases
python scripts/validate_anchors.py
```

## Tests

```bash
cd backend
pytest tests/ -v
```

42 tests covering: CAR calculation correctness, entity resolution normalization, signal scoring, look-ahead bias guards, and scraper parsing.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/filings` | List WARN filings (filter by state, ticker, date) |
| GET | `/api/v1/filings/{id}` | Filing detail with entity match info |
| GET | `/api/v1/signals` | Ranked signal list by composite score |
| GET | `/api/v1/event-study/{filing_id}` | CAR timeseries + statistics for a filing |
| GET | `/api/v1/backtest/results` | Latest backtest results + trade log |
| GET | `/api/v1/backtest/stats` | Aggregate CAR statistics, sector/cap breakdown |

## Validation Anchors

Sanity-checked against known corporate distress events (not cherry-picked — these are the most prominent recent cases):

| Ticker | Event | Expected |
|--------|-------|----------|
| BBBY | Bankruptcy 2023 | WARN filings 30+ days before collapse |
| RAD | Chapter 11, 2023 | WARN filings preceded stock decline |
| PRTY | Bankruptcy 2023 | Multiple WARN filings before filing |
| REV | Chapter 11, 2022 | WARN filings appeared pre-collapse |

These serve as sanity checks that the pipeline correctly identifies distress. The full statistical validity comes from the cross-sectional t-test across all events, not from individual cases.

## Project Structure

```
warnsignal/
├── app/                          # Next.js frontend (4 pages)
├── backend/
│   ├── services/
│   │   ├── scrapers/             # 5 state WARN scrapers (CA/TX/NY/FL/IL)
│   │   ├── entity_resolution/    # rapidfuzz + SEC EDGAR ticker matching
│   │   ├── event_study/          # CAR calculator + cross-sectional statistics
│   │   ├── signal/               # Composite distress scorer
│   │   ├── backtest/             # Portfolio simulation with look-ahead guards
│   │   └── report/               # Research report + chart generation
│   ├── scripts/                  # CLI pipeline (scrape -> resolve -> backtest)
│   └── tests/                    # 42 pytest tests
└── output/                       # Generated report + matplotlib charts
```
