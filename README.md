# WARNSignal

Event study on WARN Act layoff filings as a potential distress signal for public equities. **Result: the signal is inverted.** WARN filings are a lagging indicator of distress already priced in — stocks mean-revert upward after filing, not down.

## Headline Finding

The hypothesis was that WARN filings predict negative post-filing returns. The data shows the opposite. Stocks earn **positive** cumulative abnormal returns after WARN filings, across all windows, all sectors, and nearly all market cap buckets. The distress happens *before* the filing date, not after.

This is a null result for the original trading signal, but a positive result for market efficiency: the market prices in mass layoff distress well before the mandatory filing appears on a state government website.

## Key Results

### Full Sample (N = 4,284 events, 1,067 unique tickers, 9 states)

| Metric | [-30, 0] | [0, +30] | [0, +60] | [0, +90] |
|--------|----------|----------|----------|----------|
| **Mean CAR** | -4.97% | +2.71% | +3.22% | +5.05% |
| **t-statistic** | -13.99 | 8.44 | 7.47 | 9.82 |
| **p-value** | <0.0001 | <0.0001 | <0.0001 | <0.0001 |

Pre-filing CARs are significantly negative: the market sells off before the WARN filing date. Post-filing CARs are significantly positive: stocks bounce. The filing marks the bottom, not the beginning of distress.

### Sub-Sample Analysis

| Sub-Sample | N | Mean CAR [0,+30] | t-stat | p-value | Significant? |
|------------|---|-------------------|--------|---------|--------------|
| **Full Sample** | 4,284 | +2.71% | 8.44 | <0.0001 | Yes |
| **Micro + Small Cap** | — | +1.49% | 1.39 | 0.17 | **No** |
| **Exclude Mega-Cap** | — | +2.96% | 8.45 | <0.0001 | Yes |
| **Healthcare Only** | — | +3.99% | 3.80 | 0.0002 | Yes |

Every sub-sample shows positive post-filing CARs. The micro/small-cap bucket — where the original hypothesis predicted the strongest short signal — is the one place it is not statistically significant, but the point estimate is still positive (+1.49%), not negative. There is no sub-sample where shorting after a WARN filing is a winning strategy.

### Backtest: Short Strategy Destroys Capital

| Metric | Value |
|--------|-------|
| **Sharpe Ratio** | -0.68 |
| **Total Return** | -99% |
| **Win Rate** | 35% |

A systematic short strategy based on WARN filings loses nearly all capital. The signal is not just weak — it is pointed in the wrong direction.

## Why the Hypothesis Failed

The WARN Act requires **60 days advance notice** before mass layoffs. This creates a specific timeline:

1. **Company decides on layoffs** (internal, often leaked or anticipated by analysts)
2. **Market begins pricing in distress** (stock declines over [-30, 0] window)
3. **WARN filing appears on state website** (the event date in this study)
4. **Uncertainty resolves** — the filing confirms what was suspected, triggering a relief rally
5. **Stock mean-reverts** (positive CARs in [0, +30], [0, +60], [0, +90])

The WARN filing is not a leading indicator of future distress. It is a lagging indicator that confirms distress the market has already priced in. The mandatory 60-day notice window means layoffs are typically known or expected before the state filing is publicly available. The filing itself may act as a catalyst for uncertainty resolution rather than as new negative information.

This is consistent with semi-strong form market efficiency: publicly mandated filings on government websites do not contain information the market has not already incorporated through other channels (earnings calls, press releases, analyst coverage, supply chain signals, job board monitoring).

## Data Scale

| Dimension | Count |
|-----------|-------|
| **Total WARN filings scraped** | 11,677 |
| **States covered** | 9 (TX: 2,427 / FL: 2,391 / VA: 2,030 / CA: 1,350 / IN: 986 / IL: 793 / CO: 721 / MD: 577 / NY: 402) |
| **Unique tickers matched** | 1,067 |
| **Event studies completed** | 4,284 |
| **Price data rows** | ~3.4M |

## Methodology

### Event Study (Standard Academic Framework)

1. **Data Collection**: Scrape WARN filings from 9 US states (TX, FL, VA, CA, IN, IL, CO, MD, NY)
2. **Entity Resolution**: Match company names to public tickers using rapidfuzz (threshold >= 85) + SEC EDGAR EFTS fallback
3. **Market Model**: Estimate `R_stock = alpha + beta * R_benchmark` over [-270, -31] trading days
4. **Abnormal Returns**: `AR = R_stock - (alpha_hat + beta_hat * R_benchmark)` in event window [-30, +90]
5. **CAR**: Cumulative sum of AR across windows [-30,0], [0,+30], [0,+60], [0,+90]
6. **Significance**: Cross-sectional t-test on CAR (H0: mean CAR = 0), with sub-sample breakdowns by sector, market cap, and employee impact

### Statistical Rigor

- **Bootstrap confidence intervals** on mean CARs
- **Benjamini-Hochberg FDR correction** for multiple comparisons across sub-samples
- **Wilcoxon signed-rank tests** as non-parametric complement to t-tests
- **Placebo/permutation tests** to verify the signal is not an artifact of the methodology
- **Sub-sample analysis** across 5 cuts (market cap, sector, state, filing size, time period)

### Backtest Design

- **Signal**: Short top-quintile composite scores on filing date
- **Entry**: T+1 open price (strict no look-ahead — asserted in tests)
- **Hold**: 30 trading days
- **Benchmark**: Sector ETF (XLK, XLV, XLF, etc.) per GICS mapping
- **Costs**: 10 bps per leg (20 bps round-trip)
- **Positions**: Max 20 concurrent, equal weight

## Where It Breaks

The signal does not work. Period. But it breaks in instructive ways:

- **Every sector**: All sectors show positive post-filing CARs. The inversion is not sector-specific.
- **All cap buckets except micro/small**: Excluding mega-caps still yields +2.96% CAR (t=8.45). The positive drift is broad.
- **Micro/small-cap**: The one sub-sample where the short thesis had the best prior odds shows an insignificant +1.49% (p=0.17). Not negative — just noisy.
- **The backtest**: Sharpe of -0.68 and -99% total return. The strategy does not "almost work" — it actively destroys capital.
- **Healthcare**: The strongest positive CAR (+3.99%, p=0.0002), likely because healthcare layoffs are associated with restructuring/M&A rather than terminal decline.

## Limitations

- **Entity resolution confidence**: Match accuracy drops below 80% for private subsidiaries filing under parent company names. Low-confidence matches (< 85 score) are excluded. Approximately 60-70% of WARN filings are for private companies and never enter the event study.
- **Filing date vs. public awareness date**: Some WARN filings appear on state websites days after the actual filing date. The event study uses the filing date from the document, not the web publication date.
- **Survivorship bias risk**: Delisted companies are included but their price data terminates at delisting. CARs may understate the full decline for terminal cases, though this should bias *toward* the original hypothesis, not against it.
- **State coverage**: 9 states are scraped. WARN filings in the remaining states are missed.
- **Transaction costs**: 10 bps per leg is realistic for liquid mid-caps but optimistic for micro-caps with wide spreads. Moot given the signal is inverted.
- **Scraper fragility**: Government websites change format without notice. Scrapers include fallback parsers but may require maintenance.

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

## Project Structure

```
warnsignal/
├── app/                          # Next.js frontend (4 pages)
├── backend/
│   ├── services/
│   │   ├── scrapers/             # 9 state WARN scrapers
│   │   ├── entity_resolution/    # rapidfuzz + SEC EDGAR ticker matching
│   │   ├── event_study/          # CAR calculator + cross-sectional statistics
│   │   ├── signal/               # Composite distress scorer
│   │   ├── backtest/             # Portfolio simulation with look-ahead guards
│   │   └── report/              # Research report + chart generation
│   ├── scripts/                  # CLI pipeline (scrape -> resolve -> backtest)
│   └── tests/                    # 42 pytest tests
└── output/                       # Generated report + matplotlib charts
```
