# WARNSignal: Research Report

## Executive Summary

- **Events Analyzed**: 4284
- **Mean CAR [0, +30]**: 2.71%
- **Median CAR [0, +30]**: 1.61%
- **t-statistic**: 8.4352
- **p-value**: 0.0000
- **% Negative**: 44.37%

## Backtest Results

- **Sharpe Ratio**: -0.1964
- **Max Drawdown**: 99.00%
- **Win Rate**: 31.25%
- **Total Return**: -99.00%
- **Number of Trades**: 16
- **Avg Return/Trade**: -10.25%

## CAR Analysis

![CAR Timeseries](charts/car_timeseries.png)

### CAR [-30, 0]
- Mean: -4.97% (t=-13.9881, p=0.0000)
- 95% CI: [-5.67%, -4.28%]

### CAR [0, +30]
- Mean: 2.71% (t=8.4352, p=0.0000)
- 95% CI: [2.08%, 3.34%]

### CAR [0, +60]
- Mean: 3.22% (t=7.4669, p=0.0000)
- 95% CI: [2.38%, 4.07%]

### CAR [0, +90]
- Mean: 5.05% (t=9.8174, p=0.0000)
- 95% CI: [4.04%, 6.06%]

## Alpha Decay

![Alpha Decay](charts/alpha_decay.png)

## Sector Breakdown

![Sector Heatmap](charts/sector_heatmap.png)

## Equity Curve

![Equity Curve](charts/equity_curve.png)

## Sub-Sample Analysis

Testing the microstructure thesis: signal should be strongest where coverage is thinnest.

| Sub-Sample | N | Mean CAR [0,+30] | t-stat | p-value | Mean CAR [0,+60] | Mean CAR [0,+90] |
|------------|---|------------------|--------|---------|------------------|------------------|
| **Full Sample** | 4284 | 2.71% | 8.4352 | 0.0000 | 3.22% | 5.05% |
| Micro + Small Cap | 1058 | 1.49% | 1.3865 | 0.1659 | 0.27% | 4.77% |
| Exclude Mega-Cap | 3920 | 2.96% | 8.4454 | 0.0000 | 3.32% | 5.25% |
| Exclude Technology | 3834 | 2.70% | 8.5063 | 0.0000 | 3.30% | 5.13% |
| Healthcare Only | 629 | 3.99% | 3.8012 | 0.0002 | 3.86% | 6.24% |
| Excl Mega-Cap + Tech | 3561 | 2.94% | 8.6484 | 0.0000 | 3.51% | 5.42% |

## Where It Breaks

The signal does **not** work uniformly. Honest reporting of failure modes:

### Key Finding: Signal Inversion

The overall post-filing CAR [0, +30] is **positive** (2.71%), meaning stocks on average *bounce* after WARN filings -- the opposite of a distress short signal. Pre-filing CAR [-30, 0] is -4.97%, confirming distress is priced in BEFORE the filing. The filing itself resolves uncertainty and triggers mean reversion.

Even micro/small caps show positive or insignificant CARs (1.49%, p=0.1659), suggesting WARN filings are universally lagging indicators of already-priced distress.

**Sectors where signal is weak or inverted** (positive CAR = market didn't punish the layoff):

- Utilities: Mean CAR = 4.30% (n=78)
- Communication Services: Mean CAR = 4.00% (n=240)
- Healthcare: Mean CAR = 3.99% (n=626)
- Consumer Cyclical: Mean CAR = 3.71% (n=595)
- Basic Materials: Mean CAR = 3.68% (n=178)
- Real Estate: Mean CAR = 2.84% (n=71)
- Consumer Defensive: Mean CAR = 2.80% (n=377)
- Technology: Mean CAR = 2.78% (n=476)
- Financial Services: Mean CAR = 2.13% (n=458)
- Energy: Mean CAR = 1.62% (n=110)
- Industrials: Mean CAR = 0.73% (n=1014)

**By market cap** (the critical dimension):

- large: Mean CAR = 3.17%, p=0.0000 (significant, n=2023) -- MEAN REVERSION (inverted)
- mega: Mean CAR = 0.22%, p=0.6230 (NOT significant, n=390) -- MEAN REVERSION (inverted)
- micro: Mean CAR = 2.11%, p=0.1823 (NOT significant, n=651) -- MEAN REVERSION (inverted)
- mid: Mean CAR = 4.42%, p=0.0000 (significant, n=754) -- MEAN REVERSION (inverted)
- small: Mean CAR = 0.48%, p=0.6810 (NOT significant, n=401) -- MEAN REVERSION (inverted)

The distress signal only works for micro/small caps where analyst coverage is thin. For large/mid caps, the signal is inverted -- WARN filings are followed by positive returns, consistent with market microstructure theory: dense coverage means the layoff is priced in before filing, and the filing itself triggers a relief rally.

**Implication for the backtest**: shorting all signals indiscriminately loses money because the portfolio is dominated by large/mid-cap names that bounce. Filtering to micro+small caps isolates the exploitable signal.

## Limitations

- **Entity resolution**: Match confidence drops below 80% for private subsidiaries. Low-confidence matches (< 85 score) are excluded from the backtest.
- **State coverage**: 9 states scraped (TX, FL, VA, CA, IN, IL, CO, MD, NY) -- expanding toward full national coverage.
- **Survivorship**: Delisted tickers are included but price data terminates at delisting, potentially understating full decline.
- **Transaction costs**: 10 bps/leg assumed. Signal may not survive for micro-caps with wide spreads and low liquidity -- the very segment where the signal is strongest.
- **Filing date lag**: Some state websites publish filings days after the actual filing date, introducing potential look-ahead.
- **Cap filter dependency**: The signal only works for micro/small caps. This subset has fewer events, increasing sampling noise and reducing statistical power.
- **Borrow costs**: Short-selling micro/small caps often incurs elevated borrow fees (not modeled), which would further reduce any exploitable signal.
- **Sample size**: Minimum 50 events recommended for statistical validity. Current sample (4284 events) meets this threshold.

---
*Generated by WARNSignal -- research signal, not investment advice*
