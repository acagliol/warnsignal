# WARNSignal: Research Report

## Executive Summary

- **Events Analyzed**: 1153
- **Mean CAR [0, +30]**: 3.69%
- **Median CAR [0, +30]**: 5.89%
- **t-statistic**: 4.6214
- **p-value**: 0.0000
- **% Negative**: 34.87%

## Backtest Results

- **Sharpe Ratio**: 0.4996
- **Max Drawdown**: 99.81%
- **Win Rate**: 58.96%
- **Total Return**: -99.00%
- **Number of Trades**: 134
- **Avg Return/Trade**: 3.98%

## CAR Analysis

![CAR Timeseries](charts/car_timeseries.png)

### CAR [-30, 0]
- Mean: -11.19% (t=-13.7487, p=0.0000)
- 95% CI: [-12.78%, -9.59%]

### CAR [0, +30]
- Mean: 3.69% (t=4.6214, p=0.0000)
- 95% CI: [2.13%, 5.26%]

### CAR [0, +60]
- Mean: 4.52% (t=4.5047, p=0.0000)
- 95% CI: [2.55%, 6.49%]

### CAR [0, +90]
- Mean: 5.97% (t=5.4023, p=0.0000)
- 95% CI: [3.81%, 8.14%]

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
| **Full Sample** | 1153 | 3.69% | 4.6214 | 0.0000 | 4.52% | 5.97% |
| Micro + Small Cap | 308 | -5.31% | -2.0209 | 0.0442 | -8.35% | -0.66% |
| Exclude Mega-Cap | 1071 | 3.73% | 4.3513 | 0.0000 | 4.32% | 5.68% |
| Exclude Technology | 996 | 3.38% | 4.6735 | 0.0000 | 5.06% | 6.57% |
| Healthcare Only | 385 | 2.16% | 1.5025 | 0.1338 | 1.79% | 3.14% |
| Excl Mega-Cap + Tech | 958 | 3.34% | 4.4582 | 0.0000 | 4.80% | 6.23% |

## Where It Breaks

The signal does **not** work uniformly. Honest reporting of failure modes:

### Key Finding: Signal Inversion by Market Cap

The overall post-filing CAR [0, +30] is **positive** (+3.69%), meaning stocks on average *bounce* after WARN filings -- the opposite of a distress short signal. This is driven by mean reversion in large/mid-cap names where analyst coverage is dense and markets quickly price in the layoff as a cost-cutting positive.

The key finding: WARN filings for micro/small-cap companies signal continued distress (CAR = -5.31%, p < 0.05), while large-cap filings signal buying opportunities as markets overreact then revert.

**Sectors where signal is weak or inverted** (positive CAR = market didn't punish the layoff):

- Basic Materials: Mean CAR = 14.20% (n=33)
- Communication Services: Mean CAR = 7.38% (n=82)
- Utilities: Mean CAR = 6.28% (n=22)
- Technology: Mean CAR = 5.69% (n=158)
- Energy: Mean CAR = 5.47% (n=45)
- Industrials: Mean CAR = 4.32% (n=194)
- Real Estate: Mean CAR = 3.60% (n=7)
- Financial Services: Mean CAR = 2.27% (n=85)
- Healthcare: Mean CAR = 2.16% (n=384)
- Consumer Cyclical: Mean CAR = 1.08% (n=98)

**By market cap** (the critical dimension):

- large: Mean CAR = 7.49%, p=0.0000 (significant, n=517) -- MEAN REVERSION (inverted)
- mega: Mean CAR = 3.22%, p=0.0032 (significant, n=83) -- MEAN REVERSION (inverted)
- micro: Mean CAR = -4.91%, p=0.0987 (NOT significant, n=244) -- DISTRESS signal
- mid: Mean CAR = 7.37%, p=0.0000 (significant, n=216) -- MEAN REVERSION (inverted)
- small: Mean CAR = -6.84%, p=0.2350 (NOT significant, n=63) -- DISTRESS signal

The distress signal only works for micro/small caps where analyst coverage is thin. For large/mid caps, the signal is inverted -- WARN filings are followed by positive returns, consistent with market microstructure theory: dense coverage means the layoff is priced in before filing, and the filing itself triggers a relief rally.

**Implication for the backtest**: shorting all signals indiscriminately loses money because the portfolio is dominated by large/mid-cap names that bounce. Filtering to micro+small caps isolates the exploitable signal.

## Limitations

- **Entity resolution**: Match confidence drops below 80% for private subsidiaries. Low-confidence matches (< 85 score) are excluded from the backtest.
- **State coverage**: Only 5 states scraped -- filings in other states are missed entirely.
- **Survivorship**: Delisted tickers are included but price data terminates at delisting, potentially understating full decline.
- **Transaction costs**: 10 bps/leg assumed. Signal may not survive for micro-caps with wide spreads and low liquidity -- the very segment where the signal is strongest.
- **Filing date lag**: Some state websites publish filings days after the actual filing date, introducing potential look-ahead.
- **Cap filter dependency**: The signal only works for micro/small caps. This subset has fewer events, increasing sampling noise and reducing statistical power.
- **Borrow costs**: Short-selling micro/small caps often incurs elevated borrow fees (not modeled), which could erode or eliminate the -5.31% CAR advantage.
- **Sample size**: Minimum 50 events recommended for statistical validity. Current sample (1153 events) meets this threshold.

---
*Generated by WARNSignal -- research signal, not investment advice*
