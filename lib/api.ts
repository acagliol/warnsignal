const BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

async function fetchJSON<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    ...init,
    headers: { "Content-Type": "application/json", ...init?.headers },
  });
  if (!res.ok) throw new Error(`API error: ${res.status} ${res.statusText}`);
  return res.json();
}

// Types
export interface Filing {
  id: number;
  state: string;
  company_name_raw: string;
  filing_date: string;
  layoff_date: string | null;
  employees_affected: number | null;
  location: string | null;
  ticker: string | null;
  match_score: number | null;
  sector: string | null;
}

export interface Signal {
  id: number;
  ticker: string;
  signal_date: string;
  employees_affected: number | null;
  employees_pct: number | null;
  filing_lead_days: number | null;
  repeat_filer: boolean;
  sector: string | null;
  market_cap_bucket: string | null;
  composite_score: number;
  car_post30: number | null;
}

export interface BacktestResult {
  run_id: number;
  run_name: string | null;
  sharpe_ratio: number | null;
  max_drawdown: number | null;
  total_return: number | null;
  win_rate: number | null;
  n_trades: number | null;
  trades: Array<{
    ticker: string;
    entry_date: string;
    exit_date: string;
    return_pct: number;
    hold_days: number;
  }>;
}

export interface BacktestStats {
  mean_car_post30: number | null;
  median_car_post30: number | null;
  t_stat: number | null;
  p_value: number | null;
  pct_negative: number | null;
  n_events: number;
  sector_breakdown: Record<string, { mean: number; n_events: number }>;
  cap_breakdown: Record<string, { mean: number; n_events: number }>;
  alpha_decay: Array<{ window: number; mean_car: number }>;
}

export interface EventStudy {
  filing_id: number;
  ticker: string;
  benchmark_ticker: string;
  car_pre30: number | null;
  car_post30: number | null;
  car_post60: number | null;
  car_post90: number | null;
  car_timeseries: Array<{ day: number; car: number }>;
  alpha: number | null;
  beta: number | null;
  t_stat: number | null;
  p_value: number | null;
}

// API functions
export function getFilings(params?: {
  state?: string;
  ticker?: string;
  limit?: number;
  resolved_only?: boolean;
}) {
  const qs = new URLSearchParams();
  if (params?.state) qs.set("state", params.state);
  if (params?.ticker) qs.set("ticker", params.ticker);
  if (params?.limit) qs.set("limit", String(params.limit));
  if (params?.resolved_only) qs.set("resolved_only", "true");
  return fetchJSON<{ total: number; filings: Filing[] }>(
    `/api/v1/filings?${qs.toString()}`
  );
}

export function getSignals(params?: { min_score?: number; limit?: number }) {
  const qs = new URLSearchParams();
  if (params?.min_score) qs.set("min_score", String(params.min_score));
  if (params?.limit) qs.set("limit", String(params.limit));
  return fetchJSON<{ signals: Signal[] }>(`/api/v1/signals?${qs.toString()}`);
}

export function getBacktestResults() {
  return fetchJSON<BacktestResult>("/api/v1/backtest/results");
}

export function getBacktestStats() {
  return fetchJSON<BacktestStats>("/api/v1/backtest/stats");
}

export function getEventStudy(filingId: number) {
  return fetchJSON<EventStudy>(`/api/v1/event-study/${filingId}`);
}
