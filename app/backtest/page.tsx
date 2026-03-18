"use client";

import { useEffect, useState } from "react";
import {
  getBacktestResults,
  getBacktestStats,
  type BacktestResult,
  type BacktestStats,
} from "@/lib/api";
import { formatPct, formatNum, pctColor } from "@/lib/utils";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from "recharts";

export default function BacktestPage() {
  const [result, setResult] = useState<BacktestResult | null>(null);
  const [stats, setStats] = useState<BacktestStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.allSettled([getBacktestResults(), getBacktestStats()])
      .then(([resResult, resStats]) => {
        if (resResult.status === "fulfilled") setResult(resResult.value);
        if (resStats.status === "fulfilled") setStats(resStats.value);
      })
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-muted-foreground">
        Loading backtest results...
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <h1 className="text-lg font-bold tracking-wider">BACKTEST RESULTS</h1>

      {/* Stats Cards */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
        <StatCard label="SHARPE RATIO" value={formatNum(result?.sharpe_ratio)} />
        <StatCard
          label="MAX DRAWDOWN"
          value={formatPct(result?.max_drawdown)}
          color="text-negative"
        />
        <StatCard label="WIN RATE" value={formatPct(result?.win_rate)} />
        <StatCard
          label="TOTAL RETURN"
          value={formatPct(result?.total_return)}
          color={pctColor(result?.total_return)}
        />
        <StatCard label="N TRADES" value={result?.n_trades?.toString() ?? "--"} />
      </div>

      {/* Signal Statistics */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div className="bg-card rounded border border-border p-4">
          <h2 className="text-sm font-semibold tracking-wider mb-3">
            CAR STATISTICS
          </h2>
          <div className="space-y-2 text-sm">
            <Row label="Mean CAR [0,+30]" value={formatPct(stats?.mean_car_post30)} color={pctColor(stats?.mean_car_post30)} />
            <Row label="Median CAR [0,+30]" value={formatPct(stats?.median_car_post30)} color={pctColor(stats?.median_car_post30)} />
            <Row label="t-statistic" value={formatNum(stats?.t_stat)} />
            <Row label="p-value" value={formatNum(stats?.p_value, 4)} color={stats?.p_value != null && stats.p_value < 0.05 ? "text-positive" : ""} />
            <Row label="% Negative" value={formatPct(stats?.pct_negative)} />
            <Row label="Events" value={stats?.n_events?.toString() ?? "--"} />
          </div>
        </div>

        {/* Alpha Decay */}
        {stats?.alpha_decay && stats.alpha_decay.length > 0 && (
          <div className="bg-card rounded border border-border p-4">
            <h2 className="text-sm font-semibold tracking-wider mb-3">
              ALPHA DECAY CURVE
            </h2>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={stats.alpha_decay}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                <XAxis
                  dataKey="window"
                  stroke="#64748b"
                  fontSize={11}
                  tickFormatter={(v: number) => `+${v}d`}
                />
                <YAxis
                  stroke="#64748b"
                  fontSize={11}
                  tickFormatter={(v: number) => `${(v * 100).toFixed(1)}%`}
                />
                <Tooltip
                  contentStyle={{
                    background: "#0f172a",
                    border: "1px solid #1e293b",
                    borderRadius: 4,
                    fontSize: 12,
                  }}
                  formatter={(v: number) => [`${(v * 100).toFixed(2)}%`, "Mean CAR"]}
                  labelFormatter={(v: number) => `Hold: +${v} days`}
                />
                <Bar dataKey="mean_car">
                  {stats.alpha_decay.map((entry, idx) => (
                    <Cell
                      key={idx}
                      fill={entry.mean_car < 0 ? "#ef4444" : "#22c55e"}
                    />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>

      {/* Cap Breakdown */}
      {stats?.cap_breakdown && Object.keys(stats.cap_breakdown).length > 0 && (
        <div className="bg-card rounded border border-border p-4">
          <h2 className="text-sm font-semibold tracking-wider mb-3">
            MARKET CAP BREAKDOWN
          </h2>
          <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
            {Object.entries(stats.cap_breakdown).map(([bucket, data]) => (
              <div key={bucket} className="bg-muted/50 rounded p-3 text-center">
                <div className="text-xs text-muted-foreground uppercase">{bucket}</div>
                <div className={`text-lg font-bold ${pctColor(data.mean)}`}>
                  {formatPct(data.mean)}
                </div>
                <div className="text-xs text-muted-foreground">n={data.n_events}</div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Recent Trades */}
      {result?.trades && result.trades.length > 0 && (
        <div className="bg-card rounded border border-border">
          <div className="px-4 py-3 border-b border-border">
            <h2 className="text-sm font-semibold tracking-wider">TRADE LOG</h2>
          </div>
          <div className="overflow-x-auto max-h-96">
            <table className="data-table">
              <thead>
                <tr>
                  <th>TICKER</th>
                  <th>ENTRY</th>
                  <th>EXIT</th>
                  <th>HOLD</th>
                  <th>RETURN</th>
                </tr>
              </thead>
              <tbody>
                {result.trades.slice(0, 50).map((t, i) => (
                  <tr key={i}>
                    <td className="font-semibold text-accent">{t.ticker}</td>
                    <td>{t.entry_date}</td>
                    <td>{t.exit_date}</td>
                    <td>{t.hold_days}d</td>
                    <td className={pctColor(t.return_pct)}>
                      {formatPct(t.return_pct)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

function StatCard({
  label,
  value,
  color = "",
}: {
  label: string;
  value: string;
  color?: string;
}) {
  return (
    <div className="bg-card rounded border border-border p-4">
      <div className="text-xs text-muted-foreground tracking-wider mb-1">
        {label}
      </div>
      <div className={`text-xl font-bold ${color}`}>{value}</div>
    </div>
  );
}

function Row({
  label,
  value,
  color = "",
}: {
  label: string;
  value: string;
  color?: string;
}) {
  return (
    <div className="flex justify-between">
      <span className="text-muted-foreground">{label}</span>
      <span className={`font-semibold ${color}`}>{value}</span>
    </div>
  );
}
