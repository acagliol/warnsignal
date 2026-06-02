"use client";

import { useEffect, useState } from "react";
import { getSignals, getBacktestStats, type Signal, type BacktestStats } from "@/lib/api";
import { formatPct, formatNum, pctColor } from "@/lib/utils";

export default function Dashboard() {
  const [signals, setSignals] = useState<Signal[]>([]);
  const [stats, setStats] = useState<BacktestStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.allSettled([
      getSignals({ limit: 10 }),
      getBacktestStats(),
    ]).then(([sigRes, statsRes]) => {
      if (sigRes.status === "fulfilled") setSignals(sigRes.value.signals);
      if (statsRes.status === "fulfilled") setStats(statsRes.value);
      setLoading(false);
    });
  }, []);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-lg font-bold tracking-wider">DASHBOARD</h1>
        <span className="text-xs text-muted-foreground">
          WARN ACT DISTRESS SIGNAL MONITOR
        </span>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatCard
          label="EVENTS ANALYZED"
          value={stats?.n_events?.toString() ?? "--"}
        />
        <StatCard
          label="MEAN CAR [0,+30]"
          value={formatPct(stats?.mean_car_post30)}
          color={pctColor(stats?.mean_car_post30)}
        />
        <StatCard
          label="T-STATISTIC"
          value={formatNum(stats?.t_stat)}
        />
        <StatCard
          label="P-VALUE"
          value={formatNum(stats?.p_value, 4)}
          color={stats?.p_value != null && stats.p_value < 0.05 ? "text-positive" : ""}
        />
      </div>

      {/* Recent Signals */}
      <div className="bg-card rounded border border-border">
        <div className="px-4 py-3 border-b border-border flex items-center justify-between">
          <h2 className="text-sm font-semibold tracking-wider">RECENT SIGNALS</h2>
          <a href="/signals" className="text-xs text-primary hover:underline">
            VIEW ALL
          </a>
        </div>
        <div className="overflow-x-auto">
          <table className="data-table">
            <thead>
              <tr>
                <th>TICKER</th>
                <th>DATE</th>
                <th>EMPLOYEES</th>
                <th>SECTOR</th>
                <th>SCORE</th>
                <th>CAR [0,+30]</th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={6} className="text-center text-muted-foreground py-8">
                    Loading...
                  </td>
                </tr>
              ) : signals.length === 0 ? (
                <tr>
                  <td colSpan={6} className="text-center text-muted-foreground py-8">
                    No signals yet. Run the pipeline first.
                  </td>
                </tr>
              ) : (
                signals.map((s) => (
                  <tr key={s.id}>
                    <td className="font-semibold text-accent">{s.ticker}</td>
                    <td>{s.signal_date}</td>
                    <td>{s.employees_affected?.toLocaleString() ?? "--"}</td>
                    <td className="text-xs">{s.sector ?? "--"}</td>
                    <td>
                      <span className="bg-primary/20 text-primary px-2 py-0.5 rounded text-xs font-semibold">
                        {(s.composite_score * 100).toFixed(0)}
                      </span>
                    </td>
                    <td className={pctColor(s.car_post30)}>
                      {formatPct(s.car_post30)}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Sector Breakdown */}
      {stats?.sector_breakdown && Object.keys(stats.sector_breakdown).length > 0 && (
        <div className="bg-card rounded border border-border">
          <div className="px-4 py-3 border-b border-border">
            <h2 className="text-sm font-semibold tracking-wider">SECTOR BREAKDOWN</h2>
          </div>
          <div className="p-4">
            <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
              {Object.entries(stats.sector_breakdown).map(([sector, data]) => (
                <div key={sector} className="bg-muted/50 rounded p-3">
                  <div className="text-xs text-muted-foreground">{sector}</div>
                  <div className={`text-lg font-bold ${pctColor(data.mean)}`}>
                    {formatPct(data.mean)}
                  </div>
                  <div className="text-xs text-muted-foreground">
                    n={data.n_events}
                  </div>
                </div>
              ))}
            </div>
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
      <div className="text-xs text-muted-foreground tracking-wider mb-1">{label}</div>
      <div className={`text-xl font-bold ${color}`}>{value}</div>
    </div>
  );
}
