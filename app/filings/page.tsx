"use client";

import { useEffect, useState } from "react";
import { getFilings, type Filing } from "@/lib/api";

const STATES = ["", "CA", "TX", "NY", "FL", "IL"];

export default function FilingsPage() {
  const [filings, setFilings] = useState<Filing[]>([]);
  const [total, setTotal] = useState(0);
  const [state, setState] = useState("");
  const [resolvedOnly, setResolvedOnly] = useState(false);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    getFilings({
      state: state || undefined,
      resolved_only: resolvedOnly,
      limit: 100,
    })
      .then((data) => {
        setFilings(data.filings);
        setTotal(data.total);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [state, resolvedOnly]);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-lg font-bold tracking-wider">WARN FILINGS</h1>
        <span className="text-xs text-muted-foreground">{total} total</span>
      </div>

      {/* Filters */}
      <div className="flex gap-3 items-center">
        <select
          value={state}
          onChange={(e) => setState(e.target.value)}
          className="bg-muted border border-border rounded px-3 py-1.5 text-sm"
        >
          <option value="">All States</option>
          {STATES.filter(Boolean).map((s) => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>

        <label className="flex items-center gap-2 text-sm text-muted-foreground cursor-pointer">
          <input
            type="checkbox"
            checked={resolvedOnly}
            onChange={(e) => setResolvedOnly(e.target.checked)}
            className="rounded"
          />
          Resolved only
        </label>
      </div>

      {/* Table */}
      <div className="bg-card rounded border border-border overflow-x-auto">
        <table className="data-table">
          <thead>
            <tr>
              <th>STATE</th>
              <th>COMPANY</th>
              <th>FILED</th>
              <th>LAYOFF DATE</th>
              <th>EMPLOYEES</th>
              <th>LOCATION</th>
              <th>TICKER</th>
              <th>MATCH</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={8} className="text-center text-muted-foreground py-8">
                  Loading...
                </td>
              </tr>
            ) : filings.length === 0 ? (
              <tr>
                <td colSpan={8} className="text-center text-muted-foreground py-8">
                  No filings found. Run scrapers first.
                </td>
              </tr>
            ) : (
              filings.map((f) => (
                <tr key={f.id}>
                  <td>
                    <span className="bg-muted px-2 py-0.5 rounded text-xs font-semibold">
                      {f.state}
                    </span>
                  </td>
                  <td className="max-w-xs truncate">{f.company_name_raw}</td>
                  <td>{f.filing_date}</td>
                  <td className="text-muted-foreground">{f.layoff_date ?? "--"}</td>
                  <td>{f.employees_affected?.toLocaleString() ?? "--"}</td>
                  <td className="text-muted-foreground text-xs max-w-32 truncate">
                    {f.location ?? "--"}
                  </td>
                  <td>
                    {f.ticker ? (
                      <span className="text-accent font-semibold">{f.ticker}</span>
                    ) : (
                      <span className="text-muted-foreground">--</span>
                    )}
                  </td>
                  <td>
                    {f.match_score != null ? (
                      <span
                        className={`text-xs px-2 py-0.5 rounded ${
                          f.match_score >= 90
                            ? "bg-green-900/30 text-green-400"
                            : f.match_score >= 75
                            ? "bg-yellow-900/30 text-yellow-400"
                            : "bg-red-900/30 text-red-400"
                        }`}
                      >
                        {f.match_score.toFixed(0)}%
                      </span>
                    ) : (
                      "--"
                    )}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
