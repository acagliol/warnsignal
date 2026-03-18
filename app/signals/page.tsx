"use client";

import { useEffect, useState } from "react";
import { getSignals, getEventStudy, type Signal, type EventStudy } from "@/lib/api";
import { formatPct, formatNum, pctColor } from "@/lib/utils";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ReferenceLine,
  ResponsiveContainer,
} from "recharts";

export default function SignalsPage() {
  const [signals, setSignals] = useState<Signal[]>([]);
  const [loading, setLoading] = useState(true);
  const [expandedId, setExpandedId] = useState<number | null>(null);
  const [eventStudy, setEventStudy] = useState<EventStudy | null>(null);

  useEffect(() => {
    getSignals({ limit: 100 })
      .then((data) => setSignals(data.signals))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const toggleExpand = async (signal: Signal) => {
    if (expandedId === signal.id) {
      setExpandedId(null);
      setEventStudy(null);
      return;
    }
    setExpandedId(signal.id);
    try {
      const es = await getEventStudy(signal.id);
      setEventStudy(es);
    } catch {
      setEventStudy(null);
    }
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-lg font-bold tracking-wider">SHORT SIGNALS</h1>
        <span className="text-xs text-muted-foreground">
          Ranked by composite distress score
        </span>
      </div>

      <div className="bg-card rounded border border-border overflow-x-auto">
        <table className="data-table">
          <thead>
            <tr>
              <th>TICKER</th>
              <th>DATE</th>
              <th>EMPLOYEES</th>
              <th>% OF TOTAL</th>
              <th>LEAD DAYS</th>
              <th>REPEAT</th>
              <th>SECTOR</th>
              <th>CAP</th>
              <th>SCORE</th>
              <th>CAR [0,+30]</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={10} className="text-center text-muted-foreground py-8">
                  Loading...
                </td>
              </tr>
            ) : signals.length === 0 ? (
              <tr>
                <td colSpan={10} className="text-center text-muted-foreground py-8">
                  No signals yet. Run the full pipeline first.
                </td>
              </tr>
            ) : (
              signals.map((s) => (
                <>
                  <tr
                    key={s.id}
                    onClick={() => toggleExpand(s)}
                    className="cursor-pointer"
                  >
                    <td className="font-semibold text-accent">{s.ticker}</td>
                    <td>{s.signal_date}</td>
                    <td>{s.employees_affected?.toLocaleString() ?? "--"}</td>
                    <td>{s.employees_pct != null ? `${s.employees_pct.toFixed(1)}%` : "--"}</td>
                    <td>{s.filing_lead_days ?? "--"}</td>
                    <td>
                      {s.repeat_filer && (
                        <span className="bg-destructive/20 text-destructive px-2 py-0.5 rounded text-xs">
                          YES
                        </span>
                      )}
                    </td>
                    <td className="text-xs">{s.sector ?? "--"}</td>
                    <td className="text-xs">{s.market_cap_bucket ?? "--"}</td>
                    <td>
                      <span className="bg-primary/20 text-primary px-2 py-0.5 rounded text-xs font-bold">
                        {(s.composite_score * 100).toFixed(0)}
                      </span>
                    </td>
                    <td className={pctColor(s.car_post30)}>
                      {formatPct(s.car_post30)}
                    </td>
                  </tr>

                  {expandedId === s.id && eventStudy && (
                    <tr key={`${s.id}-expand`}>
                      <td colSpan={10} className="bg-muted/30 p-4">
                        <div className="grid grid-cols-4 gap-4 mb-4 text-sm">
                          <div>
                            <span className="text-muted-foreground">CAR [-30,0]:</span>{" "}
                            <span className={pctColor(eventStudy.car_pre30)}>
                              {formatPct(eventStudy.car_pre30)}
                            </span>
                          </div>
                          <div>
                            <span className="text-muted-foreground">CAR [0,+30]:</span>{" "}
                            <span className={pctColor(eventStudy.car_post30)}>
                              {formatPct(eventStudy.car_post30)}
                            </span>
                          </div>
                          <div>
                            <span className="text-muted-foreground">CAR [0,+60]:</span>{" "}
                            <span className={pctColor(eventStudy.car_post60)}>
                              {formatPct(eventStudy.car_post60)}
                            </span>
                          </div>
                          <div>
                            <span className="text-muted-foreground">CAR [0,+90]:</span>{" "}
                            <span className={pctColor(eventStudy.car_post90)}>
                              {formatPct(eventStudy.car_post90)}
                            </span>
                          </div>
                        </div>

                        {eventStudy.car_timeseries.length > 0 && (
                          <ResponsiveContainer width="100%" height={250}>
                            <LineChart data={eventStudy.car_timeseries}>
                              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                              <XAxis
                                dataKey="day"
                                stroke="#64748b"
                                fontSize={11}
                                label={{ value: "Days", position: "bottom", offset: -5 }}
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
                                formatter={(v: number) => [`${(v * 100).toFixed(2)}%`, "CAR"]}
                              />
                              <ReferenceLine x={0} stroke="#94a3b8" strokeDasharray="3 3" />
                              <ReferenceLine y={0} stroke="#94a3b8" strokeDasharray="3 3" />
                              <Line
                                type="monotone"
                                dataKey="car"
                                stroke="#ef4444"
                                strokeWidth={2}
                                dot={false}
                              />
                            </LineChart>
                          </ResponsiveContainer>
                        )}
                      </td>
                    </tr>
                  )}
                </>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
