import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function formatPct(val: number | null | undefined): string {
  if (val == null) return "N/A";
  return `${(val * 100).toFixed(2)}%`;
}

export function formatNum(val: number | null | undefined, decimals = 2): string {
  if (val == null) return "N/A";
  return val.toFixed(decimals);
}

export function pctColor(val: number | null | undefined): string {
  if (val == null) return "";
  return val < 0 ? "text-negative" : "text-positive";
}
