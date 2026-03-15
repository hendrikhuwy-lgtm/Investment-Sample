import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function formatCurrency(value: string | number) {
  const num = typeof value === "string" ? parseFloat(value.replace(/[^0-9.-]+/g,"")) : value;
  if (isNaN(num)) return value;
  return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 }).format(num);
}

export function formatPercent(value: string | number) {
  const num = typeof value === "string" ? parseFloat(value.replace(/[^0-9.-]+/g,"")) : value;
  if (isNaN(num)) return value;
  return new Intl.NumberFormat('en-US', { style: 'percent', maximumFractionDigits: 2 }).format(num / 100);
}
