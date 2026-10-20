"use client"

import { cn } from "@/lib/utils"
import {
  ANOMALY_LABELS,
  MOVEMENT_LABELS,
  RELIABILITY_LABELS,
  RELIABILITY_TONES,
  SEVERITY_TONES,
} from "@/lib/analytics-v2/labels"
import type { Anomaly, CostReliability, MovementClass } from "@/lib/analytics-v2/types"

const base = "inline-flex items-center whitespace-nowrap rounded-md border px-1.5 py-0.5 text-[11px] font-medium"

export function ReliabilityBadge({ status, title }: { status: CostReliability; title?: string }) {
  return (
    <span className={cn(base, RELIABILITY_TONES[status])} title={title}>
      {RELIABILITY_LABELS[status]}
    </span>
  )
}

export function MovementBadge({ movement }: { movement: MovementClass }) {
  const purchase = movement === "factura" || movement === "guia_despacho"
  return (
    <span
      className={cn(
        base,
        purchase ? "border-border bg-background text-foreground" : "border-violet-300 bg-violet-50 text-violet-800 dark:border-violet-400/25 dark:bg-violet-400/10 dark:text-violet-200",
      )}
    >
      {MOVEMENT_LABELS[movement]}
    </span>
  )
}

export function AnomalyChips({ anomalies, max = 3 }: { anomalies: Anomaly[]; max?: number }) {
  if (!anomalies.length) return <span className="text-xs text-muted-foreground">—</span>
  const shown = anomalies.slice(0, max)
  return (
    <div className="flex flex-wrap gap-1">
      {shown.map((a) => (
        <span key={a.code} className={cn(base, SEVERITY_TONES[a.severity])} title={a.message}>
          {ANOMALY_LABELS[a.code]}
        </span>
      ))}
      {anomalies.length > max ? (
        <span className="text-[11px] text-muted-foreground">+{anomalies.length - max}</span>
      ) : null}
    </div>
  )
}

export function AnomalyList({ anomalies }: { anomalies: Anomaly[] }) {
  if (!anomalies.length) {
    return <p className="text-sm text-muted-foreground">Sin anomalías en la última compra comparable.</p>
  }
  return (
    <ul className="space-y-1.5">
      {anomalies.map((a) => (
        <li key={a.code} className="flex items-start gap-2 text-sm">
          <span className={cn(base, "mt-0.5", SEVERITY_TONES[a.severity])}>{ANOMALY_LABELS[a.code]}</span>
          <span className="text-muted-foreground">{a.message}</span>
        </li>
      ))}
    </ul>
  )
}
