"use client"

import { useEffect, useState } from "react"
import { Loader2 } from "lucide-react"

import { cn } from "@/lib/utils"
import { getPolicyReview } from "@/lib/analytics-v2/api"
import { formatPct } from "@/lib/analytics-v2/labels"
import { LIST_SCOPE_LABELS, RULE_ISSUE_LABELS } from "@/lib/analytics-v2/margins-labels"
import type { PolicyReviewResponse } from "@/lib/analytics-v2/margins-types"

export function MarginV2PolicyReview({ companyId }: { companyId: number }) {
  const [onlyIssues, setOnlyIssues] = useState(true)
  const [inScopeOnly, setInScopeOnly] = useState(true)
  const [data, setData] = useState<PolicyReviewResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const ac = new AbortController()
    setLoading(true)
    setError(null)
    getPolicyReview(companyId, onlyIssues, ac.signal)
      .then(setData)
      .catch((e: unknown) => {
        if (!ac.signal.aborted) setError(e instanceof Error ? e.message : "Error al cargar la revisión de políticas")
      })
      .finally(() => {
        if (!ac.signal.aborted) setLoading(false)
      })
    return () => ac.abort()
  }, [companyId, onlyIssues])

  const rules = (data?.rules ?? []).filter((r) => !inScopeOnly || r.in_commercial_scope)
  const p = data?.policy

  return (
    <div className="space-y-3">
      <p className="rounded-md border border-violet-300 bg-violet-50 px-3 py-2 text-xs text-violet-900">
        Las {p?.rules_total ?? "—"} reglas actuales se conservan sin cambios y no son la política definitiva: se
        reemplazarán con el Excel de gerencia mediante una importación controlada. Hasta entonces todos los resultados de
        margen son provisionales. Las reglas de listas retiradas se mantienen como historia y no generan alertas.
      </p>

      {p ? (
        <div className="grid grid-cols-2 gap-2 text-sm md:grid-cols-5">
          {[
            ["En listas evaluadas", p.rules_in_scope],
            ["Pendientes de revisión", p.rules_pending_review],
            ["Máximo 0", p.rules_max_zero_in_scope],
            ["Combinaciones sin regla", p.missing_rule_combinations],
            ["Históricas (fuera de alcance)", p.rules_out_of_scope],
          ].map(([label, value]) => (
            <div key={label as string} className="rounded-md border px-3 py-2">
              <p className="text-[11px] uppercase tracking-wide text-muted-foreground">{label}</p>
              <p className="text-xl font-semibold tabular-nums">{value}</p>
            </div>
          ))}
        </div>
      ) : null}

      {error ? <p className="text-sm text-destructive">{error}</p> : null}

      {data && data.missing_rules.length ? (
        <div className="rounded-md border">
          <p className="border-b bg-muted/40 px-3 py-2 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
            Combinaciones lista × tipo sin regla (con SKUs activos con precio)
          </p>
          <ul className="divide-y text-sm">
            {data.missing_rules.map((m) => (
              <li key={`${m.price_list_id}-${m.product_type_id}`} className="flex justify-between px-3 py-1.5">
                <span>
                  {m.price_list_name ?? `Lista ${m.price_list_id}`} · {m.product_type_name ?? "Sin tipo de producto"}
                </span>
                <span className="tabular-nums text-muted-foreground">{m.priced_active_skus} SKUs</span>
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      <div className="flex flex-wrap gap-4 text-sm">
        <label className="flex items-center gap-2">
          <input type="checkbox" checked={onlyIssues} onChange={(e) => setOnlyIssues(e.target.checked)} />
          Solo pendientes o con observaciones
        </label>
        <label className="flex items-center gap-2">
          <input type="checkbox" checked={inScopeOnly} onChange={(e) => setInScopeOnly(e.target.checked)} />
          Solo listas evaluadas
        </label>
        {loading ? <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" /> : null}
      </div>

      <div className={cn("overflow-x-auto rounded-md border", loading && "opacity-60")}>
        <table className="w-full min-w-[900px] text-sm">
          <thead className="bg-muted/40 text-left text-[11px] uppercase tracking-wide text-muted-foreground">
            <tr>
              <th className="px-3 py-2 font-medium">Regla</th>
              <th className="px-3 py-2 font-medium">Lista</th>
              <th className="px-3 py-2 font-medium">Tipo de producto</th>
              <th className="px-3 py-2 text-right font-medium">Mín</th>
              <th className="px-3 py-2 text-right font-medium">Máx</th>
              <th className="px-3 py-2 font-medium">Revisión</th>
              <th className="px-3 py-2 font-medium">Observaciones</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border/60">
            {rules.map((r) => (
              <tr key={r.rule_id} className={cn(!r.in_commercial_scope && "text-muted-foreground")}>
                <td className="px-3 py-1.5 font-mono text-xs">{r.rule_key ?? r.rule_id}</td>
                <td className="px-3 py-1.5">
                  {r.price_list_name ?? r.price_list_id}
                  <span className="block text-[10px]">{LIST_SCOPE_LABELS[r.price_list_scope]}</span>
                </td>
                <td className="px-3 py-1.5">{r.product_type_name ?? r.product_type_id ?? "General"}</td>
                <td className="px-3 py-1.5 text-right tabular-nums">{formatPct(r.min_markup_pct)}</td>
                <td className="px-3 py-1.5 text-right tabular-nums">{formatPct(r.max_markup_pct)}</td>
                <td className="px-3 py-1.5 text-xs">{r.review_status === "validada" ? "Validada" : "Pendiente (Excel)"}</td>
                <td className="px-3 py-1.5 text-xs">
                  {r.issues.length ? r.issues.map((i) => RULE_ISSUE_LABELS[i]).join(" · ") : "—"}
                  {!r.alerts_enabled ? <span className="block text-[10px]">Sin alertas (fuera de alcance)</span> : null}
                </td>
              </tr>
            ))}
            {data && rules.length === 0 ? (
              <tr>
                <td colSpan={7} className="px-3 py-6 text-center text-muted-foreground">
                  Sin reglas para mostrar.
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </div>
  )
}
