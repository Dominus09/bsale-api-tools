"use client"

import { useEffect, useState } from "react"
import { Loader2 } from "lucide-react"

import { ProfessionalDataTable, SectionCard, SmartFilterBar, StatusBadge } from "@/components/analytics-v2/shared/analytics-ui"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { cn } from "@/lib/utils"
import { getCostAlerts } from "@/lib/analytics-v2/api"
import {
  ANOMALY_LABELS,
  SEVERITY_LABELS,
  SEVERITY_TONES,
  formatDate,
  formatMoney,
  formatPct,
  formatQty,
} from "@/lib/analytics-v2/labels"
import type { AlertsResponse, AnomalyCode, CostFilters, Severity } from "@/lib/analytics-v2/types"

import { AnomalyChips, MovementBadge } from "./badges"
import { Pager } from "./cost-v2-products"

function isoDaysAgo(days: number): string {
  const d = new Date()
  d.setDate(d.getDate() - days)
  return d.toISOString().slice(0, 10)
}

const selectClass = "h-9 rounded-md border border-input bg-background px-2 text-sm"

const reviewAction: Partial<Record<AnomalyCode, string>> = {
  alza_anormal: "Comparar con la compra anterior y revisar el documento.",
  baja_sospechosa: "Confirmar unidades y costo informado en la recepción.",
  costo_cero: "Revisar el documento: el costo informado es cero.",
  costo_nulo: "Revisar el documento: no se recibió un costo utilizable.",
  cantidad_extrema: "Verificar las unidades registradas en la recepción.",
  cantidad_no_positiva: "Corregir la cantidad en el sistema de origen.",
  movimiento_no_determinado: "Revisar el tipo de documento y su origen.",
}

export function CostV2Alerts({
  companyId,
  filters,
  onOpenProduct,
  onOpenReception,
}: {
  companyId: number
  filters: CostFilters | null
  onOpenProduct: (variantId: number) => void
  onOpenReception: (receptionId: number, detailId: number) => void
}) {
  const [draft, setDraft] = useState({
    dateFrom: isoDaysAgo(90),
    dateTo: isoDaysAgo(0),
    code: "" as AnomalyCode | "",
    severity: "" as Severity | "",
    officeId: "",
  })
  const [applied, setApplied] = useState(draft)
  const [page, setPage] = useState(1)
  const [data, setData] = useState<AlertsResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const ac = new AbortController()
    setLoading(true)
    setError(null)
    getCostAlerts(
      {
        company_id: companyId,
        date_from: applied.dateFrom,
        date_to: applied.dateTo,
        anomaly_code: applied.code || null,
        severity: applied.severity || null,
        office_id: applied.officeId ? Number(applied.officeId) : null,
        page,
        page_size: 50,
      },
      ac.signal,
    )
      .then(setData)
      .catch((e: unknown) => {
        if (!ac.signal.aborted) setError(e instanceof Error ? e.message : "Error al cargar alertas")
      })
      .finally(() => {
        if (!ac.signal.aborted) setLoading(false)
      })
    return () => ac.abort()
  }, [companyId, applied, page])

  const counts = data?.counts_by_code ?? {}
  const countKeys = (Object.keys(counts) as AnomalyCode[]).filter((k) => (counts[k] ?? 0) > 0)

  return (
    <div className="space-y-3">
      <form
        onSubmit={(e) => {
          e.preventDefault()
          setPage(1)
          setApplied({ ...draft })
        }}
      >
        <SmartFilterBar>
        <div className="flex flex-wrap items-end gap-3">
        <label className="flex flex-col gap-1 text-[11px] font-medium uppercase text-muted-foreground">
          Desde
          <Input
            type="date"
            min="2025-01-01"
            value={draft.dateFrom}
            onChange={(e) => setDraft((d) => ({ ...d, dateFrom: e.target.value }))}
          />
        </label>
        <label className="flex flex-col gap-1 text-[11px] font-medium uppercase text-muted-foreground">
          Hasta
          <Input
            type="date"
            value={draft.dateTo}
            onChange={(e) => setDraft((d) => ({ ...d, dateTo: e.target.value }))}
          />
        </label>
        <label className="flex flex-col gap-1 text-[11px] font-medium uppercase text-muted-foreground">
          Alerta
          <select
            className={selectClass}
            value={draft.code}
            onChange={(e) => setDraft((d) => ({ ...d, code: e.target.value as AnomalyCode | "" }))}
          >
            <option value="">Todas</option>
            {(Object.keys(ANOMALY_LABELS) as AnomalyCode[])
              .filter((k) => k !== "diferencia_promedio_bsale")
              .map((k) => (
                <option key={k} value={k}>
                  {ANOMALY_LABELS[k]}
                </option>
              ))}
          </select>
        </label>
        <label className="flex flex-col gap-1 text-[11px] font-medium uppercase text-muted-foreground">
          Severidad
          <select
            className={selectClass}
            value={draft.severity}
            onChange={(e) => setDraft((d) => ({ ...d, severity: e.target.value as Severity | "" }))}
          >
            <option value="">Todas</option>
            {(Object.keys(SEVERITY_LABELS) as Severity[]).map((k) => (
              <option key={k} value={k}>
                {SEVERITY_LABELS[k]}
              </option>
            ))}
          </select>
        </label>
        <label className="flex flex-col gap-1 text-[11px] font-medium uppercase text-muted-foreground">
          Sucursal
          <select
            className={selectClass}
            value={draft.officeId}
            onChange={(e) => setDraft((d) => ({ ...d, officeId: e.target.value }))}
          >
            <option value="">Todas</option>
            {(filters?.offices ?? []).map((o) => (
              <option key={o.id} value={o.id}>
                {o.name ?? `Oficina ${o.id}`}
              </option>
            ))}
          </select>
        </label>
        <Button type="submit" disabled={loading || !draft.dateFrom || !draft.dateTo || draft.dateFrom > draft.dateTo}>
          {loading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
          Aplicar
        </Button>
        </div>
        </SmartFilterBar>
      </form>

      {countKeys.length ? (
        <SectionCard title="Prioridades del período" description="Cantidad de alertas por causa; cada una conserva el vínculo a su recepción original.">
        <div className="flex flex-wrap gap-2">
          {countKeys.map((k) => (
            <StatusBadge key={k} tone="warning">
              {ANOMALY_LABELS[k]}: <strong className="tabular-nums">{counts[k]}</strong>
            </StatusBadge>
          ))}
        </div>
        </SectionCard>
      ) : null}

      {error ? <p className="text-sm text-destructive">{error}</p> : null}

      <ProfessionalDataTable loading={loading}>
        <div className="overflow-x-auto">
        <table className="w-full min-w-[1000px] text-sm">
          <thead className="bg-muted/40 text-left text-[11px] uppercase tracking-wide text-muted-foreground">
            <tr>
              <th className="px-3 py-2 font-medium">Severidad</th>
              <th className="px-3 py-2 font-medium">Producto</th>
              <th className="px-3 py-2 font-medium">Recepción Bsale</th>
              <th className="px-3 py-2 font-medium">Movimiento</th>
              <th className="px-3 py-2 text-right font-medium">Cant.</th>
              <th className="px-3 py-2 text-right font-medium">Costo</th>
              <th className="px-3 py-2 text-right font-medium">Anterior</th>
              <th className="px-3 py-2 text-right font-medium">Var.</th>
              <th className="px-3 py-2 font-medium">Motivo</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border/60">
            {(data?.items ?? []).map((a) => (
              <tr key={`${a.reception_id}-${a.detail_id}`} className="align-top">
                <td className="px-3 py-2">
                  <span className={cn("rounded-md border px-1.5 py-0.5 text-[11px] font-medium", SEVERITY_TONES[a.max_severity])}>
                    {SEVERITY_LABELS[a.max_severity]}
                  </span>
                </td>
                <td className="max-w-[240px] px-3 py-2">
                  {a.variant_id != null ? (
                    <button
                      type="button"
                      className="truncate text-left font-medium hover:underline"
                      onClick={() => onOpenProduct(a.variant_id as number)}
                    >
                      {a.product_name ?? `Variante ${a.variant_id}`}
                    </button>
                  ) : (
                    <span>Variante desconocida</span>
                  )}
                  <span className="block truncate text-[11px] text-muted-foreground">{a.code ?? "—"}</span>
                </td>
                <td className="px-3 py-2">
                  <button
                    type="button"
                    className="text-left text-primary hover:underline"
                    onClick={() => onOpenReception(a.reception_id, a.detail_id)}
                  >
                    #{a.reception_id} · línea {a.detail_id}
                  </button>
                  <span className="block text-[11px] text-muted-foreground">
                    {formatDate(a.admission_date)} · {a.office_name ?? a.office_id ?? "—"}
                  </span>
                  <span className="block max-w-[200px] truncate text-[11px] text-muted-foreground">
                    {a.document ?? "Sin etiqueta"} {a.document_number ? `N° ${a.document_number}` : ""}
                  </span>
                </td>
                <td className="px-3 py-2">
                  <MovementBadge movement={a.movement_class} />
                </td>
                <td className="px-3 py-2 text-right tabular-nums">{formatQty(a.quantity)}</td>
                <td className="px-3 py-2 text-right tabular-nums">{formatMoney(a.cost)}</td>
                <td className="px-3 py-2 text-right tabular-nums">{formatMoney(a.previous_comparable_cost)}</td>
                <td className="px-3 py-2 text-right tabular-nums">{formatPct(a.change_pct)}</td>
                <td className="px-3 py-2">
                  <AnomalyChips anomalies={a.anomalies} max={3} />
                  <p className="mt-1 max-w-[260px] text-xs leading-5 text-lq-muted">
                    {a.anomalies[0] ? reviewAction[a.anomalies[0].code] ?? a.anomalies[0].message : "Revisar la recepción original."}
                  </p>
                </td>
              </tr>
            ))}
            {data && data.items.length === 0 ? (
              <tr>
                <td colSpan={9} className="px-3 py-8 text-center text-muted-foreground">
                  Sin alertas en el período.
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
        </div>
      </ProfessionalDataTable>
      {data ? (
        <Pager page={data.page} totalPages={data.total_pages} total={data.total} loading={loading} onPage={setPage} />
      ) : null}
      <p className="text-[11px] text-muted-foreground">
        Las alertas no modifican costos. Cada una identifica la recepción y línea originales de Bsale para su revisión
        administrativa.
      </p>
    </div>
  )
}
