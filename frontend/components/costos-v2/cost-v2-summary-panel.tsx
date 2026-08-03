"use client"

import { AnalyticsKpiCard } from "@/components/analytics/analytics-kpi-card"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { CostV2StatusChart } from "@/components/costos-v2/cost-v2-status-chart"
import type { CostV2SummaryBody } from "@/lib/costos-v2/types"
import { statusLabel, warningLabel } from "@/lib/costos-v2/labels"

export function CostV2SummaryPanel({
  summary,
  loading,
}: {
  summary: CostV2SummaryBody | null
  loading?: boolean
}) {
  const by = summary?.by_status ?? {}
  const warns = summary?.by_warning ?? {}
  const outlierCount = Number(warns.suspicious_outlier ?? 0)
  const incomplete = Number(by.incomplete_tax_context ?? 0)
  const missing = Number(by.missing_cost ?? 0)

  return (
    <div className="space-y-4">
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4 xl:grid-cols-7">
        <AnalyticsKpiCard
          title="Recepciones analizadas"
          value={summary?.total_rows ?? "—"}
          loading={loading}
          tooltip="Filas con cálculo V2 en el alcance filtrado"
        />
        <AnalyticsKpiCard
          title="Productos / variantes"
          value={summary?.unique_variants ?? "—"}
          loading={loading}
        />
        <AnalyticsKpiCard
          title="Documentos únicos"
          value={summary?.unique_documents ?? "—"}
          loading={loading}
        />
        <AnalyticsKpiCard
          title="Costos corregidos disponibles"
          value={summary?.with_corrected_gross ?? "—"}
          loading={loading}
          tooltip="Filas con corrected_gross_cost calculable"
        />
        <AnalyticsKpiCard
          title="Sin costo disponible"
          value={missing}
          loading={loading}
          subtitle={statusLabel("missing_cost")}
        />
        <AnalyticsKpiCard
          title="Contexto tributario incompleto"
          value={incomplete}
          loading={loading}
          subtitle={statusLabel("incomplete_tax_context")}
        />
        <AnalyticsKpiCard
          title="Alertas por outlier"
          value={outlierCount}
          loading={loading}
          subtitle={warningLabel("suspicious_outlier")}
          tooltip="Warning; no reemplaza el estado principal"
        />
      </div>

      {summary && !summary.status_sum_matches_total ? (
        <p className="text-xs text-amber-700 dark:text-amber-300">
          La suma de estados no coincide con el total de filas. Revisar filtros.
        </p>
      ) : null}

      <Card className="shadow-none">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium">Distribución por estado</CardTitle>
        </CardHeader>
        <CardContent>
          {loading ? (
            <p className="py-8 text-center text-sm text-muted-foreground">Cargando…</p>
          ) : (
            <CostV2StatusChart byStatus={summary?.by_status} />
          )}
        </CardContent>
      </Card>
    </div>
  )
}
