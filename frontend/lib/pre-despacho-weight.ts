import type { DistribuidoraDispatchPrepPlanningRow, OrderWeightDetail } from "@/lib/api"

export type PreDespachoWeightBadgeKind = "incomplete" | "manual" | "complete"

export type PreDespachoWeightBadge = {
  kind: PreDespachoWeightBadgeKind
  label: string
  count?: number
}

export function resolvePreDespachoWeightBadge(
  row: Pick<
    DistribuidoraDispatchPrepPlanningRow,
    "productos_sin_peso" | "productos_manuales" | "porcentaje_cobertura_peso"
  >,
): PreDespachoWeightBadge | null {
  const sinPeso = Number(row.productos_sin_peso ?? 0)
  const manual = Number(row.productos_manuales ?? 0)
  const coverage = Number(row.porcentaje_cobertura_peso ?? 0)

  if (sinPeso > 0) {
    return { kind: "incomplete", label: `Peso incompleto (${sinPeso})`, count: sinPeso }
  }
  if (manual > 0) {
    return { kind: "manual", label: "Peso manual", count: manual }
  }
  if (coverage >= 100) {
    return { kind: "complete", label: "Peso completo" }
  }
  return null
}

export function weightBadgeClass(kind: PreDespachoWeightBadgeKind): string {
  switch (kind) {
    case "incomplete":
      return "border-amber-300 bg-amber-50 text-amber-900 dark:border-amber-800 dark:bg-amber-950/40 dark:text-amber-100"
    case "manual":
      return "border-yellow-400/60 bg-yellow-50 text-yellow-900 dark:border-yellow-800 dark:bg-yellow-950/40 dark:text-yellow-100"
    case "complete":
      return "border-emerald-300/60 bg-emerald-50 text-emerald-900 dark:border-emerald-800 dark:bg-emerald-950/40 dark:text-emerald-100"
  }
}

export function weightBadgeEmoji(kind: PreDespachoWeightBadgeKind): string {
  switch (kind) {
    case "incomplete":
      return "⚠"
    case "manual":
      return "🟡"
    case "complete":
      return "🟢"
  }
}

export function orderWeightToPlanningPatch(
  detail: OrderWeightDetail,
): Partial<DistribuidoraDispatchPrepPlanningRow> {
  return {
    weight_kg: detail.peso_total_kg,
    peso_total_kg: detail.peso_total_kg,
    productos_sin_peso: detail.productos_sin_peso,
    productos_manuales: detail.productos_manuales,
    productos_estimados: detail.productos_estimados,
    porcentaje_cobertura_peso: detail.porcentaje_cobertura,
  }
}

export function logisticsPatchFromUnitKg(
  unitKg: number,
  unitsPerBox: number | null | undefined,
): { units_per_box?: number; weight_box_kg: number } {
  const upb = unitsPerBox != null && unitsPerBox > 0 ? unitsPerBox : 1
  return {
    units_per_box: unitsPerBox != null && unitsPerBox > 0 ? unitsPerBox : undefined,
    weight_box_kg: Math.round(unitKg * upb * 10000) / 10000,
  }
}

export function formatFuentePeso(fuente: string | null | undefined): string {
  const f = (fuente || "").trim().toLowerCase()
  if (f === "erp" || f === "maestro") return "ERP"
  if (f === "manual") return "Manual"
  if (f === "estimado") return "Estimado"
  if (f === "sin_datos") return "Sin datos"
  return fuente || "—"
}
