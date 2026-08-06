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
  const status = detail.weight?.status
  const value =
    status === "unavailable" || status === "error"
      ? null
      : (detail.weight?.value_kg ?? detail.peso_total_kg)
  return {
    weight_kg: value,
    peso_total_kg: value,
    weight: detail.weight ?? {
      value_kg: value,
      status: status ?? (value == null ? "unavailable" : "calculated"),
      source: "product_lines",
      reason: detail.weight?.reason ?? null,
    },
    productos_sin_peso: detail.productos_sin_peso,
    productos_manuales: detail.productos_manuales,
    productos_estimados: detail.productos_estimados,
    porcentaje_cobertura_peso: detail.porcentaje_cobertura,
  }
}

export type GroupWeightSummary = {
  knownKg: number
  unavailableCount: number
  partialCount: number
  incomplete: boolean
}

/** Suma solo pesos conocidos; unavailable no cuenta como 0 kg real. */
export function summarizeGroupWeights(
  rows: Array<
    Pick<DistribuidoraDispatchPrepPlanningRow, "peso_total_kg" | "weight_kg" | "weight">
  >,
): GroupWeightSummary {
  let knownKg = 0
  let unavailableCount = 0
  let partialCount = 0
  for (const row of rows) {
    const status = row.weight?.status
    if (status === "unavailable" || status === "error") {
      unavailableCount += 1
      continue
    }
    if (status === "partial") partialCount += 1
    const raw = row.weight?.value_kg ?? row.peso_total_kg ?? row.weight_kg
    if (raw == null) {
      unavailableCount += 1
      continue
    }
    const v = typeof raw === "number" ? raw : Number(raw)
    if (!Number.isFinite(v)) {
      unavailableCount += 1
      continue
    }
    knownKg += v
  }
  return {
    knownKg,
    unavailableCount,
    partialCount,
    incomplete: unavailableCount > 0 || partialCount > 0,
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
