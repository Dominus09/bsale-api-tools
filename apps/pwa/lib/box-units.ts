/**
 * Equivalencia cajas/unidades solo con factor estructurado confiable.
 * No parsear SEC del nombre. Sin decimales de caja: cajas enteras + sueltas.
 */

export type BoxUnitsBreakdown = {
  units: number
  unitsPerBox: number | null
  fullBoxes: number | null
  looseUnits: number | null
  label: string | null
  pillLabel: string | null
  available: boolean
}

export function breakdownBoxUnits(
  units: number | null | undefined,
  unitsPerBox: number | null | undefined,
  opts?: { sinUnidadCaja?: boolean; equivalenceStatus?: string | null },
): BoxUnitsBreakdown {
  const u = typeof units === "number" && Number.isFinite(units) ? units : 0
  const blocked = Boolean(opts?.sinUnidadCaja)
  // Factor estructurado válido basta; no exigir equivalence_status=verified.
  const upb =
    typeof unitsPerBox === "number" && Number.isFinite(unitsPerBox) && unitsPerBox > 0
      ? unitsPerBox
      : null

  if (blocked || upb == null) {
    return {
      units: u,
      unitsPerBox: null,
      fullBoxes: null,
      looseUnits: null,
      label: null,
      pillLabel: null,
      available: false,
    }
  }

  const fullBoxes = Math.floor(u / upb)
  const looseUnits = Math.round((u - fullBoxes * upb) * 1000) / 1000
  let label: string
  let pillLabel: string
  if (looseUnits > 0 && fullBoxes > 0) {
    label = `${u} un = ${fullBoxes} cajas + ${looseUnits} un`
    pillLabel = `${u} un = ${fullBoxes} cajas + ${looseUnits}`
  } else if (looseUnits > 0) {
    label = `${u} un`
    pillLabel = `${u} un`
  } else {
    label = `${u} un = ${fullBoxes} cajas`
    pillLabel = `${u} un = ${fullBoxes} cajas`
  }

  return {
    units: u,
    unitsPerBox: upb,
    fullBoxes,
    looseUnits,
    label,
    pillLabel,
    available: true,
  }
}
