import type { DistribuidoraResumenDiaJson, DistribuidoraResumenVendedorJson } from "@/lib/api"

export type ConsolidatedClientePdfRow = {
  ordenGlobal: number
  ordenManual: number
  nombre: string
  dia: string
  comuna: string
  tipo: string
}

function stripAccents(s: string): string {
  return s.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase()
}

/** Lunes=1 … Domingo=7; sin coincidencia=99 (queda al final). */
export function diaSemanaSortKey(label: string): number {
  const s = stripAccents(String(label || "").trim())
  const pairs: [RegExp, number][] = [
    [/^lunes/, 1],
    [/^martes/, 2],
    [/^miercoles|^mie/, 3],
    [/^jueves/, 4],
    [/^viernes/, 5],
    [/^sabado/, 6],
    [/^domingo/, 7],
  ]
  for (const [re, n] of pairs) {
    if (re.test(s)) return n
  }
  return 99
}

function clientesOrdenadosPorDia(dia: DistribuidoraResumenDiaJson): Record<string, unknown>[] {
  const raw = dia.clientes
  if (!Array.isArray(raw)) return []
  const rows = raw as Record<string, unknown>[]
  return [...rows].sort((a, b) => {
    const oa = Number(a.orden_manual ?? a.orden_visita) || 0
    const ob = Number(b.orden_manual ?? b.orden_visita) || 0
    return oa - ob
  })
}

function nombreCliente(c: Record<string, unknown>): string {
  return String(
    c.cliente_nombre ?? c.nombre_fantasia ?? c.nombre ?? c.razon_social ?? "Cliente",
  ).trim()
}

function tipoAtencionLabel(c: Record<string, unknown>): string {
  const tipoRaw = String(c.tipo_atencion ?? "").toLowerCase()
  return tipoRaw.includes("telefon") ? "Telefónico" : "Terreno"
}

/**
 * Una fila por visita programada: orden global, día (Lun→Dom), orden manual dentro del día.
 */
/** Líneas tipo "Lunes: 120 km — 28 clientes" con marcas de carga (sin símbolos especiales). */
export function buildDiasCargaSummaryLines(resumen: DistribuidoraResumenVendedorJson): string[] {
  const dias = [...(resumen.dias ?? [])].sort((a, b) => {
    const ka = diaSemanaSortKey(String(a.dia ?? ""))
    const kb = diaSemanaSortKey(String(b.dia ?? ""))
    if (ka !== kb) return ka - kb
    return String(a.dia ?? "").localeCompare(String(b.dia ?? ""), "es")
  })
  const kms = dias.map((d) => Number(d.km_totales) || 0).filter((k) => k > 0)
  const maxKm = kms.length ? Math.max(...kms) : 0
  const minKm = kms.length ? Math.min(...kms) : 0
  return dias.map((d) => {
    const km = Number(d.km_totales) || 0
    const n = Number(d.clientes_count) || 0
    let tag = ""
    if (km > 0 && maxKm > 0 && km === maxKm && maxKm !== minKm) tag = " [Mayor carga]"
    if (km > 0 && minKm >= 0 && km === minKm && maxKm !== minKm) tag = " [Menor recorrido]"
    return `${String(d.dia ?? "-")}: ${km} km — ${n} clientes${tag}`
  })
}

export function buildConsolidatedSemanaClientRows(
  resumen: DistribuidoraResumenVendedorJson,
): ConsolidatedClientePdfRow[] {
  const dias = [...(resumen.dias ?? [])].sort((a, b) => {
    const ka = diaSemanaSortKey(String(a.dia ?? ""))
    const kb = diaSemanaSortKey(String(b.dia ?? ""))
    if (ka !== kb) return ka - kb
    return String(a.dia ?? "").localeCompare(String(b.dia ?? ""), "es")
  })

  const out: ConsolidatedClientePdfRow[] = []
  let ordenGlobal = 0
  for (const dia of dias) {
    const diaLabel = String(dia.dia ?? "—").trim() || "—"
    const rows = clientesOrdenadosPorDia(dia)
    for (const c of rows) {
      ordenGlobal += 1
      const ordenManual = Number(c.orden_manual ?? c.orden_visita) || 0
      const comuna = String(c.municipality ?? c.comuna ?? "").trim() || "—"
      out.push({
        ordenGlobal,
        ordenManual,
        nombre: nombreCliente(c),
        dia: diaLabel,
        comuna,
        tipo: tipoAtencionLabel(c),
      })
    }
  }
  return out
}
