/**
 * Solo para PDF / impresión: columnas de clientes por día (orden_manual).
 * No importar desde componentes React.
 */

import type { DistribuidoraResumenDiaJson, DistribuidoraResumenVendedorJson } from "@/lib/api"

export type PdfClienteColumn = {
  /** Etiqueta de columna (ej. nombre del día desde el API). */
  titulo: string
  /** Líneas "1. Nombre cliente" */
  lineas: string[]
}

function stripAccents(s: string): string {
  return s.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase()
}

/** Lunes=1 … Domingo=7; sin coincidencia=99. */
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

const TITULO_DIA_DEF: Record<number, string> = {
  1: "Lunes",
  2: "Martes",
  3: "Miércoles",
  4: "Jueves",
  5: "Viernes",
  6: "Sábado",
  7: "Domingo",
}

function nombreCliente(c: Record<string, unknown>): string {
  return String(
    c.cliente_nombre ?? c.nombre_fantasia ?? c.nombre ?? c.razon_social ?? "Cliente",
  ).trim()
}

function clientesLineasOrdenados(dia: DistribuidoraResumenDiaJson): string[] {
  const raw = dia.clientes
  if (!Array.isArray(raw)) return []
  const rows = raw as Record<string, unknown>[]
  const sorted = [...rows].sort((a, b) => {
    const oa = Number(a.orden_manual ?? a.orden_visita) || 0
    const ob = Number(b.orden_manual ?? b.orden_visita) || 0
    if (oa !== ob) return oa - ob
    return nombreCliente(a).localeCompare(nombreCliente(b), "es")
  })
  return sorted.map((c, i) => {
    const ord = Number(c.orden_manual ?? c.orden_visita) || i + 1
    return `${ord}. ${nombreCliente(c)}`
  })
}

/** Una entrada por clave 1..7 (la última dia del resumen gana si hay duplicado de etiqueta). */
function diasPorClaveSemana(resumen: DistribuidoraResumenVendedorJson): Map<number, DistribuidoraResumenDiaJson> {
  const m = new Map<number, DistribuidoraResumenDiaJson>()
  for (const d of resumen.dias ?? []) {
    const k = diaSemanaSortKey(String(d.dia ?? ""))
    if (k >= 1 && k <= 7) m.set(k, d)
  }
  return m
}

function columnaParaClave(
  clave: number,
  porClave: Map<number, DistribuidoraResumenDiaJson>,
): PdfClienteColumn {
  const dia = porClave.get(clave)
  const tituloBase = TITULO_DIA_DEF[clave] ?? `Día ${clave}`
  if (!dia) {
    return { titulo: tituloBase, lineas: [] }
  }
  const titulo = String(dia.dia ?? "").trim() || tituloBase
  return { titulo, lineas: clientesLineasOrdenados(dia) }
}

/** Tres columnas: Lunes, Martes, Miércoles. */
export function buildClienteColumnsLunMie(resumen: DistribuidoraResumenVendedorJson): PdfClienteColumn[] {
  const porClave = diasPorClaveSemana(resumen)
  return [1, 2, 3].map((k) => columnaParaClave(k, porClave))
}

/** Dos columnas: Jueves, Viernes. */
export function buildClienteColumnsJueVie(resumen: DistribuidoraResumenVendedorJson): PdfClienteColumn[] {
  const porClave = diasPorClaveSemana(resumen)
  return [4, 5].map((k) => columnaParaClave(k, porClave))
}
