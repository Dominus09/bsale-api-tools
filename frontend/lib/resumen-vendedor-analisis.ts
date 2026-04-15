/**
 * Cálculo de análisis operativo para resumen vendedor (UI + PDF).
 * Sin efectos secundarios; errores capturados en `safeBuildOperationalInsights`.
 */
import type { DistribuidoraResumenVendedorJson } from "@/lib/api"

/** km totales / visitas semana. */
export function kmPorClienteSemana(resumen: DistribuidoraResumenVendedorJson | null | undefined): number {
  if (!resumen) return 0
  const n = resumen.clientes_total_semana
  if (n == null || n <= 0) return 0
  const km = Number(resumen.km_total_semana)
  if (!Number.isFinite(km)) return 0
  return km / n
}

export function clasificarEficiencia(kmPorCli: number): {
  etiqueta: "Alta" | "Media" | "Baja"
  texto: string
} {
  if (!Number.isFinite(kmPorCli) || kmPorCli <= 0) {
    return { etiqueta: "Media", texto: "sin datos de clientes" }
  }
  if (kmPorCli < 5) return { etiqueta: "Alta", texto: `${kmPorCli.toFixed(1)} km por cliente` }
  if (kmPorCli <= 10) return { etiqueta: "Media", texto: `${kmPorCli.toFixed(1)} km por cliente` }
  return { etiqueta: "Baja", texto: `${kmPorCli.toFixed(1)} km por cliente` }
}

/** Párrafos de análisis; no lanza si los datos vienen incompletos. */
export function buildOperationalInsights(
  resumen: DistribuidoraResumenVendedorJson | null | undefined,
): string[] {
  const out: string[] = []
  const dias = resumen?.dias ?? []
  const kmT = Number(resumen?.km_total_semana) || 0
  const nCli = Number(resumen?.clientes_total_semana) || 0

  if (!dias.length) {
    out.push("No hay jornadas con ruta terrestre registrada para este vendedor en el resumen actual.")
    return out
  }

  const entries = dias.map((d) => ({
    dia: String(d?.dia ?? "—"),
    km: Number(d?.km_totales) || 0,
    clis: Number(d?.clientes_count) || 0,
    kmPc: d?.km_por_cliente != null ? Number(d.km_por_cliente) : 0,
  }))

  const maxEntry = entries.reduce((a, b) => (a.km >= b.km ? a : b))
  const pctMax = kmT > 0 ? (100 * maxEntry.km) / kmT : 0
  if (pctMax >= 40 && entries.length >= 2) {
    out.push(
      `Durante la semana, la carga de kilómetros se concentra con fuerza el día ${maxEntry.dia} (aprox. ${pctMax.toFixed(
        0,
      )}% del total), lo que marca el pico operativo de la semana.`,
    )
  }

  const avgKmDia = entries.length ? kmT / entries.length : 0
  const lowDays = entries.filter((e) => e.km > 0 && avgKmDia > 0 && e.km < avgKmDia * 0.45)
  if (lowDays.length >= 1 && entries.length >= 3) {
    out.push(
      `Existen jornadas con menor recorrido (${lowDays
        .map((x) => x.dia)
        .join(", ")}), lo que puede representar oportunidades de redistribución de visitas.`,
    )
  }

  if (kmT >= 400) {
    out.push(
      "El kilometraje semanal acumulado es elevado: la ruta implica alta exigencia logística en tiempo de manejo y costo de combustible.",
    )
  } else if (kmT > 0 && kmT <= 130 && entries.length >= 2) {
    out.push(
      "El kilometraje total es moderado: podría existir margen para densificar visitas en algunas jornadas sin saturar la semana.",
    )
  }

  const kmPerClienteGlobal = nCli > 0 ? kmT / nCli : 0
  if (kmPerClienteGlobal >= 12) {
    out.push(
      "El promedio de kilómetros por cliente sugiere puntos de atención relativamente dispersos, con impacto directo en tiempos de traslado.",
    )
  } else if (kmPerClienteGlobal > 0 && kmPerClienteGlobal <= 4.5) {
    out.push(
      "El promedio de kilómetros por cliente es favorable: la geografía de visitas tiende a ser compacta entre paradas consecutivas.",
    )
  }

  const kms = entries.map((e) => e.km)
  const mean = kms.reduce((s, x) => s + x, 0) / Math.max(kms.length, 1)
  const stdev = Math.sqrt(kms.reduce((s, x) => s + (x - mean) ** 2, 0) / Math.max(kms.length, 1))
  if (mean > 0 && stdev / mean > 0.38 && entries.length >= 3) {
    out.push(
      "Hay desbalance entre jornadas (variación notable en km por día); conviene revisar la asignación semanal frente al mix de clientes y prioridades.",
    )
  }

  if (entries.some((e) => e.clis > 0 && e.km / e.clis > 18)) {
    out.push(
      "Algunos días muestran ratios altos de km por cliente, típicos de trayectos largos o secuencias poco compactas.",
    )
  }

  out.push(
    "Se sugiere revisar periódicamente la secuencia de visitas y el equilibrio entre días para optimizar costos y tiempos, alineando la operación con metas comerciales y de servicio.",
  )

  return out
}

export type SafeAnalisisResult = {
  ok: boolean
  paragraphs: string[]
  /** Solo si ok === false */
  message?: string
}

/**
 * Envoltura defensiva: nunca lanza; útil en render React.
 */
export function safeBuildOperationalInsights(
  resumen: DistribuidoraResumenVendedorJson | null | undefined,
): SafeAnalisisResult {
  try {
    const paragraphs = buildOperationalInsights(resumen)
    return { ok: true, paragraphs: Array.isArray(paragraphs) ? paragraphs : [] }
  } catch (e) {
    if (process.env.NODE_ENV === "development") {
      console.error("[resumen-vendedor-analisis] safeBuildOperationalInsights", e)
    }
    return {
      ok: false,
      paragraphs: [],
      message: "No fue posible generar el análisis automático para este vendedor.",
    }
  }
}

export function safeClasificarEficiencia(
  resumen: DistribuidoraResumenVendedorJson | null | undefined,
): { etiqueta: string; texto: string } {
  try {
    return clasificarEficiencia(kmPorClienteSemana(resumen))
  } catch {
    return { etiqueta: "—", texto: "Sin datos" }
  }
}
