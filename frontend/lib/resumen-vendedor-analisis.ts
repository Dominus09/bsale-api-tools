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
    return { etiqueta: "Media", texto: "No hay suficientes datos para comparar." }
  }
  if (kmPorCli < 5) {
    return {
      etiqueta: "Alta",
      texto: `En promedio recorre ${kmPorCli.toFixed(1)} km por cada cliente visitado (recorrido concentrado).`,
    }
  }
  if (kmPorCli <= 10) {
    return {
      etiqueta: "Media",
      texto: `En promedio recorre ${kmPorCli.toFixed(1)} km por cliente; hay margen razonable de optimización.`,
    }
  }
  return {
    etiqueta: "Baja",
    texto: `En promedio supera ${kmPorCli.toFixed(1)} km por cliente; conviene revisar orden de visitas y agrupación territorial.`,
  }
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
    out.push(
      "En el período consultado no aparecen jornadas con visitas planificadas para este vendedor. Si debería haberlas, conviene revisar la carga de datos o el filtro de fechas.",
    )
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
      `La jornada del ${maxEntry.dia} concentra alrededor del ${pctMax.toFixed(0)}% de los kilómetros de la semana; es el día con mayor carga de conducción y suele ser el primer candidato a revisar si se busca alivianar la semana.`,
    )
  }

  const avgKmDia = entries.length ? kmT / entries.length : 0
  const lowDays = entries.filter((e) => e.km > 0 && avgKmDia > 0 && e.km < avgKmDia * 0.45)
  if (lowDays.length >= 1 && entries.length >= 3) {
    out.push(
      `Los días ${lowDays.map((x) => x.dia).join(", ")} muestran un recorrido claramente menor al resto; pueden servir para incorporar visitas adicionales o equilibrar la carga sin recargar los días más exigentes.`,
    )
  }

  if (kmT >= 400) {
    out.push(
      "El total de kilómetros de la semana es alto en términos operativos: implica muchas horas al volante y un costo de combustible relevante; vale la pena validar prioridades de visita y apoyos logísticos.",
    )
  } else if (kmT > 0 && kmT <= 130 && entries.length >= 2) {
    out.push(
      "El kilometraje total de la semana es moderado; si la cartera lo permite, podría evaluarse una mayor densidad de visitas en algunos días sin comprometer el servicio.",
    )
  }

  const kmPerClienteGlobal = nCli > 0 ? kmT / nCli : 0
  if (kmPerClienteGlobal >= 12) {
    out.push(
      "En promedio se recorren muchos kilómetros por cada cliente atendido; suele indicar clientes muy dispersos o rutas poco agrupadas, con impacto directo en tiempo de traslado y cansancio del vendedor.",
    )
  } else if (kmPerClienteGlobal > 0 && kmPerClienteGlobal <= 4.5) {
    out.push(
      "El promedio de kilómetros por cliente es favorable: las visitas tienden a estar bien agrupadas, lo que facilita cumplir horarios y contener costos de desplazamiento.",
    )
  }

  const kms = entries.map((e) => e.km)
  const mean = kms.reduce((s, x) => s + x, 0) / Math.max(kms.length, 1)
  const stdev = Math.sqrt(kms.reduce((s, x) => s + (x - mean) ** 2, 0) / Math.max(kms.length, 1))
  if (mean > 0 && stdev / mean > 0.38 && entries.length >= 3) {
    out.push(
      "La semana no es homogénea: hay días muy cargados y otros más livianos. Revisar la distribución semanal ayuda a nivelar esfuerzo, costos y cumplimiento de visitas frente a prioridades comerciales.",
    )
  }

  if (entries.some((e) => e.clis > 0 && e.km / e.clis > 18)) {
    out.push(
      "En al menos un día el recorrido por cliente es muy alto; suele asociarse a trayectos largos entre una parada y otra. Conviene revisar el orden de ruta o la asignación territorial de ese día.",
    )
  }

  out.push(
    "Como próximo paso práctico, se recomienda revisar con el vendedor el orden de visitas y el balance entre jornadas, priorizando clientes clave y buscando rutas más compactas sin perder calidad de servicio.",
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
