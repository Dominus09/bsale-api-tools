/** Clasificación de errores MapLibre / tiles (sin inventar coordenadas). */

export type MapFailureKind = "fatal" | "recoverable"

export type ClassifiedMapFailure = {
  kind: MapFailureKind
  messageEs: string
  code: string
}

function textOf(err: unknown): string {
  if (err == null) return ""
  if (typeof err === "string") return err
  if (err instanceof Error) return err.message || String(err)
  if (typeof err === "object" && "message" in err) {
    return String((err as { message?: unknown }).message || "")
  }
  return String(err)
}

/**
 * Distingue fallos que impiden ver el mapa de errores de tile recuperables.
 * `styleLoaded` = true después del evento `load` / estilo listo.
 */
export function classifyMapFailure(
  err: unknown,
  opts?: { styleLoaded?: boolean; sourceId?: string | null },
): ClassifiedMapFailure {
  const styleLoaded = Boolean(opts?.styleLoaded)
  const sourceId = opts?.sourceId || ""
  const raw = textOf(err)
  const lower = raw.toLowerCase()

  const looksLikeTile =
    Boolean(sourceId) ||
    /tile|tiles|raster|vector tile|ajax|networkerror|failed to fetch|load tile|404|503/.test(
      lower,
    )

  if (styleLoaded || looksLikeTile) {
    return {
      kind: "recoverable",
      code: "map_tiles_degraded",
      messageEs:
        "Algunas capas del mapa no cargaron. Puedes seguir usando los marcadores o la lista de clientes.",
    }
  }

  if (/style|openfreemap|stylesheet|webgl|maplibre|inicial|init|construct/.test(lower)) {
    return {
      kind: "fatal",
      code: "map_style_unavailable",
      messageEs:
        "No se pudo cargar el mapa (estilo o proveedor). Usa la lista de clientes para consultar documentos y productos.",
    }
  }

  return {
    kind: "fatal",
    code: "map_unavailable",
    messageEs:
      "El mapa no está disponible en este momento. Puedes continuar por la lista de clientes.",
  }
}

export function classifyMapInitError(err: unknown): ClassifiedMapFailure {
  const raw = textOf(err) || "init_failed"
  return classifyMapFailure(raw, { styleLoaded: false })
}
