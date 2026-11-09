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
  if (err instanceof Error) {
    const name = err.name || ""
    return `${name} ${err.message || ""}`.trim()
  }
  if (typeof err === "object" && "message" in err) {
    return String((err as { message?: unknown }).message || "")
  }
  return String(err)
}

/**
 * Errores de MapLibre durante carga de estilo/tiles suelen ser recuperables.
 * Solo marcar fatal ante fallo de GPU/init o timeout sin estilo listo.
 */
export function classifyMapFailure(
  err: unknown,
  opts?: { styleLoaded?: boolean; sourceId?: string | null; phase?: string },
): ClassifiedMapFailure {
  const styleLoaded = Boolean(opts?.styleLoaded)
  const sourceId = opts?.sourceId || ""
  const phase = opts?.phase || ""
  const raw = textOf(err)
  const lower = raw.toLowerCase()

  if (
    /gpuinitialization|webgl2|webgl not|failed to initialize|no webgl|context lost/.test(
      lower,
    )
  ) {
    return {
      kind: "fatal",
      code: "map_gpu_unavailable",
      messageEs:
        "Este dispositivo no pudo iniciar el mapa (WebGL). Usa la lista de clientes para documentos y productos.",
    }
  }

  if (phase === "timeout" && !styleLoaded) {
    return {
      kind: "fatal",
      code: "map_style_timeout",
      messageEs:
        "El mapa tardó demasiado en cargar. Reintenta o continúa por la lista de clientes.",
    }
  }

  if (phase === "init") {
    return {
      kind: "fatal",
      code: "map_init_failed",
      messageEs:
        "No se pudo inicializar el mapa. Usa la lista de clientes para consultar el despacho.",
    }
  }

  // Cualquier error de red/tiles/sprites/glyphs con o sin estilo → degradado, no destruir mapa.
  if (
    styleLoaded ||
    sourceId ||
    /tile|tiles|sprite|glyph|ajax|network|failed to fetch|404|503|load/.test(lower) ||
    phase === "style" ||
    phase === "source"
  ) {
    return {
      kind: "recoverable",
      code: "map_tiles_degraded",
      messageEs:
        "Algunas capas del mapa no cargaron. Puedes seguir usando los marcadores o la lista de clientes.",
    }
  }

  // Antes de load, errores genéricos: degradar (evitar falsos fatales por worker/style parcial).
  return {
    kind: "recoverable",
    code: "map_load_warning",
    messageEs:
      "Hubo un aviso al cargar el mapa. Si no ves el mapa, reintenta o usa la lista de clientes.",
  }
}

export function classifyMapInitError(err: unknown): ClassifiedMapFailure {
  return classifyMapFailure(err, { styleLoaded: false, phase: "init" })
}

export function classifyMapTimeout(): ClassifiedMapFailure {
  return classifyMapFailure("timeout", { styleLoaded: false, phase: "timeout" })
}
