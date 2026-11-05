/** Parseo local de folios OC (pegar / Excel). No sube el archivo al servidor. */

export const SIMPLE_PLANNING_MAX_FOLIOS = 200
export const SIMPLE_PLANNING_MAX_XLSX_BYTES = 2 * 1024 * 1024

export type FolioParseIssue = {
  kind: "empty" | "invalid" | "duplicate" | "too_many"
  input?: string
  folio?: number
  message: string
}

export type FolioParseResult = {
  folios: number[]
  /** Entradas originales normalizadas en el mismo orden que `folios` (únicos). */
  inputs: string[]
  issues: FolioParseIssue[]
  skippedEmpty: number
}

/** Suma montos de OC (una fila = una OC). No agrega por cliente. */
export function sumOcVentaClp(
  items: { total_amount?: number | null }[] | null | undefined,
): number {
  let sum = 0
  for (const item of items || []) {
    const n = Number(item.total_amount)
    if (Number.isFinite(n)) sum += n
  }
  return sum
}

export type TruckAssignmentDraftStats = {
  truckId: number
  orders: { folio?: number | null; total_amount?: number | null; client?: unknown; weight?: { known_weight_kg?: number | null; peso_total_kg?: number | null; coverage_percent?: number | null } | null }[]
  ocCount: number
  clients: number
  ventaOcClp: number
  knownKg: number
  avgCoverage: number | null
  withWeight: number
}

/** Totales locales por camión desde asignación draft (sin assign-preview). */
export function draftTruckStatsFromAssignments<
  T extends {
    folio?: number | null
    total_amount?: number | null
    client?: { client_id?: number | null; client_name?: string | null } | null
    weight?: {
      known_weight_kg?: number | null
      peso_total_kg?: number | null
      coverage_percent?: number | null
    } | null
  },
>(
  items: T[],
  assignmentByFolio: Record<string, number>,
  uniqueClients: (orders: T[]) => number,
): TruckAssignmentDraftStats[] {
  const byTruck = new Map<number, T[]>()
  for (const item of items) {
    const folio = item.folio
    if (folio == null) continue
    const truckId = assignmentByFolio[String(folio)]
    if (truckId == null || !Number.isFinite(truckId)) continue
    const list = byTruck.get(truckId) || []
    list.push(item)
    byTruck.set(truckId, list)
  }
  const out: TruckAssignmentDraftStats[] = []
  for (const [truckId, orders] of [...byTruck.entries()].sort((a, b) => a[0] - b[0])) {
    let knownKg = 0
    let withWeight = 0
    let coverageSum = 0
    let coverageN = 0
    for (const o of orders) {
      const kw = o.weight?.known_weight_kg ?? o.weight?.peso_total_kg
      if (kw != null && Number.isFinite(Number(kw))) {
        knownKg += Number(kw)
        withWeight += 1
      }
      const cov = o.weight?.coverage_percent
      if (cov != null && Number.isFinite(Number(cov))) {
        coverageSum += Number(cov)
        coverageN += 1
      }
    }
    out.push({
      truckId,
      orders,
      ocCount: orders.length,
      clients: uniqueClients(orders),
      ventaOcClp: sumOcVentaClp(orders),
      knownKg,
      avgCoverage: coverageN > 0 ? coverageSum / coverageN : null,
      withWeight,
    })
  }
  return out
}

function cleanRaw(raw: unknown): string {
  if (raw == null) return ""
  let s = String(raw).replace(/\u00a0/g, " ").trim()
  if (s.endsWith(".0") && /^\d+\.0$/.test(s)) s = s.slice(0, -2)
  return s
}

function digitsOnly(s: string): string {
  return s.replace(/[^\d]/g, "")
}

export function parseFolioToken(raw: unknown): { ok: true; folio: number; input: string } | { ok: false; input: string; reason: string } {
  const input = cleanRaw(raw)
  if (!input) return { ok: false, input: "", reason: "empty" }
  const digits = digitsOnly(input)
  if (!digits || !/^\d+$/.test(digits)) {
    return { ok: false, input, reason: "invalid" }
  }
  const folio = Number(digits)
  if (!Number.isSafeInteger(folio) || folio <= 0) {
    return { ok: false, input, reason: "invalid" }
  }
  return { ok: true, folio, input }
}

/** Une líneas pegadas (una OC por fila) y normaliza. */
export function parseFoliosFromText(text: string): FolioParseResult {
  const lines = text.split(/\r?\n/)
  return collectFolios(lines)
}

function collectFolios(rawValues: unknown[]): FolioParseResult {
  const folios: number[] = []
  const inputs: string[] = []
  const issues: FolioParseIssue[] = []
  const seen = new Map<number, number>()
  let skippedEmpty = 0

  for (const raw of rawValues) {
    const parsed = parseFolioToken(raw)
    if (!parsed.ok) {
      if (parsed.reason === "empty") {
        skippedEmpty += 1
        continue
      }
      issues.push({
        kind: "invalid",
        input: parsed.input,
        message: `Folio inválido: "${parsed.input}"`,
      })
      continue
    }
    if (seen.has(parsed.folio)) {
      issues.push({
        kind: "duplicate",
        folio: parsed.folio,
        input: parsed.input,
        message: `Duplicado en lista: ${parsed.folio}`,
      })
      continue
    }
    if (folios.length >= SIMPLE_PLANNING_MAX_FOLIOS) {
      issues.push({
        kind: "too_many",
        folio: parsed.folio,
        message: `Máximo ${SIMPLE_PLANNING_MAX_FOLIOS} OC por solicitud`,
      })
      break
    }
    seen.set(parsed.folio, folios.length)
    folios.push(parsed.folio)
    inputs.push(parsed.input)
  }

  return { folios, inputs, issues, skippedEmpty }
}

/**
 * Lee la primera hoja de un .xlsx en el navegador.
 * Usa la primera columna con encabezado que parezca OC/folio/número, o la col A.
 */
export async function parseFoliosFromXlsxFile(file: File): Promise<FolioParseResult> {
  if (!file.name.toLowerCase().endsWith(".xlsx")) {
    return {
      folios: [],
      inputs: [],
      issues: [{ kind: "invalid", message: "Solo se admiten archivos .xlsx" }],
      skippedEmpty: 0,
    }
  }
  if (file.size <= 0) {
    return {
      folios: [],
      inputs: [],
      issues: [{ kind: "empty", message: "El archivo está vacío" }],
      skippedEmpty: 0,
    }
  }
  if (file.size > SIMPLE_PLANNING_MAX_XLSX_BYTES) {
    return {
      folios: [],
      inputs: [],
      issues: [
        {
          kind: "invalid",
          message: `Archivo demasiado grande (máx. ${Math.round(SIMPLE_PLANNING_MAX_XLSX_BYTES / (1024 * 1024))} MB)`,
        },
      ],
      skippedEmpty: 0,
    }
  }

  const XLSX = await import("xlsx")
  const buf = await file.arrayBuffer()
  const wb = XLSX.read(buf, { type: "array", cellDates: false })
  const sheetName = wb.SheetNames[0]
  if (!sheetName) {
    return {
      folios: [],
      inputs: [],
      issues: [{ kind: "empty", message: "El libro no tiene hojas" }],
      skippedEmpty: 0,
    }
  }
  const sheet = wb.Sheets[sheetName]
  const rows = XLSX.utils.sheet_to_json<(string | number | null)[]>(sheet, {
    header: 1,
    defval: "",
    raw: true,
  }) as unknown[][]

  if (!rows.length) {
    return {
      folios: [],
      inputs: [],
      issues: [{ kind: "empty", message: "La hoja está vacía" }],
      skippedEmpty: 0,
    }
  }

  const header = (rows[0] || []).map((c) => cleanRaw(c).toLowerCase())
  const ocHeaderIdx = header.findIndex((h) =>
    /^(oc|n[ºo]?\s*oc|folio|numero|número|nro|n°)$/i.test(h) ||
    h.includes("orden de compra") ||
    h === "oc" ||
    h.startsWith("folio"),
  )
  const colIdx = ocHeaderIdx >= 0 ? ocHeaderIdx : 0
  const hasHeader = ocHeaderIdx >= 0
  const dataRows = hasHeader ? rows.slice(1) : rows

  const values: unknown[] = []
  for (const row of dataRows) {
    if (!Array.isArray(row)) continue
    values.push(row[colIdx])
  }
  return collectFolios(values)
}

export function uniqueClientCount(
  orders: { client?: { client_id?: number | null; client_name?: string | null } | null }[],
): number {
  const keys = new Set<string>()
  for (const o of orders) {
    const id = o.client?.client_id
    if (id != null) {
      keys.add(`id:${id}`)
      continue
    }
    const name = (o.client?.client_name || "").trim().toLowerCase()
    if (name) keys.add(`n:${name}`)
  }
  return keys.size
}
