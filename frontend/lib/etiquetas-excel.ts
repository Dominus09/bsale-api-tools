import * as XLSX from "xlsx"

export type EtiquetasExcelRow = {
  barcode: string
  quantity: number
}

export type LabelResolveItemInput = {
  barcode: unknown
  quantity?: unknown
}

export type PreparedLabelResolveItem = {
  barcode: string
  quantity: number
}

export type EtiquetasExcelParseResult = {
  rows: EtiquetasExcelRow[]
  duplicates: { barcode: string; count: number }[]
  skippedEmpty: number
}

const BARCODE_COLUMNS = new Set([
  "codigo_barra",
  "cod_barra",
  "barcode",
  "codigo",
])

const QUANTITY_COLUMNS = new Set(["cantidad", "qty", "quantity"])

/** Normaliza nombre de columna: minúsculas, sin tildes, espacios → _ */
export function normalizeExcelColumnKey(key: string): string {
  return key
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
    .replace(/\s+/g, "_")
}

export function isBarcodeExcelColumn(key: string): boolean {
  return BARCODE_COLUMNS.has(normalizeExcelColumnKey(key))
}

export function isQuantityExcelColumn(key: string): boolean {
  const k = normalizeExcelColumnKey(key)
  return QUANTITY_COLUMNS.has(k) || k.includes("cantidad")
}

/**
 * Convierte celda Excel a código de barras en string.
 * Acepta número, texto, fórmula ="..." y evita notación científica.
 */
export function normalizeExcelBarcodeCell(value: unknown): string {
  if (value == null || value === "") return ""

  if (typeof value === "number") {
    if (!Number.isFinite(value)) return ""
    const intVal = Math.round(value)
    if (Math.abs(value - intVal) < 1e-6) {
      return formatIntegerNoScientific(intVal)
    }
    return String(value).trim()
  }

  if (typeof value === "boolean") return ""

  let text = String(value).trim()
  if (!text) return ""

  const formulaQuoted = text.match(/^=\s*["']([^"']+)["']\s*$/i)
  if (formulaQuoted) return formulaQuoted[1].trim()

  if (text.startsWith("=")) {
    text = text.slice(1).trim()
    const innerQuoted = text.match(/^["']([^"']+)["']$/i)
    if (innerQuoted) return innerQuoted[1].trim()
  }

  if (/^[\d.]+e[+-]?\d+$/i.test(text)) {
    const n = Number(text)
    if (Number.isFinite(n)) return formatIntegerNoScientific(Math.round(n))
  }

  if (/^\d+\.0+$/.test(text)) {
    return text.replace(/\.0+$/, "")
  }

  return text
}

function formatIntegerNoScientific(n: number): string {
  return n.toLocaleString("en-US", {
    useGrouping: false,
    maximumFractionDigits: 0,
  })
}

function parseQuantityCell(value: unknown): number {
  if (value == null || value === "") return 1
  if (typeof value === "number" && Number.isFinite(value) && value > 0) {
    return Math.min(9999, Math.trunc(value))
  }
  const text = String(value).trim().replace(",", ".")
  const n = parseInt(text.split(".")[0], 10)
  return Number.isFinite(n) && n > 0 ? Math.min(9999, n) : 1
}

/**
 * Normaliza filas antes de POST /labels/resolve.
 * Garantiza barcode string (sin .0, sin notación científica) y quantity entero 1..9999.
 */
export function prepareLabelResolveItems(
  items: LabelResolveItemInput[],
): PreparedLabelResolveItem[] {
  const out: PreparedLabelResolveItem[] = []

  for (const it of items) {
    let barcode = normalizeExcelBarcodeCell(it.barcode)
    let quantity = parseQuantityCell(it.quantity)

    console.log("barcode parsed", it.barcode, "->", barcode)

    const qtyRaw = it.quantity
    const qtyLooksLikeBarcode =
      (typeof qtyRaw === "number" && qtyRaw >= 10_000_000) ||
      (typeof qtyRaw === "string" && /^\d{10,}$/.test(qtyRaw.trim().replace(/\.0+$/, "")))
    const barcodeLooksLikeQty =
      /^\d{1,4}$/.test(barcode) && barcode.length <= 4

    if (!barcode && qtyLooksLikeBarcode) {
      barcode = normalizeExcelBarcodeCell(qtyRaw)
      quantity = 1
    } else if (qtyLooksLikeBarcode && barcodeLooksLikeQty) {
      const swapped = normalizeExcelBarcodeCell(qtyRaw)
      if (swapped) {
        barcode = swapped
        quantity = parseQuantityCell(it.barcode)
      }
    }

    if (!barcode || barcode.length > 50) continue

    quantity = Math.max(1, Math.min(9999, Math.trunc(quantity)))
    out.push({ barcode, quantity })
  }

  return out
}

function findDuplicates(rows: EtiquetasExcelRow[]): { barcode: string; count: number }[] {
  const counts = new Map<string, number>()
  for (const row of rows) {
    counts.set(row.barcode, (counts.get(row.barcode) ?? 0) + 1)
  }
  return [...counts.entries()]
    .filter(([, count]) => count > 1)
    .map(([barcode, count]) => ({ barcode, count }))
    .sort((a, b) => a.barcode.localeCompare(b.barcode))
}

/** Agrupa filas repetidas sumando cantidades. */
export function mergeEtiquetasExcelRows(rows: EtiquetasExcelRow[]): EtiquetasExcelRow[] {
  const map = new Map<string, number>()
  for (const row of rows) {
    map.set(row.barcode, (map.get(row.barcode) ?? 0) + row.quantity)
  }
  return [...map.entries()].map(([barcode, quantity]) => ({ barcode, quantity }))
}

export function parseEtiquetasExcel(buffer: ArrayBuffer): EtiquetasExcelParseResult {
  const wb = XLSX.read(buffer, { type: "array", cellFormula: true })
  const sheet = wb.Sheets[wb.SheetNames[0]]
  if (!sheet) {
    return { rows: [], duplicates: [], skippedEmpty: 0 }
  }

  const rawRows = XLSX.utils.sheet_to_json<Record<string, unknown>>(sheet, {
    defval: "",
    raw: true,
  })

  const out: EtiquetasExcelRow[] = []
  let skippedEmpty = 0

  for (const row of rawRows) {
    const entries = Object.entries(row)
    if (entries.length === 0) {
      skippedEmpty += 1
      continue
    }

    let barcode = ""
    let quantity = 1
    let barcodeKey: string | null = null

    for (const [key, raw] of entries) {
      if (isBarcodeExcelColumn(key)) {
        barcodeKey = key
        const val = normalizeExcelBarcodeCell(raw)
        if (val) barcode = val
      }
      if (isQuantityExcelColumn(key)) {
        quantity = parseQuantityCell(raw)
      }
    }

    if (!barcode) {
      const firstKey = entries[0][0]
      const firstVal = normalizeExcelBarcodeCell(entries[0][1])
      if (firstVal && !isQuantityExcelColumn(firstKey)) {
        barcode = firstVal
      }
    }

    if (!barcode && barcodeKey) {
      barcode = normalizeExcelBarcodeCell(row[barcodeKey])
    }

    if (!barcode) {
      skippedEmpty += 1
      continue
    }

    out.push({ barcode, quantity })
  }

  return {
    rows: out,
    duplicates: findDuplicates(out),
    skippedEmpty,
  }
}

/** @deprecated use parseEtiquetasExcel */
export function parseExcelRows(buffer: ArrayBuffer): EtiquetasExcelRow[] {
  return parseEtiquetasExcel(buffer).rows
}

export function downloadEtiquetasExcelTemplate(): void {
  const wb = XLSX.utils.book_new()
  const ws = XLSX.utils.aoa_to_sheet([
    ["codigo_barra", "cantidad"],
    ["7802100505323", 1],
    ["7802100001719", 1],
  ])

  const textFormat = "@"
  ;["A2", "A3"].forEach((ref) => {
    if (ws[ref]) {
      ws[ref].t = "s"
      ws[ref].z = textFormat
    }
  })
  if (ws.A1) ws.A1.t = "s"
  if (ws.B1) ws.B1.t = "s"

  ws["!cols"] = [{ wch: 18 }, { wch: 10 }]

  XLSX.utils.book_append_sheet(wb, ws, "etiquetas")
  XLSX.writeFile(wb, "plantilla-etiquetas.xlsx")
}
