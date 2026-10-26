/** Filtro operativo de stock (Fase 2.7: stock operativo ≠ stock total Bsale). */

export type StockFilter = "with" | "without" | "all"

export const DEFAULT_STOCK_FILTER: StockFilter = "with"

export const STOCK_FILTER_OPTIONS: { value: StockFilter; label: string }[] = [
  { value: "with", label: "Con stock operativo" },
  { value: "without", label: "Sin stock operativo" },
  { value: "all", label: "Todos" },
]

export const STOCK_FILTER_LABELS: Record<StockFilter, string> = {
  with: "CON STOCK OPERATIVO",
  without: "SIN STOCK OPERATIVO",
  all: "TODOS",
}

export const AUXILIARY_ONLY_STOCK_LEGEND = "Sin stock operativo · Stock en bodegas auxiliares"

export function isStockFilter(value: string | null | undefined): value is StockFilter {
  return value === "with" || value === "without" || value === "all"
}
