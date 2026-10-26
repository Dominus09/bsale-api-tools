/** Filtro operativo de stock global (Fase 2.4). */

export type StockFilter = "with" | "without" | "all"

export const DEFAULT_STOCK_FILTER: StockFilter = "with"

export const STOCK_FILTER_OPTIONS: { value: StockFilter; label: string }[] = [
  { value: "with", label: "Con stock" },
  { value: "without", label: "Sin stock" },
  { value: "all", label: "Todos" },
]

export const STOCK_FILTER_LABELS: Record<StockFilter, string> = {
  with: "CON STOCK",
  without: "SIN STOCK",
  all: "TODOS",
}

export function isStockFilter(value: string | null | undefined): value is StockFilter {
  return value === "with" || value === "without" || value === "all"
}
