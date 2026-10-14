import type { DataClass, FreshnessStatus } from "./dashboard-types"

export const DATA_CLASS_LABELS: Record<DataClass, string> = {
  confirmado: "Confirmado",
  provisional: "Provisional",
  no_disponible: "No disponible",
}

export const DATA_CLASS_HINTS: Record<DataClass, string> = {
  confirmado: "Cálculo con política validada y costo verificado",
  provisional: "Calculado, pero la política o el costo aún no están validados",
  no_disponible: "Sin precio, sin costo, sin regla o con costo no verificable",
}

export const DATA_CLASS_TONES: Record<DataClass, string> = {
  confirmado: "border-emerald-300 bg-emerald-50 text-emerald-800",
  provisional: "border-amber-300 bg-amber-50 text-amber-800",
  no_disponible: "border-zinc-300 bg-zinc-100 text-zinc-700",
}

export const FRESHNESS_LABELS: Record<FreshnessStatus, string> = {
  al_dia: "Al día",
  atrasada: "Atrasada",
  con_error: "Con error",
  sin_datos: "Sin datos",
}

export const FRESHNESS_TONES: Record<FreshnessStatus, string> = {
  al_dia: "border-emerald-300 bg-emerald-50 text-emerald-800",
  atrasada: "border-amber-300 bg-amber-50 text-amber-800",
  con_error: "border-red-300 bg-red-50 text-red-800",
  sin_datos: "border-zinc-300 bg-zinc-100 text-zinc-700",
}

export const RAW_RESOURCE_LABELS: Record<string, string> = {
  price_lists: "Listas de precios",
  variant_prices: "Precios por lista",
  variant_costs: "Costos promedio",
  stock_receptions: "Recepciones de stock",
  variants: "Variantes",
  products: "Productos",
  product_taxes: "Impuestos de producto",
  stocks: "Stock",
}

export const BLOCK_REASON_LABELS: Record<string, string> = {
  sin_permiso: "Su perfil no tiene acceso a este módulo.",
  tiempo_agotado: "La consulta superó el tiempo máximo. Reintente en unos minutos.",
  error_de_datos: "No se pudieron leer los datos de origen.",
  sin_alcance_comercial: "La empresa no tiene alcance comercial configurado.",
  error_interno: "Error interno al calcular este bloque.",
}

export const WARNING_LABELS: Record<string, string> = {
  sincronizacion_atrasada: "La sincronización BSALE RAW está atrasada",
  sincronizacion_con_error: "La última sincronización BSALE RAW terminó con error",
  sincronizacion_sin_datos: "No hay registro de sincronización para algunos recursos",
  politica_margenes_pendiente_revision: "Política de márgenes pendiente de revisión",
  costo_bruto_preliminar: "Costo bruto preliminar (sin contrastar con facturas)",
  configuracion_tributaria_no_contrastada: "Configuración tributaria de Bsale aún no contrastada por contabilidad",
  productos_requieren_revision_tributaria: "Hay productos que requieren revisión tributaria",
  promociones_no_integradas: "Promociones V2 no integrado en márgenes",
  bloque_margenes_no_disponible: "Márgenes no disponible",
  bloque_costos_no_disponible: "Costos no disponible",
  bloque_impuestos_no_disponible: "Impuestos no disponible",
  bloque_promociones_no_disponible: "Promociones no disponible",
}

export function warningLabel(code: string): string {
  return WARNING_LABELS[code] ?? code.replaceAll("_", " ")
}
