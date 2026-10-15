import { TAX_LABELS } from "./labels"
import type { CostBasis, MarginRisk, MarginStatus, PriceListScopeStatus, PromotionCheck, RuleIssue } from "./margins-types"
import { PACK_STATUS_LABELS } from "./packs-labels"
import type { PackCostStatus } from "./packs-types"
import type { TaxProfileStatus } from "./types"

export const MARGIN_STATUS_LABELS: Record<MarginStatus, string> = {
  bajo_politica: "Bajo política",
  dentro_politica: "Dentro de política",
  sobre_politica: "Sobre política",
  sin_costo: "Sin costo",
  sin_precio: "Sin precio",
  sin_regla: "Sin regla",
  costo_no_verificado: "Costo no verificado",
  requiere_revision_tributaria: "Requiere revisión tributaria",
  pack_no_calculable: "Pack con costo pendiente",
  politica_provisional: "Política provisional",
  promocion_vigente: "Promoción vigente",
  remate_vigente: "Remate vigente",
  promocion_pendiente_verificacion: "Promoción por verificar",
}

export const MARGIN_STATUS_TONES: Record<MarginStatus, string> = {
  bajo_politica: "border-red-300 bg-red-50 text-red-800",
  dentro_politica: "border-emerald-300 bg-emerald-50 text-emerald-800",
  sobre_politica: "border-sky-300 bg-sky-50 text-sky-800",
  sin_costo: "border-zinc-300 bg-zinc-100 text-zinc-700",
  sin_precio: "border-zinc-300 bg-zinc-100 text-zinc-700",
  sin_regla: "border-amber-300 bg-amber-50 text-amber-800",
  costo_no_verificado: "border-amber-300 bg-amber-50 text-amber-800",
  requiere_revision_tributaria: "border-orange-300 bg-orange-50 text-orange-800",
  pack_no_calculable: "border-zinc-300 bg-zinc-100 text-zinc-700",
  politica_provisional: "border-violet-300 bg-violet-50 text-violet-800",
  promocion_vigente: "border-fuchsia-300 bg-fuchsia-50 text-fuchsia-800",
  remate_vigente: "border-fuchsia-300 bg-fuchsia-50 text-fuchsia-800",
  promocion_pendiente_verificacion: "border-orange-300 bg-orange-50 text-orange-800",
}

export const PRIMARY_STATUSES: MarginStatus[] = [
  "bajo_politica",
  "dentro_politica",
  "sobre_politica",
  "costo_no_verificado",
  "requiere_revision_tributaria",
  "pack_no_calculable",
  "sin_regla",
  "sin_costo",
  "sin_precio",
]

export const QUALIFIER_STATUSES: MarginStatus[] = [
  "costo_no_verificado",
  "politica_provisional",
  "promocion_pendiente_verificacion",
]

export const COST_BASIS_LABELS: Record<CostBasis, string> = {
  promedio_bsale: "Promedio Bsale",
  ultimo_comparable: "Última compra comparable",
}

export const PROMOTION_CHECK_LABELS: Record<PromotionCheck, string> = {
  fuente_no_integrada: "Promociones V2 no integrado: no se puede descartar una promoción",
  sin_excepcion: "Sin excepción vigente",
  excepcion_verificada: "Excepción verificada contra Bsale",
  excepcion_no_verificada: "Excepción aprobada, precio Bsale no coincide",
  excepcion_pendiente_aprobacion: "Campaña registrada sin aprobar",
  conflicto_promociones: "Promociones superpuestas sin resolver",
  precio_no_verificable: "Precio Bsale ausente o desactualizado: no verificable",
  promocion_legacy_no_verificable: "Promoción del módulo anterior: no verificable",
}

export const MARGIN_RISK_LABELS: Record<MarginRisk, string> = {
  venta_bajo_costo: "Venta bajo costo",
  costo_sospechoso: "Costo sospechoso",
  costo_desactualizado: "Costo desactualizado",
  costo_no_verificado: "Costo no verificado",
  sin_costo_valido: "Sin costo válido",
  precio_no_verificado: "Precio no verificado",
}

/** Riesgos que se destacan siempre, también con promoción o remate vigente. */
export const HIGHLIGHTED_RISKS: MarginRisk[] = ["venta_bajo_costo", "costo_sospechoso", "costo_desactualizado"]

export const RULE_ISSUE_LABELS: Record<RuleIssue, string> = {
  maximo_cero_requiere_revision: "Máximo 0: requiere revisión",
  minimo_mayor_que_maximo: "Mínimo mayor que máximo",
  regla_inactiva: "Regla inactiva",
  minimo_nulo: "Mínimo vacío",
}

export const LIST_SCOPE_LABELS: Record<PriceListScopeStatus, string> = {
  evaluada: "Evaluada",
  autorizada_no_vigente: "Autorizada pero no vigente en Bsale",
  retirada: "Retirada (histórica)",
  retirada_reactivada: "Retirada que aparece vigente en Bsale",
  vigente_no_autorizada: "Vigente en Bsale, fuera del análisis",
  no_vigente: "No vigente",
}

export const REASON_LABELS: Record<string, string> = {
  sin_precio_en_lista: "SKU sin precio en la lista",
  precio_retirado_en_bsale: "Precio ya no existe en Bsale",
  precio_bruto_nulo: "Precio bruto vacío",
  precio_cero_o_negativo: "Precio 0 o negativo",
  precio_simbolico: "Precio simbólico (< $10)",
  sin_costo_ultimo_comparable: "Sin compra comparable (factura/guía)",
  sin_costo_promedio_bsale: "Sin costo promedio Bsale",
  regla_inexistente: "No existe regla para lista y tipo",
  regla_no_aplicable: "Regla inactiva o sin mínimo",
  politica_pendiente_revision: "Política pendiente de revisión (Excel)",
  costo_no_verificado: "Costo base no verificado",
}

export const MARGIN_SORT_LABELS: Record<string, string> = {
  status: "Prioridad de revisión",
  gap_asc: "Mayor brecha bajo el mínimo",
  markup_asc: "Menor recargo",
  markup_desc: "Mayor recargo",
  product: "Producto (A–Z)",
}

export function reasonLabel(code: string): string {
  if (code.startsWith("pack_")) {
    const status = code.slice(5) as PackCostStatus
    return `Pack: ${PACK_STATUS_LABELS[status] ?? status.replaceAll("_", " ")}`
  }
  if (code.startsWith("impuesto_")) {
    const status = code.slice(9) as TaxProfileStatus
    return `Revisión tributaria: ${TAX_LABELS[status] ?? status.replaceAll("_", " ")}`
  }
  return REASON_LABELS[code] ?? code
}
