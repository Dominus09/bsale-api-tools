import type { PackComponentStatus, PackCostStatus, PackProblem } from "./packs-types"

export const PACK_STATUS_LABELS: Record<PackCostStatus, string> = {
  calculado: "Calculado",
  sin_composicion: "Sin composición en RAW",
  composicion_no_interpretable: "Composición no interpretable",
  pack_abierto: "Pack abierto (variante se elige al vender)",
  ciclo_detectado: "Ciclo entre packs",
  componente_inexistente: "Componente inexistente o retirado",
  componente_pack_no_calculable: "Contiene un pack no calculable",
  componente_requiere_revision_tributaria: "Componente con impuestos sin resolver",
  componente_sin_costo: "Componente sin costo",
}

export const PACK_STATUS_TONES: Record<PackCostStatus, string> = {
  calculado: "border-emerald-300 bg-emerald-50 text-emerald-800",
  sin_composicion: "border-zinc-300 bg-zinc-100 text-zinc-700",
  composicion_no_interpretable: "border-orange-300 bg-orange-50 text-orange-800",
  pack_abierto: "border-amber-300 bg-amber-50 text-amber-800",
  ciclo_detectado: "border-red-300 bg-red-50 text-red-800",
  componente_inexistente: "border-red-300 bg-red-50 text-red-800",
  componente_pack_no_calculable: "border-amber-300 bg-amber-50 text-amber-800",
  componente_requiere_revision_tributaria: "border-orange-300 bg-orange-50 text-orange-800",
  componente_sin_costo: "border-zinc-300 bg-zinc-100 text-zinc-700",
}

export const PACK_COMPONENT_STATUS_LABELS: Record<PackComponentStatus, string> = {
  calculado: "Calculado",
  inexistente_o_retirado: "Inexistente o retirado",
  sin_costo: "Sin costo",
  requiere_revision_tributaria: "Impuestos sin resolver",
  pack_no_calculable: "Pack no calculable",
  ciclo: "Ciclo",
  anidamiento_excesivo: "Anidamiento excesivo",
  variante_no_determinada: "Variante no determinada",
}

export const PACK_PROBLEM_LABELS: Record<PackProblem, string> = {
  precio_bajo_costo: "Precio bajo costo",
  bajo_politica: "Recargo bajo política",
  pack_no_calculable: "Costo pendiente",
  componentes_faltantes: "Componentes faltantes",
  componentes_sin_costo: "Componentes sin costo",
  componente_costo_desactualizado: "Costo de componente desactualizado",
  componente_costo_requiere_revision: "Costo de componente por revisar",
  componente_alza_de_costo: "Alza de costo en componente",
  componente_inactivo: "Componente inactivo",
  detalle_inactivo_excluido: "Detalle inactivo excluido",
  diferencia_extrema_entre_listas: "Diferencia extrema entre listas",
}

export const PACK_SORT_LABELS: Record<string, string> = {
  status: "Prioridad de revisión",
  markup_asc: "Menor recargo",
  markup_desc: "Mayor recargo",
  product: "Pack (A–Z)",
}
