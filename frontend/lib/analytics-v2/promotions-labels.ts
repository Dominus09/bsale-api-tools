import type {
  CampaignApproval,
  CampaignKind,
  CampaignTimeline,
  ClearanceReason,
  ItemExceptionCheck,
  PriceVerification,
  PromotionSection,
} from "./promotions-types"
import { PROMOTION_CHECK_LABELS } from "./margins-labels"

export const KIND_LABELS: Record<CampaignKind, string> = { oferta: "Oferta", remate: "Remate" }

export const APPROVAL_LABELS: Record<CampaignApproval, string> = {
  borrador: "Borrador",
  pendiente_aprobacion: "Pendiente de aprobación",
  aprobada: "Aprobada",
  rechazada: "Rechazada",
  cancelada: "Cancelada",
}

export const APPROVAL_TONES: Record<CampaignApproval, string> = {
  borrador: "border-zinc-300 bg-zinc-100 text-zinc-700",
  pendiente_aprobacion: "border-amber-300 bg-amber-50 text-amber-800",
  aprobada: "border-emerald-300 bg-emerald-50 text-emerald-800",
  rechazada: "border-red-300 bg-red-50 text-red-800",
  cancelada: "border-zinc-300 bg-zinc-100 text-zinc-500 line-through",
}

export const TIMELINE_LABELS: Record<CampaignTimeline, string> = {
  programada: "Programada",
  vigente: "Vigente",
  finalizada: "Finalizada",
  cancelada: "Cancelada",
}

export const TIMELINE_TONES: Record<CampaignTimeline, string> = {
  programada: "border-sky-300 bg-sky-50 text-sky-800",
  vigente: "border-fuchsia-300 bg-fuchsia-50 text-fuchsia-800",
  finalizada: "border-zinc-300 bg-zinc-100 text-zinc-600",
  cancelada: "border-zinc-300 bg-zinc-100 text-zinc-500",
}

export const CLEARANCE_LABELS: Record<ClearanceReason, string> = {
  vencimiento: "Próximo a vencer",
  descontinuado: "Descontinuado",
  dano: "Daño / merma",
  sobrestock: "Sobrestock",
  otro: "Otro",
}

export const VERIFICATION_LABELS: Record<PriceVerification, string> = {
  coincide_bsale: "Precio aplicado en Bsale",
  no_coincide_bsale: "Bsale tiene otro precio",
  sin_precio_bsale: "Sin precio en Bsale",
  precio_bsale_desactualizado: "Precio Bsale desactualizado",
  vencida_sin_revertir: "Finalizada, Bsale sigue con precio promo",
  no_aplica: "No aplica",
}

export const VERIFICATION_TONES: Record<PriceVerification, string> = {
  coincide_bsale: "border-emerald-300 bg-emerald-50 text-emerald-800",
  no_coincide_bsale: "border-red-300 bg-red-50 text-red-800",
  sin_precio_bsale: "border-amber-300 bg-amber-50 text-amber-800",
  precio_bsale_desactualizado: "border-amber-300 bg-amber-50 text-amber-800",
  vencida_sin_revertir: "border-orange-300 bg-orange-50 text-orange-800",
  no_aplica: "border-zinc-300 bg-zinc-100 text-zinc-600",
}

export const EXCEPTION_LABELS: Record<ItemExceptionCheck, string> = {
  ...PROMOTION_CHECK_LABELS,
  no_vigente: "Fuera de vigencia",
  no_participa: "No participa (borrador, rechazada, cancelada o retirado)",
}

export const SECTION_LABELS: Record<PromotionSection, string> = {
  vigentes: "Ofertas vigentes",
  remates: "Remates",
  proximas: "Próximas campañas",
  pendientes: "Pendientes de verificación",
  historial: "Historial del mes",
}

export const HISTORY_EVENT_LABELS: Record<string, string> = {
  campania_creada: "Campaña creada",
  campania_editada: "Campaña editada",
  enviada_aprobacion: "Enviada a aprobación",
  aprobada: "Aprobada",
  rechazada: "Rechazada",
  cancelada: "Cancelada",
  aprobacion_revocada_por_cambio: "Aprobación revocada por cambio",
  item_agregado: "Producto agregado",
  item_editado: "Producto editado",
  item_precio_cambiado: "Precio promocional cambiado",
  item_retirado: "Producto retirado",
  importada_legacy: "Importada desde módulo anterior",
}

export const LEGACY_MAPPING_LABELS: Record<string, string> = {
  mapeable: "Identificado en RAW",
  variante_no_encontrada: "Código de barras sin variante",
  variante_ambigua: "Código de barras con varias variantes",
  lista_no_encontrada: "Lista no encontrada",
  lista_ambigua: "Lista ambigua",
  lista_fuera_de_alcance: "Lista fuera del alcance vigente",
}

export function currentMonth(): string {
  const now = new Date()
  return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`
}

export function shiftMonth(month: string, delta: number): string {
  const [y, m] = month.split("-").map(Number)
  const d = new Date(y, m - 1 + delta, 1)
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`
}

export function monthLabel(month: string): string {
  const [y, m] = month.split("-").map(Number)
  return new Date(y, m - 1, 1).toLocaleDateString("es-CL", { month: "long", year: "numeric" })
}
