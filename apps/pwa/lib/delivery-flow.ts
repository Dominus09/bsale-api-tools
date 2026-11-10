/** Helpers compartidos del circuito modular de entrega (P4.1). */

export type DeliveryStep = 1 | 2 | 3

export const STEP = {
  documentos: 1,
  cobranza: 2,
  firma: 3,
} as const

export type StepKey = keyof typeof STEP

export const STEP_DEFS: ReadonlyArray<{
  step: DeliveryStep
  key: StepKey
  label: string
  path: string
}> = [
  { step: 1, key: "documentos", label: "Documentos", path: "documentos" },
  { step: 2, key: "cobranza", label: "Cobranza", path: "cobranza" },
  { step: 3, key: "firma", label: "Firma", path: "firma" },
]

export function stepPath(step: DeliveryStep): string {
  return STEP_DEFS.find((s) => s.step === step)?.path ?? "documentos"
}

export function moneyCLP(n: number | null | undefined): string {
  if (n == null || Number.isNaN(n)) return "—"
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  }).format(n)
}

const VISIT_STATUS_LABELS: Record<string, string> = {
  in_progress: "En curso",
  completed: "Entregado",
  partial: "Entrega parcial",
  not_delivered: "No entregado",
  pending: "Pendiente",
  cancelled: "Cancelado",
}

export function visitStatusLabel(status: string | null | undefined): string {
  if (!status) return "Sin iniciar"
  return VISIT_STATUS_LABELS[status] || status
}

/** Clave estable de documento para rutas: related_document_id || document_number. */
export function docKeyFromDocument(doc: {
  related_document_id?: number | string | null
  document_number?: number | string | null
}): string {
  const id = doc.related_document_id || doc.document_number
  return id == null || id === "" ? "" : String(id)
}

/** customerKey se codifica con encodeURIComponent; la decodificación la hace cada página. */
export function encodeCustomerKey(customerKey: string): string {
  return encodeURIComponent(customerKey)
}

export function decodeCustomerKey(raw: string | string[] | undefined): string {
  const s = Array.isArray(raw) ? raw.join("/") : String(raw || "")
  try {
    return decodeURIComponent(s)
  } catch {
    return s
  }
}

type EntregaSubpath =
  | "documentos"
  | "cobranza"
  | "firma"
  | "incidencia"
  | "resultado"
  | "ficha"

/**
 * Construye el href de una pantalla del circuito de entrega.
 * `step` puede ser 1|2|3, un nombre de paso/subruta, o undefined (raíz /entrega).
 * `extra`: { docKey?, query? } — docKey agrega /documentos/[docKey].
 */
export function buildEntregaHref(
  planId: number | string,
  customerKey: string,
  step?: DeliveryStep | EntregaSubpath,
  extra?: { docKey?: string; query?: Record<string, string | number | null | undefined> },
): string {
  const base = `/despachos/${planId}/clientes/${encodeCustomerKey(customerKey)}/entrega`
  let path = base
  if (step === "ficha") {
    path = `/despachos/${planId}/clientes/${encodeCustomerKey(customerKey)}`
  } else if (step != null) {
    const sub = typeof step === "number" ? stepPath(step) : step
    path = `${base}/${sub}`
    if (sub === "documentos" && extra?.docKey) {
      path += `/${encodeURIComponent(extra.docKey)}`
    }
  }
  const q = extra?.query
  if (q) {
    const sp = new URLSearchParams()
    for (const [k, v] of Object.entries(q)) {
      if (v != null && v !== "") sp.set(k, String(v))
    }
    const qs = sp.toString()
    if (qs) path += `?${qs}`
  }
  return path
}
