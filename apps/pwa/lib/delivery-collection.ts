/** Forma de pago DECLARATIVA (Operativa 2.0). Sin montos. */

export type PaymentMethod =
  | "cash"
  | "transfer_declared"
  | "cash_and_transfer"
  | "pending"
  | "not_applicable"

export const PAYMENT_METHODS: ReadonlyArray<{ value: PaymentMethod; label: string }> = [
  { value: "cash", label: "Efectivo" },
  { value: "transfer_declared", label: "Transferencia informada" },
  { value: "cash_and_transfer", label: "Efectivo + transferencia" },
  { value: "pending", label: "Pago pendiente" },
  { value: "not_applicable", label: "Sin cobro / No aplica" },
]

export function paymentMethodLabel(method: string | null | undefined): string {
  const m = String(method || "")
  return PAYMENT_METHODS.find((x) => x.value === m)?.label || m || "—"
}

/** True si la fila es del flujo declarativo (sin monto inventado). */
export function isDeclarativeCollection(row: {
  declared_amount?: number | null
  method?: string | null
}): boolean {
  return row.declared_amount == null
}

export function validatePaymentMethod(method: string | null | undefined): string | null {
  if (!method || !PAYMENT_METHODS.some((m) => m.value === method)) {
    return "Seleccione cómo pagó el cliente"
  }
  return null
}

/** @deprecated legado con montos — conservado para tests históricos. */
export type PaymentRow = {
  key: string
  method: PaymentMethod | string
  amount: string
  note: string
}

/** @deprecated */
export function parseAmount(raw: string): number | null {
  const t = String(raw || "").trim()
  if (!t) return null
  const cleaned = t
    .replace(/\$/g, "")
    .replace(/\s/g, "")
    .replace(/\./g, "")
    .replace(/,/g, "")
  if (!cleaned || !/^-?\d+$/.test(cleaned)) return null
  const n = Number(cleaned)
  return Number.isFinite(n) ? n : null
}

/** @deprecated */
export function sumDeclared(rows: PaymentRow[]): number {
  return rows.reduce((acc, r) => {
    if (r.method === "not_applicable") return acc
    const n = parseAmount(r.amount)
    return acc + (n != null && n > 0 ? n : 0)
  }, 0)
}

/** @deprecated */
export function pendingBalance(documentTotal: number, rows: PaymentRow[]): number {
  return Math.max(0, Math.round((documentTotal - sumDeclared(rows)) * 100) / 100)
}

/** @deprecated */
export function validateMixedPayments(
  documentTotal: number,
  rows: PaymentRow[],
): string | null {
  if (!rows.length) return "Agregue al menos una forma de pago"
  let sum = 0
  for (const r of rows) {
    const n = parseAmount(r.amount)
    if (r.method === "not_applicable") {
      if (n != null && n > 0) return "No corresponde no admite monto positivo"
      continue
    }
    if (n == null || n < 0) return "Monto inválido en una forma de pago"
    sum += n
  }
  if (sum - documentTotal > 0.009) {
    return "La suma de montos supera el total del documento"
  }
  return null
}

/** @deprecated */
export function newPaymentRow(partial?: Partial<PaymentRow>): PaymentRow {
  return {
    key: `pay-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
    method: (partial?.method as PaymentMethod) || "cash",
    amount: partial?.amount ?? "",
    note: partial?.note ?? "",
  }
}
