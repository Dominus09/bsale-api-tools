/** Helpers de cobranza mixta (declaración auditada, no contable). */

export type PaymentMethod =
  | "cash"
  | "transfer_declared"
  | "credit"
  | "pending"
  | "not_applicable"

export type PaymentRow = {
  key: string
  method: PaymentMethod
  amount: string
  note: string
}

export const PAYMENT_METHODS: ReadonlyArray<{ value: PaymentMethod; label: string }> = [
  { value: "cash", label: "Efectivo" },
  { value: "transfer_declared", label: "Transferencia informada" },
  { value: "credit", label: "Crédito autorizado" },
  { value: "pending", label: "Pago pendiente" },
  { value: "not_applicable", label: "No corresponde" },
]

export function parseAmount(raw: string): number | null {
  const t = raw.trim()
  if (!t) return null
  const n = Number(t.replace(",", "."))
  return Number.isFinite(n) ? n : null
}

/** Suma de montos declarados en filas (pending cuenta como monto asignado a pendiente). */
export function sumDeclared(rows: PaymentRow[]): number {
  return rows.reduce((acc, r) => {
    if (r.method === "not_applicable") return acc
    const n = parseAmount(r.amount)
    return acc + (n != null && n > 0 ? n : 0)
  }, 0)
}

export function pendingBalance(documentTotal: number, rows: PaymentRow[]): number {
  return Math.max(0, round2(documentTotal - sumDeclared(rows)))
}

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
    if (r.method !== "pending" && n === 0 && rows.length === 1) {
      /* permitir 0 solo si es pending u otras filas */
    }
    sum += n
  }
  if (sum - documentTotal > 0.009) {
    return "La suma de montos supera el total del documento"
  }
  return null
}

function round2(n: number): number {
  return Math.round(n * 100) / 100
}

export function newPaymentRow(partial?: Partial<PaymentRow>): PaymentRow {
  return {
    key: `pay-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
    method: partial?.method || "cash",
    amount: partial?.amount ?? "",
    note: partial?.note ?? "",
  }
}
