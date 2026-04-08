"use client"

import type { PurchaseOrderDetailRow, PurchaseOrderHeader } from "@/lib/api"

function fmtMoney(n: number | string | null | undefined): string {
  const x = typeof n === "string" ? parseFloat(n) : Number(n)
  if (!Number.isFinite(x)) return "—"
  return x.toLocaleString("es-CL", { minimumFractionDigits: 0, maximumFractionDigits: 0 })
}

function fmtDate(s: string | null | undefined): string {
  if (!s) return "—"
  return String(s).slice(0, 10)
}

export type OcInvoiceProps = {
  header: PurchaseOrderHeader
  details: PurchaseOrderDetailRow[]
}

/** Contenido imprimible de una OC. */
export function OcInvoicePrint({ header, details }: OcInvoiceProps) {
  return (
    <div className="oc-invoice-root text-slate-900">
      <style>{`
        @media print {
          .oc-invoice-root { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
        }
      `}</style>
      <header className="border-b border-slate-300 pb-4">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">Orden de compra</p>
            <h1 className="text-2xl font-bold text-slate-900">OC #{header.oc_id}</h1>
            <p className="mt-1 text-sm text-slate-600">
              {header.company_name ?? `Empresa ${header.company_id}`}
            </p>
            <p className="mt-0.5 text-sm text-slate-600">
              <span className="text-slate-500">Sucursal:</span>{" "}
              {header.office_name?.trim() ? (
                <>
                  <span className="font-medium text-slate-800">{header.office_name.trim()}</span>
                  <span className="text-slate-400"> (id {header.office_id})</span>
                </>
              ) : (
                <span className="font-medium text-slate-800">Sucursal {header.office_id}</span>
              )}
              {header.office_state != null && header.office_state !== 0 ? (
                <span className="ml-1 font-medium text-amber-800">· Inactiva en Bsale</span>
              ) : null}
            </p>
          </div>
          <div className="text-right text-sm text-slate-600">
            <p>
              <span className="text-slate-500">Emisión:</span> {fmtDate(header.fecha_emision)}
            </p>
            <p>
              <span className="text-slate-500">Entrega:</span> {fmtDate(header.fecha_entrega)}
            </p>
          </div>
        </div>
        <div className="mt-4 rounded-lg bg-slate-50 px-4 py-3 text-sm">
          <p className="font-semibold text-slate-900">{header.supplier_name ?? `Proveedor #${header.supplier_id}`}</p>
          {header.forma_pago ? (
            <p className="text-slate-600">
              <span className="text-slate-500">Pago:</span> {header.forma_pago}
            </p>
          ) : null}
          {header.responsable ? (
            <p className="text-slate-600">
              <span className="text-slate-500">Responsable:</span> {header.responsable}
            </p>
          ) : null}
          {header.observacion ? <p className="mt-2 text-slate-700">{header.observacion}</p> : null}
        </div>
      </header>

      <table className="mt-6 w-full border-collapse text-sm">
        <thead>
          <tr className="border-b border-slate-300 text-left text-xs font-semibold uppercase text-slate-500">
            <th className="py-2 pr-2">Producto</th>
            <th className="py-2 pr-2">Variante</th>
            <th className="py-2 pr-2 text-right">Cant.</th>
            <th className="py-2 pr-2 text-right">Cajas</th>
            <th className="py-2 pr-2 text-right">Unit.</th>
            <th className="py-2 text-right">Total</th>
          </tr>
        </thead>
        <tbody>
          {details.map((d) => (
            <tr key={d.oc_detail_id} className="border-b border-slate-200">
              <td className="py-2 pr-2 align-top">{d.product_name ?? "—"}</td>
              <td className="py-2 pr-2 align-top text-slate-600">{d.variant_name ?? "—"}</td>
              <td className="py-2 pr-2 text-right tabular-nums align-top">{fmtMoney(d.cantidad)}</td>
              <td className="py-2 pr-2 text-right tabular-nums align-top text-slate-600">
                {d.cajas != null ? fmtMoney(d.cajas) : "—"}
              </td>
              <td className="py-2 pr-2 text-right tabular-nums align-top">{fmtMoney(d.costo_unitario)}</td>
              <td className="py-2 text-right tabular-nums align-top font-medium">{fmtMoney(d.costo_total)}</td>
            </tr>
          ))}
        </tbody>
        <tfoot>
          <tr>
            <td colSpan={5} className="pt-4 text-right text-sm font-semibold text-slate-700">
              Total
            </td>
            <td className="pt-4 text-right text-lg font-bold tabular-nums text-slate-900">
              {fmtMoney(header.total_oc)}
            </td>
          </tr>
        </tfoot>
      </table>

      <p className="mt-8 text-center text-xs text-slate-400">Documento generado desde Quillotana ERP</p>
    </div>
  )
}

export function triggerPrintInvoice(rootId: string) {
  const node = document.getElementById(rootId)
  if (!node) {
    window.print()
    return
  }
  const style = document.createElement("style")
  style.setAttribute("data-oc-print", "1")
  style.textContent = `
    @media print {
      body * { visibility: hidden !important; }
      #${rootId}, #${rootId} * { visibility: visible !important; }
      #${rootId} { position: absolute !important; left: 0 !important; top: 0 !important; width: 100% !important; padding: 24px !important; background: white !important; }
    }
  `
  document.head.appendChild(style)
  window.print()
  document.head.removeChild(style)
}
