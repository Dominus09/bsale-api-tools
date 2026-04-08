"use client"

import type { PurchaseOrderDetailRow, PurchaseOrderHeader } from "@/lib/api"

/** Misma imagen que el login (Grupo La Quillotana). */
const LOGO_URL =
  "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/GRUPO%20QUILLOTANA%20PS-fK4da0sPbUwnmEpeEVmmumWdj977f0.png"

/** Moneda: sin decimales si es entero; hasta 2 si hace falta. */
function fmtMoney(n: number | string | null | undefined): string {
  const x = typeof n === "string" ? parseFloat(n) : Number(n)
  if (!Number.isFinite(x)) return "—"
  const hasDecimals = Math.abs(x - Math.round(x)) > 1e-9
  return x.toLocaleString("es-CL", {
    minimumFractionDigits: hasDecimals ? 2 : 0,
    maximumFractionDigits: hasDecimals ? 2 : 0,
  })
}

function fmtDate(s: string | null | undefined): string {
  if (!s) return "—"
  const d = String(s).slice(0, 10)
  try {
    const [y, m, day] = d.split("-")
    if (y && m && day) return `${day}/${m}/${y}`
  } catch {
    /* fallthrough */
  }
  return d
}

function officeDisplay(header: PurchaseOrderHeader): string {
  const n = header.office_name?.trim()
  if (n) return n
  return "—"
}

function companyDisplay(header: PurchaseOrderHeader): string {
  const n = header.company_name?.trim()
  if (n) return n
  return "LA QUILLOTANA SPA"
}

export type OcInvoiceProps = {
  header: PurchaseOrderHeader
  details: PurchaseOrderDetailRow[]
}

/** Contenido imprimible de una OC — layout documento comercial A4/carta. */
export function OcInvoicePrint({ header, details }: OcInvoiceProps) {
  return (
    <div className="oc-invoice-root text-slate-900 antialiased">
      <style>{`
        @page {
          size: A4;
          margin: 14mm 16mm;
        }
        @media print {
          .oc-invoice-root {
            -webkit-print-color-adjust: exact;
            print-color-adjust: exact;
            box-shadow: none !important;
            margin: 0 !important;
            max-width: none !important;
            min-height: auto !important;
            padding: 0 !important;
          }
        }
        /* Vista previa en pantalla: usar todo el ancho del modal (antes ~210mm se veía “chico”). */
        @media screen {
          .oc-invoice-sheet {
            max-width: 100% !important;
          }
        }
      `}</style>

      <div
        className="oc-invoice-sheet mx-auto w-full max-w-full bg-white px-4 py-6 text-[15px] shadow-sm sm:px-7 sm:py-8 sm:text-[16px] md:px-9 md:py-9 md:text-[17px] print:px-0 print:py-0 print:text-base print:shadow-none"
        style={{ fontFamily: "system-ui, 'Segoe UI', Roboto, sans-serif" }}
      >
        {/* Cabecera documento */}
        <header className="border-b border-slate-200 pb-6">
          <div className="flex flex-col gap-6 lg:flex-row lg:items-start lg:justify-between">
            <div className="flex min-w-0 flex-col items-center gap-4 sm:flex-row sm:items-start sm:gap-6">
              <img
                src={LOGO_URL}
                alt="Grupo Quillotana"
                className="h-20 max-w-[min(100%,340px)] shrink-0 object-contain object-left sm:h-24 sm:max-w-[380px]"
              />
              <div className="min-w-0 text-center sm:text-left">
                <p className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">
                  Orden de compra
                </p>
                <h1 className="mt-1 text-3xl font-bold tracking-tight text-slate-900 sm:text-4xl">
                  ORDEN DE COMPRA
                </h1>
                <p className="mt-3 text-xl font-semibold leading-tight text-slate-900 sm:text-2xl">
                  {companyDisplay(header)}
                </p>
                <p className="mt-2 text-base text-slate-600 sm:text-lg">
                  <span className="text-slate-500">Sucursal:</span>{" "}
                  <span className="font-medium text-slate-800">{officeDisplay(header)}</span>
                </p>
              </div>
            </div>

            <div className="flex shrink-0 flex-col gap-3 rounded-lg border border-slate-200 bg-slate-50/80 px-5 py-4 text-base sm:min-w-[12rem] sm:text-right">
              <div>
                <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">N° OC</p>
                <p className="text-3xl font-bold tabular-nums text-slate-900 sm:text-4xl">{header.oc_id}</p>
              </div>
              <div className="space-y-1.5 border-t border-slate-200/80 pt-3 text-slate-700">
                <p>
                  <span className="text-slate-500">Emisión:</span>{" "}
                  <span className="font-medium tabular-nums text-slate-900">{fmtDate(header.fecha_emision)}</span>
                </p>
                <p>
                  <span className="text-slate-500">Entrega:</span>{" "}
                  <span className="font-medium tabular-nums text-slate-900">{fmtDate(header.fecha_entrega)}</span>
                </p>
              </div>
            </div>
          </div>
        </header>

        {/* Proveedor */}
        <section
          className="mt-6 rounded-lg border border-slate-200 bg-white px-5 py-5 shadow-[inset_0_1px_0_0_rgba(255,255,255,0.8)]"
          aria-label="Datos del proveedor"
        >
          <p className="text-xs font-semibold uppercase tracking-[0.15em] text-slate-500">Proveedor</p>
          <p className="mt-2 text-lg font-semibold text-slate-900 sm:text-xl">
            {header.supplier_name?.trim() ? header.supplier_name.trim() : "—"}
          </p>
          <dl className="mt-4 grid gap-3 text-base text-slate-700 sm:grid-cols-2">
            <div>
              <dt className="text-sm text-slate-500">Forma de pago</dt>
              <dd className="font-medium text-slate-900">{header.forma_pago?.trim() || "—"}</dd>
            </div>
            <div>
              <dt className="text-sm text-slate-500">Responsable</dt>
              <dd className="font-medium text-slate-900">{header.responsable?.trim() || "—"}</dd>
            </div>
            {header.observacion?.trim() ? (
              <div className="sm:col-span-2">
                <dt className="text-sm text-slate-500">Observación</dt>
                <dd className="mt-1 whitespace-pre-wrap text-slate-800">{header.observacion.trim()}</dd>
              </div>
            ) : null}
          </dl>
        </section>

        {/* Detalle productos */}
        <div className="mt-7 min-w-0 print:overflow-visible">
          <table className="w-full min-w-0 border-collapse text-sm sm:text-base">
            <thead>
              <tr className="border-b-2 border-slate-800 text-left">
                <th className="py-3 pr-2 text-left text-xs font-bold uppercase tracking-wide text-slate-800 sm:pr-3 sm:text-sm">
                  Producto
                </th>
                <th className="py-3 pr-2 text-left text-xs font-bold uppercase tracking-wide text-slate-800 sm:pr-3 sm:text-sm">
                  Variante
                </th>
                <th className="py-3 pr-2 text-left text-xs font-bold uppercase tracking-wide text-slate-800 sm:pr-3 sm:text-sm">
                  Código
                </th>
                <th className="whitespace-nowrap py-3 pr-2 text-right text-xs font-bold uppercase tracking-wide text-slate-800 sm:pr-3 sm:text-sm">
                  Cantidad
                </th>
                <th className="whitespace-nowrap py-3 pr-2 text-right text-xs font-bold uppercase tracking-wide text-slate-800 sm:pr-3 sm:text-sm">
                  Cajas
                </th>
                <th className="whitespace-nowrap py-3 pr-2 text-right text-xs font-bold uppercase tracking-wide text-slate-800 sm:pr-3 sm:text-sm">
                  Precio unit.
                </th>
                <th className="whitespace-nowrap py-3 text-right text-xs font-bold uppercase tracking-wide text-slate-800 sm:text-sm">
                  Total
                </th>
              </tr>
            </thead>
            <tbody>
              {details.map((d, i) => (
                <tr
                  key={d.oc_detail_id}
                  className={detailRowClass(i)}
                >
                  <td className="min-w-0 break-words py-3 pr-2 align-top font-bold text-slate-900 sm:pr-3">
                    {d.product_name?.trim() || "—"}
                  </td>
                  <td className="min-w-0 break-words py-3 pr-2 align-top text-sm leading-snug text-slate-600 sm:pr-3 sm:text-[0.95rem]">
                    {d.variant_name?.trim() || "—"}
                  </td>
                  <td className="min-w-0 break-all py-3 pr-2 align-top font-mono text-xs tabular-nums text-slate-700 sm:pr-3 sm:text-sm">
                    {d.barcode?.trim() || "—"}
                  </td>
                  <td className="py-3 pr-2 text-right align-top font-medium tabular-nums text-slate-900 sm:pr-3">
                    {fmtMoney(d.cantidad)}
                  </td>
                  <td className="py-3 pr-2 text-right align-top tabular-nums text-slate-700 sm:pr-3">
                    {d.cajas != null && Number.isFinite(Number(d.cajas)) ? fmtMoney(d.cajas) : "—"}
                  </td>
                  <td className="py-3 pr-2 text-right align-top tabular-nums text-slate-800 sm:pr-3">
                    ${fmtMoney(d.costo_unitario)}
                  </td>
                  <td className="py-3 text-right align-top font-semibold tabular-nums text-slate-900">
                    ${fmtMoney(d.costo_total)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Total */}
        <div className="mt-8 flex flex-col items-end border-t-2 border-slate-900 pt-5">
          <p className="text-sm font-semibold uppercase tracking-[0.2em] text-slate-600">Total orden</p>
          <p className="mt-2 text-4xl font-bold tabular-nums tracking-tight text-slate-900 sm:text-5xl">
            ${fmtMoney(header.total_oc)}
          </p>
        </div>

        {/* Firma */}
        <div className="mt-14 grid gap-8 border-t border-slate-200 pt-8 sm:grid-cols-2">
          <div>
            <div className="h-px w-full max-w-[220px] border-b border-slate-400" />
            <p className="mt-2 text-xs text-slate-600">Firma y timbre proveedor</p>
          </div>
          <div>
            <div className="h-px w-full max-w-[220px] border-b border-slate-400 sm:ml-auto" />
            <p className="mt-2 text-xs text-slate-600 sm:text-right">Firma autorizada — La Quillotana</p>
          </div>
        </div>

        <p className="mt-10 text-center text-[10px] text-slate-400">
          Quillotana ERP · Impresión apta para guardar como PDF desde el diálogo del navegador
        </p>
      </div>
    </div>
  )
}

function detailRowClass(index: number): string {
  const base = "border-b border-slate-100"
  const zebra = index % 2 === 1 ? " bg-slate-50/70 print:bg-slate-50/80" : ""
  return base + zebra
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
    @page { size: A4; margin: 14mm 16mm; }
    @media print {
      body * { visibility: hidden !important; }
      #${rootId}, #${rootId} * { visibility: visible !important; }
      #${rootId} {
        position: absolute !important;
        left: 0 !important;
        top: 0 !important;
        width: 100% !important;
        background: white !important;
      }
    }
  `
  document.head.appendChild(style)
  window.print()
  document.head.removeChild(style)
}
