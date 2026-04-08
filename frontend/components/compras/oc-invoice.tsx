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
        {/* Cabecera: 3 columnas horizontales — logo | empresa/sucursal | OC/fechas */}
        <header className="border-b border-slate-200 pb-4 print:pb-3">
          <div className="grid grid-cols-[auto_minmax(0,1fr)_auto] items-start gap-x-4 gap-y-2 sm:gap-x-6">
            {/* Izquierda: logo */}
            <div className="flex justify-start self-start pt-0.5">
              <img
                src={LOGO_URL}
                alt="Grupo Quillotana"
                className="h-14 w-auto max-h-[3.25rem] max-w-[200px] object-contain object-left sm:h-[3.5rem] sm:max-w-[220px] print:h-[3.25rem] print:max-w-[200px]"
              />
            </div>

            {/* Centro: título + empresa (grande) + sucursal */}
            <div className="min-w-0 px-1 text-center sm:px-2">
              <p className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500 sm:text-[11px]">
                Orden de compra
              </p>
              <h1 className="mt-0.5 text-lg font-bold leading-tight tracking-tight text-slate-900 sm:text-xl print:text-[1.35rem]">
                ORDEN DE COMPRA
              </h1>
              <p className="mt-2 text-xl font-bold leading-snug text-slate-900 sm:text-2xl md:text-[1.65rem] print:text-[1.6rem]">
                {companyDisplay(header)}
              </p>
              <p className="mt-1 text-sm text-slate-600 sm:text-[0.95rem]">
                <span className="text-slate-500">Sucursal:</span>{" "}
                <span className="font-semibold text-slate-800">{officeDisplay(header)}</span>
              </p>
            </div>

            {/* Derecha: N° OC + fechas alineadas a la derecha */}
            <div className="min-w-[7.5rem] shrink-0 text-right sm:min-w-[9rem]">
              <p className="text-[10px] font-semibold uppercase tracking-wide text-slate-500 sm:text-xs">N° OC</p>
              <p className="mt-0.5 text-2xl font-bold tabular-nums leading-none text-slate-900 sm:text-3xl print:text-[1.85rem]">
                {header.oc_id}
              </p>
              <div className="mt-2 space-y-0.5 text-xs text-slate-700 sm:text-sm">
                <p className="tabular-nums">
                  <span className="text-slate-500">Emisión:</span>{" "}
                  <span className="font-semibold text-slate-900">{fmtDate(header.fecha_emision)}</span>
                </p>
                <p className="tabular-nums">
                  <span className="text-slate-500">Entrega:</span>{" "}
                  <span className="font-semibold text-slate-900">{fmtDate(header.fecha_entrega)}</span>
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

/**
 * Imprime solo la OC. El host suele vivir dentro de un Dialog con `transform`
 * (centrado Radix); `position:absolute` ahí se ancla al modal y en PDF queda
 * medio folio en blanco arriba. Clonamos al `body` para posicionar respecto
 * al área de impresión real.
 */
export function triggerPrintInvoice(rootId: string) {
  const node = document.getElementById(rootId)
  if (!node) {
    window.print()
    return
  }

  const cloneId = `${rootId}-print-clone`
  const clone = node.cloneNode(true) as HTMLElement
  clone.id = cloneId
  clone.removeAttribute("hidden")
  clone.setAttribute("data-oc-print-clone", "1")
  document.body.appendChild(clone)

  const style = document.createElement("style")
  style.setAttribute("data-oc-print", "1")
  style.textContent = `
    /* Clon fuera de vista en pantalla (solo existe mientras dura el diálogo de impresión). */
    #${cloneId} {
      position: fixed;
      left: -9999px;
      top: 0;
      width: 210mm;
      max-width: 100vw;
      opacity: 0;
      pointer-events: none;
      z-index: -1;
    }
    @page { size: A4; margin: 12mm 14mm; }
    @media print {
      html, body {
        height: auto !important;
        overflow: visible !important;
        background: white !important;
      }
      body * { visibility: hidden !important; }
      #${cloneId}, #${cloneId} * { visibility: visible !important; }
      #${cloneId} {
        position: absolute !important;
        left: 0 !important;
        top: 0 !important;
        width: 100% !important;
        max-width: 100% !important;
        margin: 0 !important;
        padding: 0 !important;
        opacity: 1 !important;
        z-index: auto !important;
        background: white !important;
        box-shadow: none !important;
      }
    }
  `
  document.head.appendChild(style)

  let cleaned = false
  const cleanup = () => {
    if (cleaned) return
    cleaned = true
    style.remove()
    clone.remove()
    window.removeEventListener("afterprint", cleanup)
  }

  window.addEventListener("afterprint", cleanup)
  window.print()
  /* Si afterprint no dispara (poco frecuente), no dejar el clon en el DOM para siempre. */
  window.setTimeout(cleanup, 120_000)
}
