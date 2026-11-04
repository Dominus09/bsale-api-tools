"use client"

/**
 * Contrato visual PWA chofer (solo presentación).
 * Eventos operativos (entrega parcial, foto/GPS, etc.) no implementados.
 */

export type DriverDeliveryProductLine = {
  label: string
  quantity: number
  barcode?: string | null
}

export type DriverDeliveryCardData = {
  stopNumber: number
  clientName: string
  fantasyName?: string | null
  address?: string | null
  commune?: string | null
  sector?: string | null
  phone?: string | null
  documentNumber?: string | number | null
  documentType?: string | null
  documentTotalClp?: number | null
  /** Forma de pago declarada en el documento (≠ cobro en ruta). */
  declaredPaymentMethod?: string | null
  observations?: string | null
  products?: DriverDeliveryProductLine[]
  deliveryStatus?: "pending" | "in_progress" | "delivered" | "partial" | "failed"
}

type Props = {
  stop: DriverDeliveryCardData
  onOpen?: () => void
  className?: string
}

export function DriverDeliveryCard({ stop, onOpen, className }: Props) {
  return (
    <article
      className={
        className ||
        "space-y-2 rounded-xl border border-border/80 bg-card/80 p-4 text-sm shadow-sm"
      }
    >
      <header className="flex items-start justify-between gap-2">
        <div>
          <p className="text-[11px] uppercase tracking-wide text-muted-foreground">
            Parada {stop.stopNumber}
            {stop.commune ? ` · ${stop.commune}` : ""}
            {stop.sector ? ` · ${stop.sector}` : ""}
          </p>
          <h3 className="text-base font-semibold leading-tight">
            {stop.fantasyName || stop.clientName}
          </h3>
          {stop.fantasyName && stop.fantasyName !== stop.clientName ? (
            <p className="text-xs text-muted-foreground">{stop.clientName}</p>
          ) : null}
        </div>
        <span className="rounded-md bg-muted px-2 py-0.5 text-[10px] uppercase">
          {stop.deliveryStatus || "pending"}
        </span>
      </header>
      <p className="text-xs">{stop.address || "Sin dirección"}</p>
      <p className="text-xs text-muted-foreground">Contacto: {stop.phone || "—"}</p>
      <dl className="grid grid-cols-2 gap-x-3 gap-y-1 text-xs">
        <dt className="text-muted-foreground">Documento</dt>
        <dd className="font-mono">
          {stop.documentType || "Doc"} {stop.documentNumber ?? "—"}
        </dd>
        <dt className="text-muted-foreground">Total documento</dt>
        <dd className="tabular-nums">
          {stop.documentTotalClp != null
            ? stop.documentTotalClp.toLocaleString("es-CL")
            : "—"}
        </dd>
        <dt className="text-muted-foreground">Pago declarado</dt>
        <dd>{stop.declaredPaymentMethod || "—"}</dd>
      </dl>
      {stop.observations ? (
        <p className="text-xs text-amber-800 dark:text-amber-200">{stop.observations}</p>
      ) : null}
      {stop.products?.length ? (
        <ul className="max-h-28 space-y-0.5 overflow-auto text-xs">
          {stop.products.slice(0, 12).map((p, i) => (
            <li key={i}>
              {p.quantity}× {p.label}
            </li>
          ))}
        </ul>
      ) : (
        <p className="text-[11px] text-muted-foreground">
          Productos: abrir detalle (API estructurada; no Excel/PDF).
        </p>
      )}
      <button
        type="button"
        className="mt-1 w-full rounded-md border px-3 py-2 text-xs font-medium"
        onClick={onOpen}
      >
        Abrir entrega
      </button>
      <p className="text-[10px] text-muted-foreground">
        Futuro: iniciar visita · entrega completa/parcial · rechazo · local cerrado ·
        devolución · foto/GPS. Cobro en ruta ≠ pago declarado del documento.
      </p>
    </article>
  )
}
