"use client"

/**
 * Contrato visual PWA bodega (solo presentación).
 * No registra preparación/carga real — E2 diseño.
 */

export type WarehousePickingCardData = {
  productLabel: string
  barcode?: string | null
  category?: string | null
  branchLabel?: string | null
  requestedUnits: number
  /** Equivalencia comercial SEC; no es caja física cargada. */
  commercialPackLabel?: string | null
  preparedUnits?: number | null
  loadedUnits?: number | null
  truckName?: string | null
  planningCode?: string | null
  sealLabel?: string | null
}

type Props = {
  item: WarehousePickingCardData
  className?: string
}

export function WarehousePickingCard({ item, className }: Props) {
  const diff =
    item.preparedUnits != null
      ? item.preparedUnits - item.requestedUnits
      : null
  return (
    <article
      className={
        className ||
        "space-y-2 rounded-xl border border-border/80 bg-card/80 p-4 text-sm shadow-sm"
      }
    >
      <header className="space-y-0.5">
        <h3 className="text-base font-semibold leading-tight">{item.productLabel}</h3>
        <p className="text-[11px] uppercase tracking-wide text-muted-foreground">
          {[item.category, item.branchLabel].filter(Boolean).join(" · ") || "Sin categoría"}
        </p>
      </header>
      <p className="font-mono text-xs text-muted-foreground">
        Código: {item.barcode || "—"}
      </p>
      <dl className="grid grid-cols-2 gap-x-3 gap-y-1 text-xs">
        <dt className="text-muted-foreground">Cantidad</dt>
        <dd className="tabular-nums font-medium">{item.requestedUnits} unidades</dd>
        <dt className="text-muted-foreground">Equiv. cajas SEC</dt>
        <dd>{item.commercialPackLabel || "Sin presentación SEC confiable"}</dd>
        <dt className="text-muted-foreground">Preparado (físico)</dt>
        <dd>{item.preparedUnits == null ? "pendiente" : item.preparedUnits}</dd>
        <dt className="text-muted-foreground">Cargado (físico)</dt>
        <dd>{item.loadedUnits == null ? "pendiente" : item.loadedUnits}</dd>
        {diff != null ? (
          <>
            <dt className="text-muted-foreground">Diferencia</dt>
            <dd className="tabular-nums">{diff}</dd>
          </>
        ) : null}
      </dl>
      <p className="text-[11px] text-muted-foreground">
        {item.planningCode || "PLAN"} · {item.truckName || "Camión"}
        {item.sealLabel ? ` · Sello ${item.sealLabel}` : ""}
      </p>
      <p className="text-[10px] text-muted-foreground">
        Acciones futuras: validar cantidad · registrar faltante · confirmar preparación
        (no implementadas).
      </p>
    </article>
  )
}
