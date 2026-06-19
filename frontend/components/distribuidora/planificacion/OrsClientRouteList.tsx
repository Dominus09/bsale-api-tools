"use client"

import {
  SEMAPHORE_BORDER_CLASS,
  SEMAPHORE_EMOJI,
} from "@/lib/ors-commercial-semaphore"
import { formatClp, type RouteClientRow } from "@/lib/ors-map-ui"
import { cn } from "@/lib/utils"
import { Skeleton } from "@/components/ui/skeleton"

type OrsClientRouteListProps = {
  clients: RouteClientRow[]
  loading?: boolean
  selectedClientId?: number | null
  onSelectClient?: (client: RouteClientRow) => void
}

export function OrsClientRouteList({
  clients,
  loading,
  selectedClientId,
  onSelectClient,
}: OrsClientRouteListProps) {
  return (
    <div className="border-b border-border/80 px-3 py-3">
      <p className="mb-2 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        Clientes de la ruta ({clients.length})
      </p>
      {loading ? (
        <div className="space-y-2">
          {Array.from({ length: 4 }).map((_, i) => (
            <Skeleton key={i} className="h-12 w-full" />
          ))}
        </div>
      ) : clients.length === 0 ? (
        <p className="py-4 text-center text-xs text-muted-foreground">
          Sin clientes identificados en esta ruta.
        </p>
      ) : (
        <ul className="max-h-52 space-y-1.5 overflow-y-auto pr-0.5">
          {clients.map((c) => {
            const active = selectedClientId === c.client_id
            return (
              <li key={c.client_id}>
                <button
                  type="button"
                  onClick={() => onSelectClient?.(c)}
                  className={cn(
                    "flex w-full items-center gap-2 rounded-lg border px-2.5 py-2 text-left text-xs transition-colors",
                    "hover:border-primary/30 hover:bg-muted/40",
                    "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
                    SEMAPHORE_BORDER_CLASS[c.semaphore],
                    active && "ring-1 ring-primary/40",
                  )}
                >
                  <span className="w-5 shrink-0 text-center font-semibold tabular-nums text-muted-foreground">
                    {c.list_index}
                  </span>
                  <span className="shrink-0" aria-hidden>
                    {c.isolated ? "⚫" : SEMAPHORE_EMOJI[c.semaphore]}
                  </span>
                  <span className="min-w-0 flex-1 truncate font-medium text-foreground">
                    {c.nombre}
                    {c.isolated ? (
                      <span className="ml-1 text-[10px] font-normal text-muted-foreground">
                        aislado
                      </span>
                    ) : null}
                  </span>
                  <span className="shrink-0 font-semibold tabular-nums text-foreground">
                    {formatClp(c.venta_total)}
                  </span>
                </button>
              </li>
            )
          })}
        </ul>
      )}
    </div>
  )
}
