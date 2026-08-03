"use client"

import { CostV2StatusBadge } from "@/components/costos-v2/cost-v2-status-badge"
import { Badge } from "@/components/ui/badge"
import {
  SYMBOLOGY_ALERTS,
  SYMBOLOGY_INTRO,
  SYMBOLOGY_SCOPE_NOTE,
  SYMBOLOGY_STATUSES,
  warningLabel,
} from "@/lib/costos-v2/labels"

/**
 * Pestaña estática — no realiza llamadas a la API.
 */
export function CostV2SymbologyPanel() {
  return (
    <div className="space-y-5">
      <div className="space-y-1.5">
        <h2 className="text-base font-semibold">Simbología de costos</h2>
        <p className="text-sm text-muted-foreground">{SYMBOLOGY_INTRO}</p>
        <p className="rounded-md border border-border/60 bg-muted/30 px-3 py-2 text-xs text-muted-foreground">
          {SYMBOLOGY_SCOPE_NOTE}
        </p>
      </div>

      <section className="space-y-2">
        <h3 className="text-sm font-semibold">Estados del costo</h3>
        <ul className="space-y-2">
          {SYMBOLOGY_STATUSES.map((s) => (
            <li
              key={s.code}
              className="rounded-md border border-border/70 px-3 py-2.5"
            >
              <div className="mb-1.5 flex flex-wrap items-center gap-2">
                <CostV2StatusBadge status={s.code} />
              </div>
              <p className="text-sm text-foreground">{s.description}</p>
              <p className="mt-1 text-xs text-muted-foreground">
                <span className="font-medium text-foreground/80">Acción sugerida:</span>{" "}
                {s.action}
              </p>
            </li>
          ))}
        </ul>
      </section>

      <section className="space-y-2">
        <h3 className="text-sm font-semibold">Alertas</h3>
        <ul className="space-y-2">
          {SYMBOLOGY_ALERTS.map((a) => (
            <li
              key={a.code}
              className="rounded-md border border-border/70 px-3 py-2.5"
            >
              <div className="mb-1.5">
                <Badge variant="outline" className="font-normal">
                  {warningLabel(a.code)}
                </Badge>
              </div>
              <p className="text-sm text-foreground">{a.description}</p>
            </li>
          ))}
        </ul>
      </section>
    </div>
  )
}
