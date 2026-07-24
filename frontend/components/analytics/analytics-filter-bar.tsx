"use client"

import type { ReactNode } from "react"
import { ChevronDown, Filter, X } from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible"
import { cn } from "@/lib/utils"

export type FilterChip = { id: string; label: string; onRemove?: () => void }

export function AnalyticsFilterBar({
  children,
  advanced,
  chips,
  onApply,
  onClear,
  applying,
  className,
}: {
  children: ReactNode
  advanced?: ReactNode
  chips?: FilterChip[]
  onApply: () => void
  onClear: () => void
  applying?: boolean
  className?: string
}) {
  return (
    <section
      className={cn(
        "space-y-3 rounded-lg border border-border/70 bg-card/40 p-3 sm:p-4",
        className,
      )}
      aria-label="Filtros"
    >
      <div className="flex flex-wrap items-end gap-3">{children}</div>

      {advanced ? (
        <Collapsible>
          <CollapsibleTrigger asChild>
            <Button type="button" variant="ghost" size="sm" className="gap-1 px-2">
              <Filter className="h-3.5 w-3.5" />
              Filtros avanzados
              <ChevronDown className="h-3.5 w-3.5" />
            </Button>
          </CollapsibleTrigger>
          <CollapsibleContent className="pt-3">
            <div className="flex flex-wrap items-end gap-3">{advanced}</div>
          </CollapsibleContent>
        </Collapsible>
      ) : null}

      {chips && chips.length > 0 ? (
        <div className="flex flex-wrap gap-1.5" aria-label="Filtros activos">
          {chips.map((c) => (
            <Badge key={c.id} variant="secondary" className="gap-1 pr-1 font-normal">
              {c.label}
              {c.onRemove ? (
                <button
                  type="button"
                  className="rounded-sm p-0.5 hover:bg-muted outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  onClick={c.onRemove}
                  aria-label={`Quitar filtro ${c.label}`}
                >
                  <X className="h-3 w-3" />
                </button>
              ) : null}
            </Badge>
          ))}
        </div>
      ) : null}

      <div className="flex flex-wrap gap-2">
        <Button type="button" size="sm" onClick={onApply} disabled={applying}>
          Aplicar
        </Button>
        <Button type="button" size="sm" variant="outline" onClick={onClear}>
          Limpiar
        </Button>
      </div>
    </section>
  )
}
