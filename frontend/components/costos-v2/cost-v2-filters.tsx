"use client"

import { ChevronDown, RefreshCw, RotateCcw } from "lucide-react"

import { Button } from "@/components/ui/button"
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import type { CostOfficeRef } from "@/lib/api"
import {
  FILTER_STATUS_OPTIONS,
  FILTER_WARNING_OPTIONS,
} from "@/lib/costos-v2/labels"

const ALL = "__all__"

export type CostV2FilterDraft = {
  officeId: string
  dateFrom: string
  dateTo: string
  search: string
  status: string
  warning: string
  barcode: string
  minChangePercent: string
  onlyWithChanges: boolean
  onlyNeedsReview: boolean
}

export function CostV2Filters({
  offices,
  draft,
  onChange,
  onApply,
  onClear,
  loading,
  disabled,
  moreOpen,
  onMoreOpenChange,
}: {
  offices: CostOfficeRef[]
  draft: CostV2FilterDraft
  onChange: (patch: Partial<CostV2FilterDraft>) => void
  onApply: () => void
  onClear: () => void
  loading?: boolean
  disabled?: boolean
  moreOpen: boolean
  onMoreOpenChange: (open: boolean) => void
}) {
  return (
    <div className="space-y-2 rounded-md border border-border/70 bg-card/30 p-3">
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-5">
        <div className="space-y-1 min-w-0">
          <Label htmlFor="v2-office">Oficina</Label>
          <Select
            value={draft.officeId || ALL}
            onValueChange={(v) => onChange({ officeId: v === ALL ? "" : v })}
            disabled={disabled}
          >
            <SelectTrigger id="v2-office" className="w-full">
              <SelectValue placeholder="Oficina" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={ALL}>Seleccione oficina</SelectItem>
              {offices.map((o) => (
                <SelectItem key={o.office_id} value={String(o.office_id)}>
                  {o.office_name || `Oficina ${o.office_id}`}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="space-y-1 min-w-0">
          <Label htmlFor="v2-from">Fecha desde</Label>
          <Input
            id="v2-from"
            type="date"
            className="w-full"
            value={draft.dateFrom}
            onChange={(e) => onChange({ dateFrom: e.target.value })}
            disabled={disabled}
          />
        </div>
        <div className="space-y-1 min-w-0">
          <Label htmlFor="v2-to">Fecha hasta</Label>
          <Input
            id="v2-to"
            type="date"
            className="w-full"
            value={draft.dateTo}
            onChange={(e) => onChange({ dateTo: e.target.value })}
            disabled={disabled}
          />
        </div>
        <div className="space-y-1 min-w-0 lg:col-span-1">
          <Label htmlFor="v2-search">Buscador</Label>
          <Input
            id="v2-search"
            className="w-full"
            value={draft.search}
            placeholder="Producto / código"
            onChange={(e) => onChange({ search: e.target.value })}
            disabled={disabled}
          />
        </div>
        <div className="flex items-end gap-2">
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="flex-1"
            onClick={onClear}
            disabled={disabled || loading}
          >
            <RotateCcw className="mr-1.5 h-3.5 w-3.5" />
            Limpiar
          </Button>
          <Button
            type="button"
            size="sm"
            className="flex-1 bg-red-700 hover:bg-red-800"
            onClick={onApply}
            disabled={disabled || loading}
          >
            <RefreshCw className={`mr-1.5 h-3.5 w-3.5 ${loading ? "animate-spin" : ""}`} />
            Actualizar
          </Button>
        </div>
      </div>

      <Collapsible open={moreOpen} onOpenChange={onMoreOpenChange}>
        <CollapsibleTrigger asChild>
          <Button type="button" variant="ghost" size="sm" className="h-7 px-2 text-xs">
            <ChevronDown className="mr-1 h-3.5 w-3.5" />
            Más filtros
          </Button>
        </CollapsibleTrigger>
        <CollapsibleContent className="mt-2 grid gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6">
          <div className="space-y-1 min-w-0">
            <Label>Estado</Label>
            <Select
              value={draft.status || ALL}
              onValueChange={(v) => onChange({ status: v === ALL ? "" : v })}
            >
              <SelectTrigger className="w-full">
                <SelectValue placeholder="Todos" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value={ALL}>Todos</SelectItem>
                {FILTER_STATUS_OPTIONS.map((opt) => (
                  <SelectItem key={opt.value} value={opt.value}>
                    {opt.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1 min-w-0">
            <Label>Alerta</Label>
            <Select
              value={draft.warning || ALL}
              onValueChange={(v) => onChange({ warning: v === ALL ? "" : v })}
            >
              <SelectTrigger className="w-full">
                <SelectValue placeholder="Todas" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value={ALL}>Todas</SelectItem>
                {FILTER_WARNING_OPTIONS.map((opt) => (
                  <SelectItem key={opt.value} value={opt.value}>
                    {opt.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1 min-w-0">
            <Label htmlFor="v2-bc">Código de barras</Label>
            <Input
              id="v2-bc"
              className="w-full"
              value={draft.barcode}
              onChange={(e) => onChange({ barcode: e.target.value })}
            />
          </div>
          <div className="space-y-1 min-w-0">
            <Label htmlFor="v2-minchg">Variación mínima %</Label>
            <Input
              id="v2-minchg"
              className="w-full"
              inputMode="decimal"
              placeholder="10"
              value={draft.minChangePercent}
              onChange={(e) => onChange({ minChangePercent: e.target.value })}
            />
          </div>
          <label className="flex items-center gap-2 text-sm pt-6">
            <input
              type="checkbox"
              checked={draft.onlyWithChanges}
              onChange={(e) => onChange({ onlyWithChanges: e.target.checked })}
            />
            Solo con cambios
          </label>
          <label className="flex items-center gap-2 text-sm pt-6">
            <input
              type="checkbox"
              checked={draft.onlyNeedsReview}
              onChange={(e) => onChange({ onlyNeedsReview: e.target.checked })}
            />
            Solo pendientes de revisión
          </label>
        </CollapsibleContent>
      </Collapsible>
    </div>
  )
}
