"use client"

import { RefreshCw, RotateCcw } from "lucide-react"

import { Button } from "@/components/ui/button"
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
import { COST_V2_STATUS_LABEL, COST_V2_WARNING_LABEL } from "@/lib/costos-v2/labels"

const ALL = "__all__"

export type CostV2FilterDraft = {
  officeId: string
  dateFrom: string
  dateTo: string
  search: string
  status: string
  warning: string
  barcode: string
}

export function CostV2Filters({
  offices,
  draft,
  onChange,
  onApply,
  onClear,
  loading,
  disabled,
}: {
  offices: CostOfficeRef[]
  draft: CostV2FilterDraft
  onChange: (patch: Partial<CostV2FilterDraft>) => void
  onApply: () => void
  onClear: () => void
  loading?: boolean
  disabled?: boolean
}) {
  return (
    <div className="rounded-md border border-border/60 bg-card/40 p-3">
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4 xl:grid-cols-8">
        <div className="space-y-1.5">
          <Label htmlFor="v2-office">Oficina</Label>
          <Select
            value={draft.officeId || ALL}
            onValueChange={(v) => onChange({ officeId: v === ALL ? "" : v })}
            disabled={disabled}
          >
            <SelectTrigger id="v2-office">
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

        <div className="space-y-1.5">
          <Label htmlFor="v2-from">Fecha desde</Label>
          <Input
            id="v2-from"
            type="date"
            value={draft.dateFrom}
            onChange={(e) => onChange({ dateFrom: e.target.value })}
            disabled={disabled}
          />
        </div>

        <div className="space-y-1.5">
          <Label htmlFor="v2-to">Fecha hasta</Label>
          <Input
            id="v2-to"
            type="date"
            value={draft.dateTo}
            onChange={(e) => onChange({ dateTo: e.target.value })}
            disabled={disabled}
          />
        </div>

        <div className="space-y-1.5">
          <Label htmlFor="v2-search">Buscador</Label>
          <Input
            id="v2-search"
            value={draft.search}
            placeholder="Producto / documento"
            onChange={(e) => onChange({ search: e.target.value })}
            disabled={disabled}
          />
        </div>

        <div className="space-y-1.5">
          <Label>Estado de calidad</Label>
          <Select
            value={draft.status || ALL}
            onValueChange={(v) => onChange({ status: v === ALL ? "" : v })}
            disabled={disabled}
          >
            <SelectTrigger>
              <SelectValue placeholder="Todos" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={ALL}>Todos</SelectItem>
              {Object.entries(COST_V2_STATUS_LABEL).map(([k, label]) => (
                <SelectItem key={k} value={k}>
                  {label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <div className="space-y-1.5">
          <Label>Warning</Label>
          <Select
            value={draft.warning || ALL}
            onValueChange={(v) => onChange({ warning: v === ALL ? "" : v })}
            disabled={disabled}
          >
            <SelectTrigger>
              <SelectValue placeholder="Todos" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={ALL}>Todos</SelectItem>
              {Object.entries(COST_V2_WARNING_LABEL).map(([k, label]) => (
                <SelectItem key={k} value={k}>
                  {label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <div className="space-y-1.5">
          <Label htmlFor="v2-barcode">Código de barras</Label>
          <Input
            id="v2-barcode"
            value={draft.barcode}
            inputMode="numeric"
            autoComplete="off"
            placeholder="Texto exacto"
            onChange={(e) => onChange({ barcode: e.target.value })}
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
            className="flex-1"
            onClick={onApply}
            disabled={disabled || loading}
          >
            <RefreshCw className={`mr-1.5 h-3.5 w-3.5 ${loading ? "animate-spin" : ""}`} />
            Actualizar
          </Button>
        </div>
      </div>
    </div>
  )
}
