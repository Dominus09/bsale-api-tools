"use client"

import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import type { Company } from "@/lib/api"

const ESTADOS_FILTRO = [
  { value: "all", label: "Todos" },
  { value: "Activa", label: "Activas" },
  { value: "Programada", label: "Próximas" },
  { value: "Vencida", label: "Vencidas" },
  { value: "Inactiva", label: "Pausadas" },
] as const

type PromotionFiltersProps = {
  filterTipo: string
  filterEstado: string
  filterCompanyId: string
  companies: Company[]
  onTipoChange: (v: string) => void
  onEstadoChange: (v: string) => void
  onCompanyChange: (v: string) => void
  showEstado?: boolean
}

export function PromotionFilters({
  filterTipo,
  filterEstado,
  filterCompanyId,
  companies,
  onTipoChange,
  onEstadoChange,
  onCompanyChange,
  showEstado = true,
}: PromotionFiltersProps) {
  return (
    <div className="flex flex-wrap gap-3">
      <div className="grid min-w-[130px] gap-1.5">
        <Label className="text-xs">Tipo</Label>
        <Select value={filterTipo} onValueChange={onTipoChange}>
          <SelectTrigger className="h-9">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">Todos</SelectItem>
            <SelectItem value="oferta">Oferta</SelectItem>
            <SelectItem value="remate">Remate</SelectItem>
          </SelectContent>
        </Select>
      </div>
      {showEstado ? (
        <div className="grid min-w-[140px] gap-1.5">
          <Label className="text-xs">Estado</Label>
          <Select value={filterEstado} onValueChange={onEstadoChange}>
            <SelectTrigger className="h-9">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {ESTADOS_FILTRO.map((e) => (
                <SelectItem key={e.value} value={e.value}>
                  {e.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      ) : null}
      <div className="grid min-w-[180px] gap-1.5">
        <Label className="text-xs">Empresa</Label>
        <Select value={filterCompanyId} onValueChange={onCompanyChange}>
          <SelectTrigger className="h-9">
            <SelectValue placeholder="Todas" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">Todas</SelectItem>
            {companies.map((c) => (
              <SelectItem key={c.company_id} value={String(c.company_id)}>
                {c.name}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
    </div>
  )
}
